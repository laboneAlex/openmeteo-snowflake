"""
DAG: openmeteo_daily_weather_de

Pulls daily weather forecast data (weather code, temp min/max, sunrise/sunset,
daylight & sunshine duration) for Renningen and Stuttgart from the Open-Meteo
API in a single batched request, and MERGEs the results into a Snowflake
table keyed on (city, date).

Requires:
- pip packages: openmeteo-requests, requests-cache, retry-requests, pandas
- Airflow connection `snowflake_default` (or update SNOWFLAKE_CONN_ID) pointing
  at your Snowflake account, with access to TARGET_TABLE.
"""

from __future__ import annotations

import pendulum
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry

from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

LOCATIONS = {
    "Renningen": {"latitude": 48.7761, "longitude": 8.9319},
    "Stuttgart": {"latitude": 48.7758, "longitude": 9.1829},
}

DAILY_VARIABLES = [
    "weather_code",
    "temperature_2m_max",
    "temperature_2m_min",
    "sunrise",
    "sunset",
    "daylight_duration",
    "sunshine_duration",
]

SNOWFLAKE_CONN_ID = "snowflake_default"
TARGET_DATABASE = "WEATHER_DB"
TARGET_SCHEMA = "RAW"
TARGET_TABLE = "DAILY_WEATHER"
STAGING_SCHEMA = "RAW_STAGING"
STAGING_TABLE = "DAILY_WEATHER_STAGING"

FULLY_QUALIFIED_TARGET = f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}"
FULLY_QUALIFIED_STAGING = f"{TARGET_DATABASE}.{STAGING_SCHEMA}.{STAGING_TABLE}"


@dag(
    dag_id="openmeteo_daily_weather_de",
    description="Daily weather forecast for Renningen and Stuttgart -> Snowflake",
    schedule="0 5 * * *",  # 05:00 Europe/Berlin daily
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Berlin"),
    catchup=False,
    tags=["weather", "open-meteo", "snowflake"],
)
def openmeteo_daily_weather_de():

    @task
    def extract_daily_weather() -> str:
        """Fetch daily forecast data for all configured cities in a single
        batched Open-Meteo API call, and return a tidy dataframe as JSON."""

        cache_session = requests_cache.CachedSession(
            "/tmp/openmeteo_cache", expire_after=3600
        )
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            # Passing lists here is what triggers Open-Meteo's multi-location
            # batching -- one HTTP request, one response per coordinate pair,
            # returned in the same order as submitted.
            "latitude": [loc["latitude"] for loc in LOCATIONS.values()],
            "longitude": [loc["longitude"] for loc in LOCATIONS.values()],
            "daily": DAILY_VARIABLES,
            "models": "dwd_icon_seamless",
            "timezone": "Europe/Berlin",
        }
        responses = openmeteo.weather_api(url, params=params)

        city_names = list(LOCATIONS.keys())
        all_frames = []

        # Loop over the *responses*, not over separate API calls.
        for city_name, response in zip(city_names, responses):
            daily = response.Daily()

            daily_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                    end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=daily.Interval()),
                    inclusive="left",
                ),
                "weather_code": daily.Variables(0).ValuesAsNumpy(),
                "temperature_2m_max": daily.Variables(1).ValuesAsNumpy(),
                "temperature_2m_min": daily.Variables(2).ValuesAsNumpy(),
                "sunrise": daily.Variables(3).ValuesInt64AsNumpy(),
                "sunset": daily.Variables(4).ValuesInt64AsNumpy(),
                "daylight_duration": daily.Variables(5).ValuesAsNumpy(),
                "sunshine_duration": daily.Variables(6).ValuesAsNumpy(),
            }

            city_df = pd.DataFrame(data=daily_data)
            city_df.insert(0, "city", city_name)
            city_df.insert(1, "latitude", response.Latitude())
            city_df.insert(2, "longitude", response.Longitude())

            all_frames.append(city_df)

        combined_df = pd.concat(all_frames, ignore_index=True)

        # Convert epoch seconds -> proper timestamps before this ever reaches
        # Snowflake, so the target columns can be declared TIMESTAMP_TZ.
        combined_df["date"] = combined_df["date"].dt.date.astype(str)
        combined_df["sunrise"] = pd.to_datetime(
            combined_df["sunrise"], unit="s", utc=True
        )
        combined_df["sunset"] = pd.to_datetime(
            combined_df["sunset"], unit="s", utc=True
        )

        # Small payload (2 cities x ~7 rows) -> fine to pass through XCom as JSON.
        # If this pipeline grows to many locations / hourly granularity,
        # switch to writing a staging file to a Snowflake stage instead.
        return combined_df.to_json(orient="records", date_format="iso")

    @task
    def load_to_snowflake(daily_weather_json: str) -> None:
        """Load the extracted dataframe into a staging table, then MERGE into
        the target table keyed on (city, date) so re-runs update rather than
        duplicate rows."""

        combined_df = pd.read_json(daily_weather_json, orient="records")
        combined_df["sunrise"] = pd.to_datetime(combined_df["sunrise"], utc=True)
        combined_df["sunset"] = pd.to_datetime(combined_df["sunset"], utc=True)

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        # 1. Replace the staging table with this run's extract.
        hook.run(f"TRUNCATE TABLE IF EXISTS {FULLY_QUALIFIED_STAGING}")

        rows = list(combined_df.itertuples(index=False, name=None))
        columns = list(combined_df.columns)
        hook.insert_rows(
            table=FULLY_QUALIFIED_STAGING,
            rows=rows,
            target_fields=columns,
            commit_every=1000,
        )

        # 2. MERGE staging into target, keyed on (city, date).
        merge_sql = f"""
            MERGE INTO {FULLY_QUALIFIED_TARGET} AS target
            USING {FULLY_QUALIFIED_STAGING} AS source
            ON target.city = source.city AND target.date = source.date
            WHEN MATCHED THEN UPDATE SET
                target.latitude = source.latitude,
                target.longitude = source.longitude,
                target.weather_code = source.weather_code,
                target.temperature_2m_max = source.temperature_2m_max,
                target.temperature_2m_min = source.temperature_2m_min,
                target.sunrise = source.sunrise,
                target.sunset = source.sunset,
                target.daylight_duration = source.daylight_duration,
                target.sunshine_duration = source.sunshine_duration,
                target.loaded_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                city, latitude, longitude, date, weather_code,
                temperature_2m_max, temperature_2m_min, sunrise, sunset,
                daylight_duration, sunshine_duration, loaded_at
            ) VALUES (
                source.city, source.latitude, source.longitude, source.date,
                source.weather_code, source.temperature_2m_max,
                source.temperature_2m_min, source.sunrise, source.sunset,
                source.daylight_duration, source.sunshine_duration,
                CURRENT_TIMESTAMP()
            )
        """
        hook.run(merge_sql)

    load_to_snowflake(extract_daily_weather())


openmeteo_daily_weather_de()
