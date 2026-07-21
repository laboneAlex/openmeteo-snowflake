#!/bin/bash
set -e

# If the SNOWFLAKE_PRIVATE_KEY secret is present, write it safely to a temporary file
if [[ -n "$SNOWFLAKE_PRIVATE_KEY" ]]; then
  echo "$SNOWFLAKE_PRIVATE_KEY" > /tmp/snowflake_key.p8
  chmod 600 /tmp/snowflake_key.p8
  echo "Snowflake private key file created at /tmp/snowflake_key.p8"
fi

# Hand execution back over to the original Airflow startup scripts
exec /entrypoint "$@"
