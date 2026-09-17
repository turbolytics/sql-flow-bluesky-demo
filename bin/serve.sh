#!/usr/bin/env bash
# The API's entrypoint: provision the schema, then serve.
set -euo pipefail

# Checked here rather than left to sqlflow. An unset template variable renders
# as an empty string: ATTACH '' fails without naming the cause, and an empty
# client id fails startup with a rule violation rather than the variable's name.
if [ -z "${SQLFLOW_POSTGRES_URI:-}" ]; then
  echo "SQLFLOW_POSTGRES_URI is not set" >&2
  exit 2
fi
if [ -z "${SQLFLOW_SERVE_TOKEN_BLUESKY_DEMO:-}" ]; then
  echo "SQLFLOW_SERVE_TOKEN_BLUESKY_DEMO is not set" >&2
  exit 2
fi

# The API prepares every statement at start, so the views must exist first.
# Running the migrations here means the API never waits on the worker's deploy
# to create them. The script takes an advisory lock, so both services starting
# at once is safe.
/app/bin/migrate.sh

# exec keeps sqlflow as PID 1, so SIGTERM reaches it and in-flight requests
# drain before it exits.
exec sqlflow serve -c /app/serve.yml "$@"
