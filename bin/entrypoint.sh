#!/usr/bin/env bash
# The image's entrypoint: provision the schema, then stream.
set -euo pipefail

# Checked here rather than left to sqlflow. An unset template variable renders
# as an empty string, and DuckDB's error for ATTACH '' does not name the cause.
if [ -z "${SQLFLOW_POSTGRES_URI:-}" ]; then
  echo "SQLFLOW_POSTGRES_URI is not set" >&2
  exit 2
fi

/app/bin/migrate.sh

# exec keeps sqlflow as PID 1, so the supervisor's SIGTERM reaches it and the
# graceful drain runs: stop consuming, write the buffered batch, final window
# poll, commit, exit 0. Without exec, bash holds PID 1 and forwards nothing.
exec sqlflow run -c /app/pipeline.yml "$@"
