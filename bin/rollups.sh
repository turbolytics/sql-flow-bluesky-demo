#!/usr/bin/env bash
# The rollup checker's entrypoint: provision the schema, then supervise.
set -euo pipefail

# Checked here rather than left to sqlflow. An unset template variable renders
# as an empty string, and the daemon's error for an empty DSN does not name it.
if [ -z "${SQLFLOW_POSTGRES_URI:-}" ]; then
  echo "SQLFLOW_POSTGRES_URI is not set" >&2
  exit 2
fi

# `rollup install` runs once at startup and exits non-zero when the minute table
# is not there yet -- it does not wait for it. Nothing orders one Render service
# ahead of another, so on a first deploy this service can beat the worker to the
# database and crash-loop until the worker's migrations land. Running them here
# instead means it never waits on another service's deploy. The script takes an
# advisory lock, so all three starting at once is safe.
/app/bin/migrate.sh

# exec keeps sqlflow as PID 1, so SIGTERM reaches it and it releases the leader
# lock on the way out rather than leaving a standby to wait for the session to
# time out.
exec sqlflow rollup run -c /app/rollups.yml --metrics prometheus "$@"
