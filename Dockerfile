# The published sqlflow image carries the matching libduckdb.so and sets
# SQLFLOW_DUCKDB_LIB. Pinned: a sql-flow release could change the config
# schema under this pipeline. Must match the Makefile and CI.
#
# An argument so an unreleased sqlflow build can be tried locally:
#   docker build --build-arg SQLFLOW_IMAGE=turbolytics/sql-flow:<tag> .
# Render and CI build with the default.
ARG SQLFLOW_IMAGE=turbolytics/sql-flow:v2026.09.17
FROM ${SQLFLOW_IMAGE}

# psql applies the migrations. postgresql-client is the only addition.
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pipeline.yml /app/pipeline.yml
COPY serve.yml /app/serve.yml
COPY migrations /app/migrations
COPY bin /app/bin

RUN chmod +x /app/bin/*.sh

# Logs and window timestamps are UTC regardless of the host.
ENV TZ=UTC

# The worker's entrypoint. The API service overrides it with bin/serve.sh.
ENTRYPOINT ["/app/bin/entrypoint.sh"]
