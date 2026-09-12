# The published sqlflow image carries the matching libduckdb.so and sets
# SQLFLOW_DUCKDB_LIB. Pinned: a sql-flow release could change the config
# schema under this pipeline. Must match the Makefile and CI.
FROM turbolytics/sql-flow:v1.2.0

# psql applies the migrations. postgresql-client is the only addition.
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pipeline.yml /app/pipeline.yml
COPY migrations /app/migrations
COPY bin /app/bin

RUN chmod +x /app/bin/*.sh

# Logs and window timestamps are UTC regardless of the host.
ENV TZ=UTC

ENTRYPOINT ["/app/bin/entrypoint.sh"]
