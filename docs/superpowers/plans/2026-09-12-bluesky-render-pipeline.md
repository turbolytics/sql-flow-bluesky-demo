# Bluesky to Render Postgres Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A sqlflow pipeline that counts Bluesky posts per minute by language and upserts each closed minute into a Postgres on Render, with the schema, the deploy config, and the docs to run it.

**Architecture:** One Docker image built `FROM turbolytics/sql-flow:v1.2.0`. Its entrypoint applies plain SQL migrations with `psql`, then execs `sqlflow run`. sqlflow consumes the Jetstream websocket, aggregates in in-memory DuckDB, and a tumbling window manager upserts closed minutes into Postgres through the DuckDB Postgres extension. `render.yaml` declares one worker and one Postgres.

**Tech Stack:** sqlflow v1.2.0 (Go), DuckDB (in-process, with the `postgres` extension), PostgreSQL 16, bash, `psql`, Docker Compose, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-09-12-bluesky-render-pipeline-design.md`

## Global Constraints

Every task's requirements implicitly include this section.

- **sqlflow image is pinned to `turbolytics/sql-flow:v1.2.0`.** Never `latest`. The tag appears in `Dockerfile`, `Makefile`, and `.github/workflows/ci.yml`; all three must match. `docker-compose.yml` builds from the Dockerfile and inherits the pin.
- **No secrets in the repository.** `SQLFLOW_POSTGRES_URI` is read from the environment with no default anywhere except the Makefile and `docker-compose.yml`, where it points at the local compose database.
- **Postgres is 16.** `postgres:16` in compose and CI, `postgresMajorVersion: "16"` in `render.yaml`.
- **Buckets are `TIMESTAMPTZ` everywhere:** the DuckDB table, the Postgres table, and the handler expression. Verified: `time_bucket(INTERVAL '1 minute', to_timestamp(time_us / 1000000))` returns `TIMESTAMP WITH TIME ZONE` in DuckDB.
- **Every insert into an attached Postgres table lists every `NOT NULL` column, including `updated_at`.** The DuckDB Postgres extension routes non-conflicting rows through a bulk `COPY`, and that `COPY` sends an explicit `NULL` for any omitted column. An explicit `NULL` defeats the column's `DEFAULT`, so relying on `DEFAULT now()` fails on exactly the rows being inserted for the first time. This is verified behaviour on DuckDB v1.5.2, not a precaution.
- **The target table needs a primary key, not a unique index.** The extension honours `ON CONFLICT` against a primary key and fails with a binder error against a unique index alone.
- **Prose follows Google Technical Writing One**, as sql-flow's `CLAUDE.md` requires: active voice, one idea per sentence, no hedging, strong lead sentence, cut unnecessary words. This governs the README, code comments, and commit messages.
- **Commit messages name the defect or deliverable, the approach, and what breaks if it is wrong.** No attribution trailers.
- **Every shell script starts `#!/usr/bin/env bash` and `set -euo pipefail`.**
- **Table and view names are exactly** `posts_per_minute_by_lang`, `pipeline_status`, and `schema_migrations`.

---

### Task 1: Postgres schema and the migration runner

Delivers the schema and the script that applies it. Independently testable against a throwaway Postgres with no sqlflow involved.

**Files:**
- Create: `migrations/0001_posts_per_minute_by_lang.sql`
- Create: `migrations/0002_pipeline_status_view.sql`
- Create: `bin/migrate.sh`
- Create: `docker-compose.yml`
- Create: `Makefile`
- Create: `.gitignore`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `bin/migrate.sh` — no arguments. Reads `SQLFLOW_POSTGRES_URI` from the environment. Resolves `migrations/` relative to its own parent directory, so the working directory does not matter. Exits `0` on success, `2` when `SQLFLOW_POSTGRES_URI` is unset or `psql` is missing, non-zero when a migration fails. Prints one line per file: `applied  <version>` or `skipped  <version>`.
  - Postgres table `posts_per_minute_by_lang (bucket TIMESTAMPTZ, lang TEXT, posts INTEGER, updated_at TIMESTAMPTZ)`, primary key `(bucket, lang)`.
  - Postgres view `pipeline_status (first_bucket, latest_bucket, last_write_at, minutes_observed, total_posts)`.
  - Postgres table `schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ)`.
  - Make targets `migrate`, `psql`, `validate`, `run`, `image`, `clean`.
  - Compose services `postgres` (published on host 5432) and `sqlflow`.

- [ ] **Step 1: Write the migration files**

`migrations/0001_posts_per_minute_by_lang.sql`:

```sql
-- One row per minute per language. The primary key leads with bucket, so a
-- time-range scan uses it and no second index is needed.
CREATE TABLE IF NOT EXISTS posts_per_minute_by_lang (
  bucket     TIMESTAMPTZ NOT NULL,
  lang       TEXT        NOT NULL,
  posts      INTEGER     NOT NULL,
  -- Wall-clock time of the last write. bucket is event time; a demo page needs
  -- both to tell "the stream is behind" from "the stream stopped".
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (bucket, lang)
);
```

`migrations/0002_pipeline_status_view.sql`:

```sql
-- The contract the demo page reads. Kept as its own migration so a later
-- change replaces the view without touching the table.
CREATE OR REPLACE VIEW pipeline_status AS
SELECT
  min(bucket)              AS first_bucket,
  max(bucket)              AS latest_bucket,
  max(updated_at)          AS last_write_at,
  count(DISTINCT bucket)   AS minutes_observed,
  coalesce(sum(posts), 0)  AS total_posts
FROM posts_per_minute_by_lang;
```

`coalesce` on the sum keeps `total_posts` numeric on an empty table, so a
consumer never has to handle a null there.

- [ ] **Step 2: Write `docker-compose.yml`**

```yaml
# Local development. `docker compose up` runs the same entrypoint as Render:
# migrate, then run.
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: bluesky
      POSTGRES_PASSWORD: bluesky
      POSTGRES_DB: bluesky
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U bluesky -d bluesky"]
      interval: 2s
      timeout: 3s
      retries: 15

  sqlflow:
    build: .
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      SQLFLOW_POSTGRES_URI: postgresql://bluesky:bluesky@postgres:5432/bluesky
      SQLFLOW_LOG_LEVEL: INFO
      TZ: UTC
```

The healthcheck plus `condition: service_healthy` is what lets the worker run
migrations on start without a retry loop in the script.

- [ ] **Step 3: Write `bin/migrate.sh`**

```bash
#!/usr/bin/env bash
# Applies every migrations/*.sql exactly once, in filename order.
#
# Safe to run on every process start, which is how the worker uses it. Two
# instances starting during the same deploy cannot race: the applied check and
# the migration run in one transaction behind an advisory lock, so the second
# instance sees the first's commit and skips.
set -euo pipefail

if [ -z "${SQLFLOW_POSTGRES_URI:-}" ]; then
  echo "SQLFLOW_POSTGRES_URI is not set" >&2
  exit 2
fi

if ! command -v psql >/dev/null; then
  echo "psql not found on PATH" >&2
  exit 2
fi

cd "$(dirname "$0")/.."

psql "$SQLFLOW_POSTGRES_URI" -v ON_ERROR_STOP=1 -q \
  -c "SET client_min_messages = warning" \
  -c "CREATE TABLE IF NOT EXISTS schema_migrations (
        version    TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )"

for file in migrations/*.sql; do
  version="$(basename "$file" .sql)"

  # Postgres DDL is transactional, so a migration that fails rolls back whole
  # and records no version. The next run retries it from a clean state.
  psql "$SQLFLOW_POSTGRES_URI" -v ON_ERROR_STOP=1 -q -v version="$version" <<SQL
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('sqlflow_bluesky_migrations')) AS locked \gset
SELECT count(*) = 0 AS pending FROM schema_migrations WHERE version = :'version' \gset
\if :pending
  \i $file
  INSERT INTO schema_migrations (version) VALUES (:'version');
  \echo 'applied ' :version
\else
  \echo 'skipped ' :version
\endif
COMMIT;
SQL
done
```

Three details that are easy to get wrong and were verified against Postgres 16:

- `AS locked \gset` consumes the advisory lock's result row. Without it `psql` prints the lock's result table on every run.
- `\echo 'applied ' :version` interpolates. `\echo 'applied :version'` does not; single quotes suppress it and the literal `:version` is printed.
- `SET client_min_messages = warning` silences the `relation "schema_migrations" already exists, skipping` notice on every run after the first.

- [ ] **Step 4: Write `Makefile`**

```makefile
# The pinned sqlflow image. Must match Dockerfile, docker-compose.yml and CI.
SQLFLOW_IMAGE ?= turbolytics/sql-flow:v1.2.0

# Defaults to the compose database, so `make migrate` works after
# `docker compose up -d postgres`.
SQLFLOW_POSTGRES_URI ?= postgresql://bluesky:bluesky@localhost:5432/bluesky
export SQLFLOW_POSTGRES_URI

.PHONY: validate migrate psql run image clean

## validate: check pipeline.yml against the pinned image's config schema
validate:
	docker run --rm -v $(PWD)/pipeline.yml:/app/pipeline.yml \
		$(SQLFLOW_IMAGE) validate /app/pipeline.yml

## migrate: apply migrations to SQLFLOW_POSTGRES_URI
migrate:
	docker compose run --rm --no-deps --entrypoint /app/bin/migrate.sh sqlflow

## run: start postgres and the pipeline, following logs
run:
	docker compose up --build

## psql: open a shell on the local database
psql:
	docker compose exec postgres psql -U bluesky -d bluesky

## image: build the worker image
image:
	docker build -t sqlflow-bluesky-demo .

## clean: stop compose and delete its volumes
clean:
	docker compose down -v
```

`make migrate` uses `--no-deps` so it reuses a running Postgres rather than
starting a second one, and overrides the entrypoint so it migrates without
starting the pipeline.

- [ ] **Step 5: Write `.gitignore`**

```
.env
*.log
```

- [ ] **Step 6: Test the migrations are idempotent**

Run:

```bash
docker compose up -d postgres
until docker compose exec -T postgres pg_isready -U bluesky -d bluesky; do sleep 1; done
```

The next step builds the image, which does not exist yet. Until Task 2 lands,
test the script directly with a container that has `psql`:

```bash
docker run --rm --network host -v "$PWD":/app -w /app \
  -e SQLFLOW_POSTGRES_URI=postgresql://bluesky:bluesky@localhost:5432/bluesky \
  postgres:16 bash bin/migrate.sh
```

Expected, first run:

```
applied  0001_posts_per_minute_by_lang
applied  0002_pipeline_status_view
```

Run the identical command a second time. Expected:

```
skipped  0001_posts_per_minute_by_lang
skipped  0002_pipeline_status_view
```

- [ ] **Step 7: Test the schema and the failure modes**

Run:

```bash
docker compose exec -T postgres psql -U bluesky -d bluesky \
  -c "\d posts_per_minute_by_lang" \
  -c "select * from pipeline_status" \
  -c "select version from schema_migrations order by version"
```

Expected: `bucket` and `updated_at` are `timestamp with time zone`, the primary
key is `btree (bucket, lang)`, `pipeline_status` returns one row with
`minutes_observed` 0 and `total_posts` 0, and both versions are recorded.

Then check the missing-variable path:

```bash
docker run --rm -v "$PWD":/app -w /app postgres:16 bash bin/migrate.sh; echo "exit=$?"
```

Expected: `SQLFLOW_POSTGRES_URI is not set` on stderr and `exit=2`.

- [ ] **Step 8: Commit**

```bash
git add migrations bin/migrate.sh docker-compose.yml Makefile .gitignore
git commit -m "schema: posts per minute by language, applied by an idempotent psql runner

The demo page needs a table it can query and a pipeline that provisions it
without a manual step. migrate.sh applies migrations/*.sql once each, tracking
versions in schema_migrations, and checks and applies inside one transaction
behind an advisory lock.

Verified: two consecutive runs print applied then skipped, and the table has a
TIMESTAMPTZ bucket with a (bucket, lang) primary key.

Without the lock, two workers starting in the same deploy both see the version
as pending and the second fails on a duplicate key, crashing the worker."
```

---

### Task 2: The pipeline config and the worker image

Delivers a running pipeline. This is the task that proves data lands in Postgres.

**Files:**
- Create: `pipeline.yml`
- Create: `bin/entrypoint.sh`
- Create: `Dockerfile`
- Create: `.dockerignore`

**Interfaces:**
- Consumes: `bin/migrate.sh` from Task 1, at `/app/bin/migrate.sh` in the image. The Postgres table and view it creates.
- Produces:
  - `pipeline.yml`, read by `sqlflow run -c`. Template variables: `SQLFLOW_POSTGRES_URI` (required, no default) and `SQLFLOW_JETSTREAM_URI` (optional).
  - `bin/entrypoint.sh` — the image's entrypoint. Passes its arguments through to `sqlflow run`.
  - Image `sqlflow-bluesky-demo`, which `render.yaml` builds in Task 3.

- [ ] **Step 1: Write `pipeline.yml`**

Derived from sql-flow's `dev/config/examples/bluesky/bluesky.postgres.windowed.yml`.
The window manager comments in that example explain why the close predicate has
two branches; keep them.

```yaml
# Counts Bluesky posts per minute by language in a 1-minute tumbling window and
# upserts each closed window into Postgres.
commands:
  - name: pin the session timezone
    sql: |
      SET TimeZone='UTC';

  - name: load postgres extension
    sql: |
      INSTALL postgres;
      LOAD postgres;

  - name: attach postgres
    sql: |
      ATTACH '{{ SQLFLOW_POSTGRES_URI }}' AS pg (TYPE POSTGRES);

  - name: declare the post schema
    sql: |
      CREATE TABLE IF NOT EXISTS posts (
        time_us BIGINT,
        commit STRUCT(
          operation TEXT,
          record STRUCT(langs TEXT[])
        )
      );

tables:
  sql:
    - name: posts_per_minute_by_lang
      sql: |
        CREATE TABLE IF NOT EXISTS posts_per_minute_by_lang (
          bucket TIMESTAMPTZ,
          lang TEXT,
          posts INTEGER
        );
        CREATE UNIQUE INDEX IF NOT EXISTS posts_per_minute_by_lang_idx
          ON posts_per_minute_by_lang (bucket, lang);

      manager:
        # A window closes against the stream's own clock, not wall clock.
        # max(bucket) is the newest window the data has reached, so a window
        # closes only once the stream has moved past it. That keeps the result
        # identical whether the data arrives live or as a replay. With now()
        # here instead, a pipeline running behind real time by more than the
        # grace period publishes a window whose rows are still arriving, once
        # per poll, and those parts are indistinguishable downstream from an
        # at-least-once duplicate.
        #
        # The second branch is the idleness bound. A stream that goes quiet
        # never moves its own clock, so the first branch alone would hold the
        # newest window open until data a full grace newer arrived.
        # sqlflow_progress.last_arrival is when the newest batch was written,
        # so after a grace of silence every open window closes.
        #
        # The grace outlives the batch wait on purpose: an event from the last
        # seconds of a window can reach DuckDB up to a flush interval after the
        # window ended.
        tumbling_window:
          poll_interval_seconds: 10
          collect_closed_windows_sql: |
            SELECT bucket, lang, posts
            FROM posts_per_minute_by_lang
            WHERE bucket + INTERVAL '1 minute' < (SELECT max(bucket) FROM posts_per_minute_by_lang) - INTERVAL '60 seconds'
               OR (SELECT now() - last_arrival FROM sqlflow_progress) > INTERVAL '1 minute'
          delete_closed_windows_sql: |
            DELETE FROM posts_per_minute_by_lang
            WHERE bucket + INTERVAL '1 minute' < (SELECT max(bucket) FROM posts_per_minute_by_lang) - INTERVAL '60 seconds'
               OR (SELECT now() - last_arrival FROM sqlflow_progress) > INTERVAL '1 minute'

        sink:
          type: sqlcommand
          sqlcommand:
            # ON CONFLICT is what makes at-least-once survivable. The manager
            # deletes a window only after the sink accepts it, so a crash
            # between the two republishes that window on the next poll. Against
            # the target's primary key a bare INSERT fails that republish, and
            # because the failed flush also blocks the delete, every later poll
            # collects the same rows and fails the same way. One duplicate would
            # stop the pipeline publishing anything, ever again.
            #
            # updated_at is listed and selected as now() on purpose. The
            # postgres extension sends rows that do not conflict through a
            # COPY, and that COPY passes an explicit NULL for any column this
            # list omits. An explicit NULL defeats the column's DEFAULT, so
            # omitting updated_at fails the NOT NULL on every new minute.
            sql: |
              INSERT INTO pg.posts_per_minute_by_lang (bucket, lang, posts, updated_at)
              SELECT bucket, lang, posts, now() FROM sqlflow_sink_batch
              ON CONFLICT (bucket, lang) DO UPDATE
                SET posts = EXCLUDED.posts, updated_at = EXCLUDED.updated_at

pipeline:
  name: bluesky-posts-per-minute-by-lang
  batch_size: 500

  source:
    type: websocket
    websocket:
      uri: "{{ SQLFLOW_JETSTREAM_URI|default('wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post') }}"

  handler:
    type: handlers.StructuredBatch
    table: posts
    sql: |
      INSERT INTO posts_per_minute_by_lang
      SELECT
        -- to_timestamp yields TIMESTAMPTZ, so the bucket is an instant and the
        -- write into a Postgres TIMESTAMPTZ column needs no interpretation. The
        -- example's date_trunc(make_timestamp(time_us)) yields a naive
        -- TIMESTAMP, and Postgres reads a naive value in the session timezone.
        time_bucket(INTERVAL '1 minute', to_timestamp(time_us / 1000000)) AS bucket,
        coalesce(commit.record.langs[1], 'unknown') AS lang,
        count(*) AS posts
      FROM posts
      WHERE commit.operation = 'create'
      GROUP BY bucket, lang
      ON CONFLICT (bucket, lang) DO UPDATE SET posts = posts + EXCLUDED.posts

  # The window manager does the writing. The pipeline sink would write every
  # batch, which is the opposite of aggregating.
  sink:
    type: noop
```

Two notes for the implementer:

- `SQLFLOW_POSTGRES_URI` has no `default` filter, which is what makes it a
  required input. `sqlflow validate` warns rather than fails when it is unset,
  so CI can validate with no secret. `bin/entrypoint.sh` does the hard check.
- There is no `pipeline.state.path`. DuckDB runs in memory by design; see the
  spec's "What a restart loses".

- [ ] **Step 2: Write `bin/entrypoint.sh`**

```bash
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
```

- [ ] **Step 3: Write `Dockerfile` and `.dockerignore`**

`Dockerfile`:

```dockerfile
# The published sqlflow image carries the matching libduckdb.so and sets
# SQLFLOW_DUCKDB_LIB. Pinned: a sql-flow release could change the config
# schema under this pipeline.
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
```

`.dockerignore`:

```
.git
.github
docs
README.md
docker-compose.yml
Makefile
render.yaml
```

- [ ] **Step 4: Validate the config before running it**

Run: `make validate`

Expected: `/app/pipeline.yml: valid` and exit 0. A warning naming
`SQLFLOW_POSTGRES_URI` as unset is correct and expected, because `make
validate` does not pass it. A `template_undefined` **error** means a typo in a
variable name; fix it before continuing.

- [ ] **Step 5: Run it end to end against local Postgres**

Run:

```bash
make clean
make run
```

Expected in the logs, in order: `applied  0001_...` and `applied  0002_...`,
then `Executing command step` for each of the four commands, then
`Creating managed table`, then batch logs. One warning is expected and
harmless: `could not derive reference tables from handler sql ... Only SELECT
statements can be serialized to json`. The handler is an INSERT, and sql-flow's
own example configs produce the same line.

Leave it running at least five minutes. The first window publishes about two
minutes after the minute it covers ends: one minute of window plus the
60-second grace. Measured: the 18:26 bucket published at 18:29:08.

- [ ] **Step 6: Verify no poll failed, before trusting anything else**

Run:

```bash
docker compose logs sqlflow | grep -c "poll failed"
```

Expected: `0`.

Do this before Step 7, and do not skip it because the container is up. A
rejected window flush logs `poll failed` every 10 seconds, never exits, and
publishes nothing, while `docker compose ps` still reports the service
running. Liveness tells you nothing here. This is an upstream sql-flow defect
the spec names; until it is fixed, this grep is the only signal.

If the count is non-zero, read one line with
`docker compose logs sqlflow | grep -m1 "poll failed"`. A
`null value in column "updated_at"` error means the sink's column list omits
`updated_at`; see the Global Constraints.

- [ ] **Step 7: Verify the rows**

In a second terminal, run:

```bash
docker compose exec -T postgres psql -U bluesky -d bluesky \
  -c "select * from pipeline_status" \
  -c "select bucket, lang, posts from posts_per_minute_by_lang order by bucket desc, posts desc limit 10"
```

Expected: `minutes_observed` is at least 1 and grows on a later query,
`last_write_at` is within the last minute, `total_posts` is in the thousands
per minute, and the top languages include `en` and `ja`. Every `bucket` ends in
`+00` and falls on a minute boundary.

If `posts_per_minute_by_lang` is empty but `pipeline_status` returns a row, the
pipeline is running and no window has closed yet. Wait another minute.

- [ ] **Step 8: Verify the sink SQL through the extension, not through psql**

This is the claim the sink's `ON CONFLICT` exists for, and it has to be
tested in the layer that runs it. Two tempting shortcuts both pass against
broken SQL, so neither is a test:

- Running the upsert in `psql` tests native Postgres, which honours the
  column's `DEFAULT`. The failure lives in the DuckDB extension's `COPY` path.
- Upserting a single row that already exists takes the extension's update
  path and never reaches `COPY`. Only a batch with at least one new key does.

So drive the extension with a batch that mixes a new key and a conflicting
one, and run it twice. Start with a known row:

```bash
docker compose exec -T postgres psql -U bluesky -d bluesky -c "
  INSERT INTO posts_per_minute_by_lang (bucket, lang, posts)
  VALUES ('2000-01-01 00:00:00+00', 'en', 10)"
```

Then write the batch through DuckDB with the sink's exact SQL, twice. Use the
`duckdb` CLI (`brew install duckdb`) at v1.5.2, the version the image carries:

```bash
cat > /tmp/sink_check.sql <<'SQL'
INSTALL postgres; LOAD postgres;
ATTACH 'postgresql://bluesky:bluesky@localhost:5432/bluesky' AS pg (TYPE POSTGRES);
CREATE TABLE sqlflow_sink_batch AS
  SELECT '2000-01-01 00:00:00+00'::TIMESTAMPTZ AS bucket, 'en' AS lang, 5::INTEGER AS posts
  UNION ALL SELECT '2000-01-01 00:00:00+00'::TIMESTAMPTZ, 'ja', 7::INTEGER;
INSERT INTO pg.posts_per_minute_by_lang (bucket, lang, posts, updated_at)
SELECT bucket, lang, posts, now() FROM sqlflow_sink_batch
ON CONFLICT (bucket, lang) DO UPDATE
  SET posts = EXCLUDED.posts, updated_at = EXCLUDED.updated_at;
SQL
duckdb < /tmp/sink_check.sql
duckdb < /tmp/sink_check.sql
docker compose exec -T postgres psql -U bluesky -d bluesky -c "
  SELECT lang, posts FROM posts_per_minute_by_lang
  WHERE bucket = '2000-01-01 00:00:00+00' ORDER BY lang"
```

Expected: both `duckdb` runs exit 0 with no error, and the query returns
`en | 5` and `ja | 7`. `en` moved from 10 to 5, which proves the conflict
clause ran. `ja` exists, which proves the new-key `COPY` path accepted the
row. The second run changed nothing, which proves a republished window is
absorbed.

Now prove the test can fail. Remove `, updated_at` and `, now()` from the two
lines in `/tmp/sink_check.sql`, change `'ja'` to `'pt'` so the batch carries a
fresh key, and run it once more. Expected: `null value in column "updated_at"
violates not-null constraint` with `CONTEXT: COPY`. A check that cannot fail
proves nothing.

Clean up:

```bash
docker compose exec -T postgres psql -U bluesky -d bluesky -c "
  DELETE FROM posts_per_minute_by_lang WHERE bucket = '2000-01-01 00:00:00+00'"
rm /tmp/sink_check.sql
```

- [ ] **Step 9: Verify the graceful drain**

Run `docker compose stop sqlflow` while it is streaming, then
`docker compose logs --tail=20 sqlflow`.

Expected: the drain lines and exit 0, not a SIGKILL after a timeout. This
confirms `exec` in the entrypoint put sqlflow at PID 1.

- [ ] **Step 10: Commit**

```bash
git add pipeline.yml bin/entrypoint.sh Dockerfile .dockerignore
git commit -m "pipeline: Jetstream posts per minute by language into Postgres

Counts posts in a 1-minute tumbling window and upserts each closed window into
Postgres through the DuckDB postgres extension. The entrypoint migrates then
execs sqlflow, so a deploy provisions its own schema and SIGTERM still reaches
the drain.

The sink lists updated_at and selects now() for it. The extension sends rows
that do not conflict through a COPY, and that COPY passes an explicit NULL for
any omitted column, which defeats the column's DEFAULT. Omitting it fails the
NOT NULL on every new minute.

The bucket is TIMESTAMPTZ via to_timestamp rather than the example's naive
make_timestamp: Postgres reads a naive timestamp in the session timezone, which
would shift every bucket on a host that is not UTC.

Verified: five minutes against the live firehose publishes minute buckets with
en and ja leading and no poll failed line. A batch mixing a new and a
conflicting key, written twice through the extension, applies once and is
absorbed on the second write. Removing updated_at from the column list
reproduces the NOT NULL failure, so the check can fail.

If the column list omits updated_at, the manager logs poll failed every tick,
never exits, and publishes nothing while the process reports healthy."
```

---

### Task 3: Render deploy config and CI

Delivers the deploy config and the two checks that keep it honest.

**Files:**
- Create: `render.yaml`
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `Dockerfile` from Task 2, `bin/migrate.sh` and `migrations/` from Task 1, `pipeline.yml` from Task 2.
- Produces: nothing later tasks read.

- [ ] **Step 1: Write `render.yaml`**

```yaml
# Render Blueprint: one worker streaming Jetstream into one Postgres.
databases:
  - name: bluesky-stats
    databaseName: bluesky
    user: bluesky
    # Not free: Render deletes a free database after 30 days, and this database
    # exists to show uptime over months.
    plan: basic-256mb
    postgresMajorVersion: "16"
    region: oregon

services:
  - type: worker
    name: sqlflow-bluesky
    runtime: docker
    dockerfilePath: ./Dockerfile
    plan: starter
    region: oregon
    autoDeploy: true
    envVars:
      # The internal connection string. It never leaves Render's network and is
      # never committed. An external URL needs ?sslmode=require; the internal
      # one does not.
      - key: SQLFLOW_POSTGRES_URI
        fromDatabase:
          name: bluesky-stats
          property: connectionString
      - key: SQLFLOW_LOG_LEVEL
        value: INFO
```

The worker and the database share `region: oregon`, because `fromDatabase`
resolves to the internal connection string and that is reachable only inside
one region.

- [ ] **Step 2: Write `.github/workflows/ci.yml`**

```yaml
name: ci

on:
  push:
  pull_request:

jobs:
  migrations:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: bluesky
          POSTGRES_PASSWORD: bluesky
          POSTGRES_DB: bluesky
        ports:
          - 5432:5432
        options: >-
          --health-cmd "pg_isready -U bluesky -d bluesky"
          --health-interval 2s
          --health-timeout 3s
          --health-retries 15
    env:
      SQLFLOW_POSTGRES_URI: postgresql://bluesky:bluesky@localhost:5432/bluesky
    steps:
      - uses: actions/checkout@v4

      - name: Apply migrations
        run: bin/migrate.sh | tee first.log

      - name: Every migration applied on the first run
        run: grep -q '^applied ' first.log

      - name: Apply migrations again
        run: bin/migrate.sh | tee second.log

      # The check that matters: a non-idempotent migration crashes the worker
      # on its second deploy, not its first.
      - name: Nothing applied on the second run
        run: |
          grep -q '^skipped ' second.log
          ! grep -q '^applied ' second.log

      - name: The schema is what the demo page reads
        run: |
          psql "$SQLFLOW_POSTGRES_URI" -v ON_ERROR_STOP=1 -c "
            SELECT bucket, lang, posts, updated_at
            FROM posts_per_minute_by_lang WHERE false"
          psql "$SQLFLOW_POSTGRES_URI" -v ON_ERROR_STOP=1 -c "
            SELECT first_bucket, latest_bucket, last_write_at,
                   minutes_observed, total_posts
            FROM pipeline_status"

  config:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      # Catches a config that no longer parses, which is the failure a pinned
      # image makes rare and a bumped image makes likely.
      - name: Validate pipeline.yml
        run: |
          docker run --rm -v "$PWD/pipeline.yml:/app/pipeline.yml" \
            turbolytics/sql-flow:v1.2.0 validate /app/pipeline.yml
```

The `migrations` job needs no `psql` install step. The `ubuntu-latest` runner
ships the PostgreSQL client.

- [ ] **Step 3: Verify the workflow parses**

Run:

```bash
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('ci.yml parses')"
python3 -c "import yaml,sys; yaml.safe_load(open('render.yaml')); print('render.yaml parses')"
```

Expected: both print.

- [ ] **Step 4: Verify the CI assertions locally**

The migrations job's assertions must pass on a real database before CI runs
them. Run:

```bash
make clean && docker compose up -d postgres
until docker compose exec -T postgres pg_isready -U bluesky -d bluesky; do sleep 1; done
docker run --rm --network host -v "$PWD":/app -w /app \
  -e SQLFLOW_POSTGRES_URI=postgresql://bluesky:bluesky@localhost:5432/bluesky \
  postgres:16 bash -c '
    bin/migrate.sh | tee first.log
    grep -q "^applied " first.log
    bin/migrate.sh | tee second.log
    grep -q "^skipped " second.log
    ! grep -q "^applied " second.log
    echo "idempotency assertions pass"'
rm -f first.log second.log
```

Expected: `idempotency assertions pass`.

- [ ] **Step 5: Commit**

```bash
git add render.yaml .github/workflows/ci.yml
git commit -m "deploy: a Render Blueprint and the two checks that keep it honest

render.yaml declares the worker and the database together, so a reader
reproduces the deployment rather than recreating it from prose. The database is
basic-256mb because Render deletes a free one after 30 days and this deployment
exists to show uptime.

CI applies the migrations twice and validates the config offline. The second
run is the assertion that matters: a non-idempotent migration passes its first
deploy and crashes the second."
```

---

### Task 4: README

Delivers the document that makes the repository usable by someone who did not
write it.

**Files:**
- Modify: `README.md` (currently one line: `# sqlflow-bluesky-demo`)

**Interfaces:**
- Consumes: every file from Tasks 1 through 3. Commands quoted in the README must be the ones those tasks created.
- Produces: nothing.

- [ ] **Step 1: Replace `README.md`**

Write these sections in this order. Follow Google Technical Writing One as the
Global Constraints require: draft short the first time.

1. **Title and one-paragraph summary.** What runs, what it writes, where it
   runs. Link to https://github.com/turbolytics/sql-flow.
2. **Why this exists.** sqlflow's README shows the Bluesky consumer as a
   one-file example. Nothing shows it running continuously against a real
   database on cheap hosting. This repository is that proof: one image, one
   worker, one Postgres, and a table anyone can query. The rows are the data
   behind a public demo page showing uptime and streaming progress.
3. **How it works.** The five-step data flow from the spec, as a numbered list.
   State that DuckDB runs in memory and there is no state path.
4. **What gets stored.** The `posts_per_minute_by_lang` columns as a table with
   a description per column, then `pipeline_status`, then one sample query and
   its real output pasted from Task 2 Step 6.
5. **Run it locally.** Prerequisite: Docker. Then `make run`, what to expect in
   the logs, how long until the first window closes, and `make psql` to query.
   Mention `make validate` and `make clean`.
6. **Deploy to Render.** Point at `render.yaml`, say that Render's Blueprint
   flow reads it and creates both the worker and the database, and that
   `SQLFLOW_POSTGRES_URI` is wired from the database so no secret is entered.
   State that the worker migrates on every start, so a deploy provisions its
   own schema. Note that an external connection string needs
   `?sslmode=require` and the internal one does not.
7. **Configuration.** A table of the three variables:

   | Variable | Required | Default |
   |---|---|---|
   | `SQLFLOW_POSTGRES_URI` | yes | none |
   | `SQLFLOW_JETSTREAM_URI` | no | `wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post` |
   | `SQLFLOW_LOG_LEVEL` | no | `INFO` |

8. **Delivery semantics and known gaps.** Four short subsections:
   - *Postgres writes are at-least-once.* A window is written before its rows
     are deleted from DuckDB, so a crash between the two republishes it. The
     upsert on `(bucket, lang)` absorbs the duplicate.
   - *A restart loses data.* The websocket source reconnects with backoff but
     sends no Jetstream cursor, so it rejoins at the live head and posts that
     arrived while the process was down are never seen. In-memory state also
     loses the open minute. On the demo page a restart shows as a missing
     bucket. The fix belongs in sql-flow: persist the last `time_us` and send
     it as the Jetstream `cursor` parameter on reconnect.
   - *A rejected window write does not stop the process.* sql-flow's window
     manager logs `poll failed` and retries on the next tick, with no deadline
     and no exit. A permanently rejected write therefore publishes nothing
     while the process stays up. Watch the logs for `poll failed`; liveness
     alone does not show it. The fix belongs in sql-flow.
   - *The language is the first language.* A post tagged `["en","ja"]` counts
     once, under `en`. Posts with no language count as `unknown`.
9. **What comes next.** The demo page reading `pipeline_status`, and TurboStats
   reporting real process uptime instead of inferring it from gaps.

- [ ] **Step 2: Verify every command in the README runs**

Run each fenced command in the README in a clean checkout, in the order the
README gives them. Confirm `make run`, `make psql`, `make validate`, and
`make clean` all behave as the README says. Fix the README, not the reader's
expectations, where they differ.

- [ ] **Step 3: Verify the sample output is real**

The sample query output in the "What gets stored" section must be pasted from
an actual run, not invented. Confirm the numbers match a real
`pipeline_status` row and real top languages.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: what this streams, how to run it, and what a restart loses

A public repository needs a reader to get from clone to rows without reading
the pipeline config. Documents the data flow, the schema, the three
environment variables, and the two semantics that surprise people: Postgres
writes are at-least-once, and a restart drops the downtime because the
websocket source carries no Jetstream cursor.

Sample output is pasted from a real run, so a reader comparing their output to
the README is comparing against something that happened."
```

---

## Self-Review

**Spec coverage.** Every spec section maps to a task:

| Spec section | Task |
|---|---|
| Data flow | 2 |
| What a restart loses | documented in 4, no code |
| Files | 1, 2, 3, 4 between them create every listed file |
| pipeline.yml | 2 |
| Migrations | 1 |
| bin/migrate.sh | 1 |
| bin/entrypoint.sh | 2 |
| Dockerfile | 2 |
| render.yaml | 3 |
| Local development | 1 creates compose and the Makefile; 2 exercises them |
| CI | 3 |
| The write path, and one trap in it | Global Constraints; 2 Step 1 sink SQL; 2 Step 8 |
| Upstream gaps | 2 Step 6 greps for `poll failed`; 4 documents both gaps |
| Verification | 1 Steps 6-7, 2 Steps 4-9, 3 Steps 3-4 |
| README | 4 |
| Out of scope | no task, correctly |

Two plan details go beyond the spec's sketches. Both were found by running the
mechanism against a real Postgres and the pinned image:

- The spec's `migrate.sh` sketch printed `\echo 'applied :version'`, which
  does not interpolate, and left the advisory lock's result row unconsumed,
  which printed a table on every run. Task 1 fixes both and says why.
- The spec did not pin the DuckDB session timezone. Task 2 adds
  `SET TimeZone='UTC'` as the first command, so logs and any naive rendering
  are UTC regardless of host.

One earlier version of this plan carried a verification step that could not
fail. It upserted a single existing row in `psql`. Native Postgres honours the
column default, and a single conflicting row never reaches the extension's
`COPY` path, so the step passed against the broken sink SQL. Task 2 Step 8
replaces it with a mixed batch driven through the extension and a deliberate
negative run.

**Placeholder scan.** No TBD, TODO, "implement later", "add error handling",
or "similar to Task N". Every code step carries the actual content. Task 4's
steps describe prose to write rather than quoting a full README, which is
correct for a document whose sample output must come from a run that has not
happened yet; each section's content is specified.

**Type and name consistency.** Checked across tasks:

- `posts_per_minute_by_lang` columns are `bucket`, `lang`, `posts`,
  `updated_at` in the migration (Task 1), the sink's insert column list
  (Task 2), and the CI assertion (Task 3).
- `pipeline_status` columns are `first_bucket`, `latest_bucket`,
  `last_write_at`, `minutes_observed`, `total_posts` in the migration
  (Task 1), the CI assertion (Task 3), and the README table (Task 4).
- `schema_migrations(version, applied_at)` is created and read only in Task 1.
- `SQLFLOW_POSTGRES_URI` is the one name used in `migrate.sh`,
  `entrypoint.sh`, `pipeline.yml`, the Makefile, compose, `render.yaml`, and
  CI.
- `turbolytics/sql-flow:v1.2.0` appears in the Dockerfile (Task 2), the
  Makefile (Task 1), and CI (Task 3). Compose builds from the Dockerfile, so
  it inherits the pin rather than repeating it.
- The sink column list `(bucket, lang, posts, updated_at)` matches every
  `NOT NULL` column of the migration, as the Global Constraints require.
- `bin/migrate.sh` takes no arguments in Task 1 and is called with none in
  Task 2's entrypoint, Task 1's Makefile target, and Task 3's CI.
