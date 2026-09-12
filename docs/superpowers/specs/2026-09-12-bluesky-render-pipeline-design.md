# Bluesky to Render Postgres: design

A sqlflow pipeline consumes the public Bluesky Jetstream, counts posts per
minute by language, and upserts each closed minute into a Postgres on Render.
This repository holds the pipeline config, the Postgres schema, and the deploy
config. Everything secret comes from the environment, because the repository
is public.

The rows it writes are the data behind a public demo page that shows the
pipeline's uptime and streaming progress. The page is a later PR. This PR
makes the data exist.

## Why

sqlflow's README shows the Bluesky consumer as a one-file example. Nothing
shows it running continuously against a real database on cheap hosting. This
repository is that proof: one Docker image, one worker, one Postgres, and a
table anyone can query.

## Decisions

Made in the design conversation. Each one closes a fork.

| Decision | Choice | Rejected |
|---|---|---|
| What is stored | Posts per minute by language. One row per (minute, language). | Raw posts: 50-100 rows/sec fills a small Postgres in days and needs retention logic. |
| Schema management | Plain SQL files in `migrations/`, applied by a `psql` script that records versions in `schema_migrations`. | golang-migrate: a tool to install for one table. `CREATE TABLE` inside the pipeline config: hides schema changes in the pipeline yaml. |
| Where migrations run | In the worker's entrypoint, before `sqlflow run`. Every deploy self-provisions. | A separate Render job: a manual step, and a forgotten one crashes the worker. |
| Hosting | One Render background worker built from the Dockerfile, plus one Render Postgres, declared in `render.yaml`. | Dashboard-only setup: not reproducible for a reader. |
| DuckDB state | In memory. No state path, no disk. | A persistent disk: saves only the 1-2 minutes of counts already in DuckDB at a crash. The downtime gap is lost either way, because the websocket source has no cursor. |
| Process metrics | None in this PR. Uptime is inferred from missing minute buckets. | TurboStats reporting: waits for the control plane. |
| Bucket type | `TIMESTAMPTZ` in DuckDB and Postgres. | Naive `TIMESTAMP` as in the sql-flow example: its cast on write depends on the container's timezone. |
| Postgres URI default | None. The variable is required. | A localhost default: a misconfigured Render worker would fail late and unclearly. |
| sqlflow version | `turbolytics/sql-flow:v1.2.0`, pinned in the Dockerfile. | `latest`: a sql-flow release could change the config schema under the demo. |
| Where the aggregation happens | In sqlflow's tumbling window. | A Postgres trigger accumulating per batch. It works, but it moves the aggregation out of sqlflow and the demo then shows sqlflow as a buffer rather than a stream processor. It also costs 4.2x the writes, measured: 133 row-writes per minute against 32, and 74% of them updates. |
| Every insert lists `updated_at` | Yes, with `now()` in the select list. | Relying on the column's `DEFAULT now()`. The DuckDB Postgres extension routes non-conflicting rows through a bulk `COPY`, and that `COPY` supplies an explicit `NULL` for any column absent from the insert's column list. An explicit `NULL` defeats the default, so the `NOT NULL` fails on exactly the new rows. |

## Data flow

1. sqlflow opens a websocket to Jetstream, filtered to `app.bsky.feed.post`.
2. Every batch of 500 messages lands in a DuckDB table `posts` declared with
   the narrow schema the handler needs: `time_us`, `commit.operation`,
   and `commit.record.langs`.
3. The handler groups the batch by minute and first language and upserts the
   counts into the DuckDB table `posts_per_minute_by_lang`.
4. Every 10 seconds the tumbling window manager selects closed minutes. A
   minute is closed when the stream's newest minute is more than 60 seconds
   past it, or when no batch has arrived for 60 seconds.
5. The window sink upserts the closed rows into Postgres over the DuckDB
   Postgres extension, then deletes them from DuckDB.

The Postgres write happens before the delete, so a crash between the two
republishes the window. The upsert on `(bucket, lang)` absorbs the duplicate.

### The write path, and one trap in it

The DuckDB Postgres extension honours `ON CONFLICT` against an attached table
when that table has a primary key. Verified on DuckDB v1.5.2, the version the
pinned image carries: a multi-row batch mixing new and conflicting keys applies
whole, and republishing the identical batch leaves every value unchanged.

The trap is not the conflict clause. The extension routes non-conflicting rows
through a bulk `COPY`, and that `COPY` supplies an explicit `NULL` for any
column the insert's column list omits. An explicit `NULL` defeats the column's
`DEFAULT`, so `updated_at TIMESTAMPTZ NOT NULL DEFAULT now()` fails on exactly
the rows being inserted for the first time:

```
ERROR:  null value in column "updated_at" violates not-null constraint
DETAIL:  Failing row contains (2026-09-12 12:00:00+00, ja, 7, null)
CONTEXT:  COPY t_conflict, line 1
```

Two consequences for this design:

- Every insert lists `updated_at` explicitly and selects `now()` for it.
  Adding a default is not a fix, because the default is never reached.
- A single-row batch whose one row conflicts takes the update path and never
  touches `COPY`, so it does not reproduce the failure. Only a batch carrying
  at least one new key does, and a real window batch nearly always does.

Two further extension limits, neither of which binds here: a unique index in
place of a primary key fails with a binder error, and two rows sharing a key
inside one statement collapse to one row rather than raising. A window batch
carries one row per key by construction.

## What a restart loses

The websocket source reconnects with backoff but sends no Jetstream cursor. It
rejoins at the live head. Posts that arrive while the process is down or
reconnecting are never seen. With in-memory state, a restart also loses the
counts already in DuckDB: the open minute plus any minute still inside the
60-second grace. On the demo page a restart shows as a missing bucket.

The fix belongs in sql-flow: persist the last `time_us` and send it as the
Jetstream `cursor` parameter on reconnect. The README names this as a known
gap.

## Files

```
README.md
pipeline.yml
migrations/
  0001_posts_per_minute_by_lang.sql
  0002_pipeline_status_view.sql
bin/
  migrate.sh
  entrypoint.sh
Dockerfile
render.yaml
docker-compose.yml
Makefile
.github/workflows/ci.yml
docs/superpowers/specs/2026-09-12-bluesky-render-pipeline-design.md
```

## pipeline.yml

Derived from sql-flow's `dev/config/examples/bluesky/bluesky.postgres.windowed.yml`.

Template variables:

| Variable | Required | Default |
|---|---|---|
| `SQLFLOW_POSTGRES_URI` | yes | none |
| `SQLFLOW_JETSTREAM_URI` | no | `wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post` |

`commands` block, in order: `INSTALL postgres; LOAD postgres;`, then
`ATTACH '{{ SQLFLOW_POSTGRES_URI }}' AS pg (TYPE POSTGRES);`, then the
`CREATE TABLE IF NOT EXISTS posts (...)` schema declaration from the example.

`tables.sql[0]` is `posts_per_minute_by_lang` with columns
`bucket TIMESTAMPTZ, lang TEXT, posts INTEGER` and a unique index on
`(bucket, lang)`. Its manager is the tumbling window from the example:
`poll_interval_seconds: 10`, the two-branch close predicate, and a
`sqlcommand` sink whose SQL is:

```sql
INSERT INTO pg.posts_per_minute_by_lang (bucket, lang, posts, updated_at)
SELECT bucket, lang, posts, now() FROM sqlflow_sink_batch
ON CONFLICT (bucket, lang) DO UPDATE
  SET posts = EXCLUDED.posts, updated_at = EXCLUDED.updated_at
```

`updated_at` is in the column list because the extension's `COPY` path would
otherwise send an explicit `NULL` for it. See "The write path, and one trap in
it".

`pipeline`: name `bluesky-posts-per-minute-by-lang`, `batch_size: 500`,
websocket source on `SQLFLOW_JETSTREAM_URI`, `handlers.StructuredBatch` over
table `posts`, and `sink.type: noop`. The handler SQL is the example's with
one change to the bucket expression:

```sql
time_bucket(INTERVAL '1 minute', to_timestamp(time_us / 1000000)) AS bucket
```

`to_timestamp` returns `TIMESTAMPTZ`, so the bucket is UTC regardless of
the host timezone.

## Migrations

`migrations/0001_posts_per_minute_by_lang.sql`:

```sql
CREATE TABLE IF NOT EXISTS posts_per_minute_by_lang (
  bucket     TIMESTAMPTZ NOT NULL,
  lang       TEXT        NOT NULL,
  posts      INTEGER     NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (bucket, lang)
);
```

The primary key leads with `bucket`, so a time-range query uses it. No second
index.

`migrations/0002_pipeline_status_view.sql`:

```sql
CREATE OR REPLACE VIEW pipeline_status AS
SELECT
  min(bucket)            AS first_bucket,
  max(bucket)            AS latest_bucket,
  max(updated_at)        AS last_write_at,
  count(DISTINCT bucket) AS minutes_observed,
  sum(posts)             AS total_posts
FROM posts_per_minute_by_lang;
```

The view is the contract the demo page reads. It is its own migration so a
later PR can replace it without touching the table. It scans the whole table;
at about 70k rows per day that is fine for a demo and a later migration can
materialize it if it stops being fine.

## bin/migrate.sh

Bash. Requires `psql` on the path and `SQLFLOW_POSTGRES_URI` in the
environment; exits 2 with a one-line message if either is missing. Steps:

1. `CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`.
2. For each `migrations/*.sql` in sorted order, with `version` set to the
   filename without extension, run one `psql` session with
   `-v ON_ERROR_STOP=1 -v version=<version>`. The shell substitutes the file
   path into the script it feeds on stdin:

```
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('sqlflow_bluesky_migrations'));
SELECT count(*) = 0 AS pending FROM schema_migrations WHERE version = :'version' \gset
\if :pending
  \i migrations/<file>
  INSERT INTO schema_migrations (version) VALUES (:'version');
  \echo applied <version>
\else
  \echo skipped <version>
\endif
COMMIT;
```

The advisory lock serializes two instances that start during the same deploy.
The check runs inside the lock, so the second instance sees the first's
commit and skips. Postgres DDL is transactional, so a failing file rolls back
whole and leaves no version recorded. Re-running the script is a no-op. The
script prints one line per file: `applied` or `skipped`.

## bin/entrypoint.sh

Bash. Exits 2 if `SQLFLOW_POSTGRES_URI` is unset, because sqlflow renders an
unset variable as an empty string and DuckDB's `ATTACH ''` error would not
name the cause. Runs `/app/bin/migrate.sh`, then
`exec sqlflow run -c /app/pipeline.yml "$@"`. `exec` keeps sqlflow as PID 1
so Render's SIGTERM reaches it and the graceful drain runs.

## Dockerfile

```
FROM turbolytics/sql-flow:v1.2.0
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client && rm -rf /var/lib/apt/lists/*
COPY pipeline.yml /app/pipeline.yml
COPY migrations /app/migrations
COPY bin /app/bin
ENTRYPOINT ["/app/bin/entrypoint.sh"]
```

The DuckDB Postgres extension is installed at startup, so the container needs
outbound network on first start. The README says so.

## render.yaml

One database and one worker, same region:

- `databases[0]`: name `bluesky-stats`, `plan: basic-256mb`,
  `postgresMajorVersion: "16"`. Not `free`: Render deletes free databases
  after 30 days, and this database exists to show uptime.
- `services[0]`: `type: worker`, name `sqlflow-bluesky`, `runtime: docker`,
  `dockerfilePath: ./Dockerfile`, `plan: starter`, `autoDeploy: true`.
  `envVars`: `SQLFLOW_POSTGRES_URI` from `fromDatabase: {name: bluesky-stats,
  property: connectionString}`, and `SQLFLOW_LOG_LEVEL: INFO`.

The internal connection string needs no TLS. A reader who points the pipeline
at Render's external URL from a laptop appends `?sslmode=require`. The README
says so.

## Local development

`docker-compose.yml` runs two services: `postgres` (`postgres:16`, published
on `localhost:5432`, user, password, and database all `bluesky`, with a
`pg_isready` healthcheck) and `sqlflow` (`build: .`, `depends_on` postgres
healthy, `SQLFLOW_POSTGRES_URI=postgresql://bluesky:bluesky@postgres:5432/bluesky`).
Starting compose runs the same entrypoint as Render: migrate, then run.
Nothing has to be installed on the host but Docker. The Makefile targets:

| Target | Runs |
|---|---|
| `validate` | `sqlflow validate /app/pipeline.yml` inside `turbolytics/sql-flow:v1.2.0` with `pipeline.yml` mounted |
| `run` | `docker compose up --build` |
| `migrate` | `docker compose run --rm --entrypoint /app/bin/migrate.sh sqlflow`, for re-running migrations alone |
| `psql` | `docker compose exec postgres psql -U bluesky`, for querying |
| `image` | `docker build -t sqlflow-bluesky-demo .` |

## CI

`.github/workflows/ci.yml` runs on push and pull request with a
`postgres:16` service container. Two jobs:

- **migrations**: run `bin/migrate.sh` twice; assert `psql` reports the table
  `posts_per_minute_by_lang` and the view `pipeline_status`, and that the
  second run printed `skipped` for every file.
- **config**: run `sqlflow validate` in the pinned image with
  `SQLFLOW_POSTGRES_URI` set to a placeholder. Exit code 0 is the assertion.

## Verification

Done before the PR is opened, with the output in the PR description:

1. `make run` logs `applied` for both files, then sqlflow's startup lines.
   `make migrate` in a second terminal logs `skipped` for both files.
2. Leave `make run` up for at least five minutes. The first window publishes
   about two minutes after the minute it covers ends: one minute of window
   plus the 60-second grace.
3. **No `poll failed` line appears in the logs.** This is the assertion that
   catches a broken write path. A rejected flush logs `poll failed` every
   `poll_interval_seconds` and never exits, so the process looks healthy while
   publishing nothing. Grep for it rather than trusting that the container is
   up.
4. `SELECT * FROM pipeline_status` returns at least two minutes observed and
   a `last_write_at` within the last two minutes.
5. `SELECT bucket, lang, posts FROM posts_per_minute_by_lang ORDER BY posts DESC LIMIT 5` shows plausible languages, `en` and `ja` among them.
6. Re-run the sink's insert by hand against a row already published. It
   returns without error and changes no value, which is what makes the
   at-least-once republish survivable.

## README

Sections, in this order:

1. What this is, in three sentences, and a link to sql-flow.
2. Why: a single-binary stream processor on a public firehose, running
   continuously into a small Postgres, as the data behind a public demo page.
3. What gets stored: the table, the view, one sample query and its output.
4. Run it locally: compose, migrate, run, query.
5. Deploy to Render: the Blueprint button or `render.yaml`, and what the
   worker does on start.
6. Environment variables: the two template variables and `SQLFLOW_LOG_LEVEL`.
7. Delivery semantics and known gaps: at-least-once into Postgres, the
   in-memory choice, the websocket cursor gap, and the manager retry gap
   below.
8. What comes next: the demo page, TurboStats.

Written to Google Technical Writing One style as sql-flow's CLAUDE.md
specifies.

## Upstream gaps this deployment runs on top of

Both belong in sql-flow. Neither blocks this PR, and the README names both.

**A rejected manager flush retries forever and the process stays up.**
`Tumbling.Start` in `internal/managers/tumbling.go` logs `poll failed` and
continues to the next tick. Nothing classifies the error, so a permanently
rejected flush, such as a constraint violation, is retried on every tick with
no deadline and no exit. The rows stay in the table, the manager never makes
progress, and the process reports healthy. `Start` returns `error` but only
ever returns nil, so the caller cannot learn about it either.

Observed directly while building this: a broken insert produced `poll failed`
every 10 seconds for over three minutes while the container stayed `Up` and
wrote nothing. The consume loop has an error policy and liveness invariants.
The manager path has neither and is not a conformance subject.

The fix has three parts: a rejected error on the manager path exits with a
code, an unreachable one follows the retry ladder with its deadline, and the
manager becomes a subject in the conformance harness. Until then the
verification step greps for `poll failed`, because an unhealthy pipeline is
indistinguishable from a healthy one by liveness alone.

**The websocket source cannot resume.** Covered above in "What a restart
loses".

## Out of scope

- The demo page.
- TurboStats or any process-level metrics.
- A Jetstream cursor in the websocket source. That is a sql-flow change.
- Fixing the manager retry path. That is a sql-flow change.
- Retention or rollup of old minutes.
