# sqlflow-bluesky-demo

A [sqlflow](https://github.com/turbolytics/sql-flow) pipeline that reads the
public Bluesky firehose, counts posts per minute by language, and writes each
minute to Postgres. It runs continuously as one background worker on Render.
The rows it writes are the data behind a public demo page that shows the
pipeline's uptime and streaming progress.

## Why this exists

sqlflow's README shows the Bluesky consumer as a one-file example. This
repository shows the same idea running for months against a real database on
small hosting: one Docker image, one worker, one Postgres, and a table anyone
can query.

The aggregation happens inside sqlflow, in a tumbling window. Postgres only
stores finished minutes. That keeps the demo about stream processing rather
than about moving rows, and it keeps writes low: about 33 rows per minute.

## How it works

1. sqlflow opens a websocket to [Jetstream](https://github.com/bluesky-social/jetstream),
   filtered to post records.
2. Every 500 messages form a batch. sqlflow loads the batch into DuckDB, which
   runs inside the process.
3. The handler's SQL groups the batch by minute and first language and adds
   the counts to an in-memory DuckDB table.
4. Every 10 seconds, sqlflow's window manager looks for closed minutes. A
   minute closes once the stream has moved 60 seconds past it, or once no
   data has arrived for a minute.
5. The manager upserts closed minutes into Postgres, then deletes them from
   DuckDB.

The whole pipeline is [`pipeline.yml`](pipeline.yml). DuckDB runs in memory.
There is no state file.

## What gets stored

One table and one view.

`posts_per_minute_by_lang` holds one row per minute per language:

| Column | Type | Meaning |
|---|---|---|
| `bucket` | `timestamptz` | Start of the minute, in event time. |
| `lang` | `text` | The post's first language tag, or `unknown`. |
| `posts` | `integer` | Posts created in that minute with that language. |
| `updated_at` | `timestamptz` | Wall-clock time of the last write to the row. |

The primary key is `(bucket, lang)`.

`pipeline_status` summarizes the table in one row: `first_bucket`,
`latest_bucket`, `last_write_at`, `minutes_observed`, and `total_posts`. The
demo page reads this view.

Output from a ten-minute local run:

```
$ SELECT * FROM pipeline_status;
      first_bucket      |     latest_bucket      |         last_write_at         | minutes_observed | total_posts
------------------------+------------------------+-------------------------------+------------------+-------------
 2026-09-12 19:25:00+00 | 2026-09-12 19:34:00+00 | 2026-09-12 19:37:07.846362+00 |               10 |       23249

$ SELECT lang, posts FROM posts_per_minute_by_lang
  WHERE bucket = '2026-09-12 19:30:00+00' ORDER BY posts DESC LIMIT 5;
  lang   | posts
---------+-------
 en      |  1728
 unknown |   583
 de      |    78
 pt      |    69
 es      |    66
```

A minute with no rows is a minute the pipeline was not running. This query
lists them:

```sql
SELECT m AS missing_minute
FROM generate_series(
  (SELECT first_bucket FROM pipeline_status),
  (SELECT latest_bucket FROM pipeline_status),
  INTERVAL '1 minute'
) AS m
WHERE NOT EXISTS (
  SELECT 1 FROM posts_per_minute_by_lang p WHERE p.bucket = m
);
```

## Run it locally

You need Docker with Compose.

Start Postgres and the pipeline:

```
make run
```

The worker applies the migrations, then starts streaming:

```
applied  0001_posts_per_minute_by_lang
applied  0002_pipeline_status_view
... Executing command step {"name": "attach postgres"}
... starting tumbling window manager {"poll_interval": "10s"}
... throughput {"messages_consumed": 182, "total_throughput_per_second": 37.5}
```

The first row appears about two minutes after the first full minute ends:
one minute of window plus 60 seconds of grace.

In a second terminal, open a Postgres shell and query:

```
make psql
```

Other targets:

| Target | Does |
|---|---|
| `make validate` | Checks `pipeline.yml` against the pinned sqlflow image. |
| `make migrate` | Applies migrations without starting the pipeline. |
| `make image` | Builds the worker image. |
| `make clean` | Stops everything and deletes the local database. |

Compose publishes Postgres on `127.0.0.1:5433`, so it does not collide with a
Postgres already on 5432. Set `POSTGRES_HOST_PORT` to change it.

## Deploy to Render

[`render.yaml`](render.yaml) is a Render Blueprint. It defines one background
worker, built from the [`Dockerfile`](Dockerfile).

The Blueprint does not create the database. It connects to an existing Render
Postgres named `sqlflow-demo-rollups` in the `virginia` region. To deploy your
own copy, create a Postgres with that name in that region, or change the name
in `render.yaml`. The worker and the database must share a region, because the
worker uses the database's private connection string.

To deploy:

1. In the Render Dashboard, create a new Blueprint and select this repository.
2. Render reads `render.yaml` and creates the `sqlflow-bluesky` worker.

Render sets `SQLFLOW_POSTGRES_URI` from the database. You enter no secrets.

On every start, the worker applies pending migrations and then runs sqlflow.
A deploy provisions its own schema. Render deploys a commit only after CI
passes.

## Configuration

The worker reads three environment variables:

| Variable | Required | Default |
|---|---|---|
| `SQLFLOW_POSTGRES_URI` | yes | none |
| `SQLFLOW_JETSTREAM_URI` | no | `wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post` |
| `SQLFLOW_LOG_LEVEL` | no | `INFO` |

Without `SQLFLOW_POSTGRES_URI`, the worker exits with code 2 before touching
anything. To connect from outside Render, use the database's external URL and
append `?sslmode=require`.

## Delivery semantics and known gaps

### Postgres writes are at-least-once

sqlflow writes a closed minute to Postgres before deleting it from DuckDB. A
crash between the two writes that minute again on restart. The upsert on
`(bucket, lang)` absorbs the duplicate, so counts stay correct.

### A restart loses data

The websocket source reconnects with backoff, but it does not send a Jetstream
cursor. It rejoins at the live head, so posts published while the worker is
down are never counted. DuckDB state is in memory, so a restart also loses the
minutes still open: the current minute and any minute inside its grace period.

On the demo page, a restart shows as missing minutes. The first minute after a
start is partial.

The fix belongs in sqlflow: persist the last event time and send it as the
Jetstream `cursor` parameter on reconnect.

### A rejected write does not stop the worker

When Postgres rejects a window write, sqlflow logs `poll failed` and retries on
the next poll. It never exits. A write that can never succeed, such as a
constraint violation, therefore publishes nothing while the worker looks
healthy. Watch the logs for `poll failed`. Liveness alone does not show the
failure. The fix belongs in sqlflow.

### A post counts under its first language

A post tagged `["en", "ja"]` counts once, under `en`. A post with no language
tag counts under `unknown`.

### Nothing is deleted

Every minute adds about 33 rows, roughly 47,000 rows a day. The table has no
retention policy yet.

## What comes next

- The demo page, reading `pipeline_status`.
- TurboStats, sqlflow's self-reported process state, to show real uptime
  instead of inferring it from missing minutes.
