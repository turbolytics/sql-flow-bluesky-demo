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
4. Every 10 seconds, sqlflow checks the table's window for closed minutes. A
   minute closes once the stream's event time is 60 seconds past the minute's
   end, or once no data has arrived for a minute.
5. sqlflow upserts closed minutes into Postgres, then deletes them from
   DuckDB.

`pipeline.yml` declares the window: the time column, the bucket size, the
grace, the idle bound, and what happens to a late row. sqlflow keeps the
watermark and generates the SQL that collects and deletes closed minutes.

The window writes through sqlflow's `postgres` sink. The sink holds its own
connection to Postgres and upserts each closed minute on `(bucket, lang)` in
one transaction, so a write costs the minute, not the table. An earlier
version wrote through DuckDB's postgres extension. That extension resolves
`ON CONFLICT` by reading every key of the target table into DuckDB, so the
worker's memory grew with the table on every write.

The whole pipeline is [`pipeline.yml`](pipeline.yml). DuckDB runs in memory.
There is no state file.

## What gets stored

One table and four views.

`posts_per_minute_by_lang` holds one row per minute per language:

| Column | Type | Meaning |
|---|---|---|
| `bucket` | `timestamptz` | Start of the minute, in event time. |
| `lang` | `text` | The post's first language tag, or `unknown`. |
| `posts` | `integer` | Posts created in that minute with that language. |
| `updated_at` | `timestamptz` | Wall-clock time the minute was first written, from the Postgres clock. A republished minute keeps it. |

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

Beside the minute table, Postgres keeps ten rollup tables: `posts_by_lang_5m`
through `posts_by_lang_1d`, one row per bucket per language, and
`posts_total_5m` through `posts_total_1d`, one row per bucket with the posts
and the minutes observed in it. Every bucket is UTC. The API reads them, and
`pipeline_status` reads the daily totals instead of scanning the minute table.

Triggers keep them current. When the pipeline writes a closed minute, a
statement-level trigger re-merges the 5-minute buckets that write touched, its
write re-merges the quarter hours, and so on up to the day, all inside the
pipeline's own transaction. A minute written twice replaces its count at every
grain rather than adding to it. Deleting a minute changes no rollup, so the
coarse history outlives any retention on the minute table.

[`rollups.yml`](rollups.yml) declares all of it: the grains, the dimensions,
the measures, and the dataset the API serves. `migrations/0005_rollups.sql`
and `serve.yml`'s `posts_by_lang` dataset are generated from it with
`sqlflow rollup`, and `make validate` fails when either has drifted. Change
the declaration, run `make rollups`, and commit what it writes.

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

## Query it over HTTP

A second service runs `sqlflow serve` with [`serve.yml`](serve.yml) and
exposes the rollups as JSON. This section is the contract a page codes
against.

Every request except `/healthz` sends a bearer token:

```
Authorization: Bearer <token>
```

The token names the caller in the API's log. It is not a secret: a page ships
it in its JavaScript. Browsers may call from `https://turbolytics.io` and
`https://www.turbolytics.io`.

| Route | Returns |
|---|---|
| `GET /healthz` | `{"status":"ok"}`, no token needed. `HEAD` works too, for monitors |
| `GET /metrics` | Prometheus text, no token. Counts and latencies per dataset; no row data |
| `GET /v1/datasets` | Every dataset, its params, grains and SQL |
| `GET /v1/datasets/pipeline_status` | One row: first and latest minute, last write, minutes observed, total posts |
| `GET /v1/datasets/posts_by_lang?since=…&until=…` | Posts per bucket per language |

`posts_by_lang` takes four optional params and an optional `grain`:

| Param | Type | Meaning |
|---|---|---|
| `since` | RFC 3339 timestamp with an offset | Start of the range, inclusive. Encode `+` as `%2B`. Defaults to `until` minus 24 hours. |
| `until` | RFC 3339 timestamp with an offset | End of the range, exclusive. Defaults to the time the request arrived. |
| `lang` | string | One language, never folded into `other`. |
| `top` | integer | How many languages keep their own series. Default 10, and 1 through 20; outside that the request is refused. |
| `grain` | string | Optional. Without it the API picks the finest grain that covers the range. |

Bluesky tags posts with about 170 languages a day, and the top 10 carry 95%
of posts. So the top `top` languages by posts over the whole range keep their
own series, and the rest sum into `lang: "other"`. Every bucket carries the
same series. A language absent from a bucket has no row; draw it as zero.

A request names a time range, and the API answers with the finest grain that
covers it. Each grain serves up to about 300 buckets, which is what a chart
can draw:

| Grain | Reads | Widest range | Points at the widest range |
|---|---|---|---|
| `1m` | `posts_per_minute_by_lang` | 6 hours | 360 |
| `5m` | `posts_by_lang_5m` | 1 day | 288 |
| `15m` | `posts_by_lang_15m` | 3 days | 288 |
| `1h` | `posts_by_lang_1h` | 14 days | 336 |
| `6h` | `posts_by_lang_6h` | 90 days | 360 |
| `1d` | `posts_by_lang_1d` | 365 days | 365 |

So the last hour comes back at `1m` with 60 points, the last 24 hours at `5m`
with 288, the last 7 days at `1h` with 168, and the last 30 days at `6h` with
120. The response says which grain it chose and the range it resolved.

Name a `grain` to pin one. A range wider than that grain's widest, or wider
than every grain's, is `400 range_too_wide`, and the message names the grains
that would serve it. Nothing is cut from the left of a chart without saying
so.

A response holds at most 365 buckets × 21 series, under the 10,000-row limit,
so `truncated` is always `false`.

Output from a local run against two hours of sample minutes:

```
$ curl -H 'Authorization: Bearer local-dev-token' \
    'localhost:8080/v1/datasets/posts_by_lang?grain=1h&top=3&since=2026-09-13T18:00:00Z&until=2026-09-13T20:00:00Z'
{
  "dataset": "posts_by_lang",
  "grain": "1h",
  "range": {"since": "2026-09-13T18:00:00Z", "until": "2026-09-13T20:00:00Z"},
  "columns": [
    {"name": "bucket", "type": "TIMESTAMP WITH TIME ZONE"},
    {"name": "lang", "type": "VARCHAR"},
    {"name": "posts", "type": "BIGINT"}
  ],
  "rows": [
    {"bucket": "2026-09-13T18:00:00Z", "lang": "en", "posts": 102184},
    {"bucket": "2026-09-13T18:00:00Z", "lang": "ja", "posts": 14584},
    {"bucket": "2026-09-13T18:00:00Z", "lang": "other", "posts": 11952},
    {"bucket": "2026-09-13T18:00:00Z", "lang": "unknown", "posts": 34984},
    {"bucket": "2026-09-13T19:00:00Z", "lang": "en", "posts": 102178},
    {"bucket": "2026-09-13T19:00:00Z", "lang": "ja", "posts": 14578},
    {"bucket": "2026-09-13T19:00:00Z", "lang": "other", "posts": 11934},
    {"bucket": "2026-09-13T19:00:00Z", "lang": "unknown", "posts": 34978}
  ],
  "row_count": 8,
  "truncated": false,
  "elapsed_ms": 8
}

$ curl -H 'Authorization: Bearer local-dev-token' \
    localhost:8080/v1/datasets/pipeline_status
{
  "dataset": "pipeline_status",
  "columns": [...],
  "rows": [
    {
      "first_bucket": "2026-09-13T18:00:00Z",
      "latest_bucket": "2026-09-13T19:59:00Z",
      "last_write_at": "2026-09-13T20:01:00Z",
      "minutes_observed": 120,
      "total_posts": 327372
    }
  ],
  "row_count": 1,
  "truncated": false,
  "elapsed_ms": 5
}
```

Buckets and timestamps are UTC. Rows sort by bucket, then language.

`elapsed_ms` is the query. `queued_ms` is how long the request waited for a
free session: the API answers four requests at once, and a fifth waits. Both
are zero-ish until the API is busy, and `queued_ms` is what grows first.

Every refusal has one shape:

```
{"error": {"code": "range_too_wide", "message": "grain 5m serves at most 1d and the range is 168h0m1s; grains that serve it: 1h, 6h, 1d"}}
```

A page should handle three statuses:

| Status | Codes | What happened |
|---|---|---|
| `400` | `range_too_wide`, `unknown_grain`, `unknown_param`, `invalid_param` | The request is wrong. The message names the param, the grain, or the grains that serve the range. |
| `401` | `unauthorized` | No token, or not the configured one. |
| `504` | `query_timeout` | The query took longer than 10 seconds. Retry later. |

A `500` with `query_failed` means the database failed; the API's log has the
cause.

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
... Executing command step {"name": "declare the post schema"}
... starting watermark manager {"table": "posts_per_minute_by_lang", "poll_interval": "10s"}
... throughput {"messages_consumed": 199, "total_throughput_per_second": 40.9}
```

Each minute reaches Postgres about 70 seconds after it ends: 60 seconds of
grace, plus up to one 10-second poll. The first minute is partial, because the worker
started partway through it.

In a second terminal, open a Postgres shell and query:

```
make psql
```

To run only the API against the local database:

```
make serve
```

It listens on `127.0.0.1:8080` with the token `local-dev-token`, which exists
only in `docker-compose.yml`. `make run` starts the API beside the pipeline.

Other targets:

| Target | Does |
|---|---|
| `make validate` | Checks every config against the pinned sqlflow image, and that the generated rollup files still match `rollups.yml`. |
| `make rollups` | Regenerates `migrations/0005_rollups.sql` and prints the `posts_by_lang` dataset to paste into `serve.yml`. |
| `make migrate` | Applies migrations without starting the pipeline. |
| `make serve` | Starts Postgres and the API. |
| `make image` | Builds the image the worker and the API share. |
| `make clean` | Stops everything and deletes the local database. |

To try an unreleased sqlflow build, set `SQLFLOW_IMAGE` to its image tag for
`make run`, `make serve` or `make image`.

Compose publishes Postgres on `127.0.0.1:5433`, so it does not collide with a
Postgres already on 5432. Set `POSTGRES_HOST_PORT` to change it.

## Deploy to Render

[`render.yaml`](render.yaml) is a Render Blueprint. It defines two services,
both built from the [`Dockerfile`](Dockerfile): the `sqlflow-bluesky`
background worker, and the `sqlflow-bluesky-api` web service, which runs
`bin/serve.sh` instead of the worker's entrypoint.

The Blueprint does not create the database. It connects to an existing Render
Postgres named `sqlflow-demo-rollups` in the `virginia` region. To deploy your
own copy, create a Postgres with that name in that region, or change the name
in `render.yaml`. The worker and the database must share a region, because the
worker uses the database's private connection string.

To deploy:

1. In the Render Dashboard, create a new Blueprint and select this repository.
2. Render reads `render.yaml` and creates the worker and the API.

Render sets `SQLFLOW_POSTGRES_URI` from the database for both services. You
enter no secrets.

Render mints the API's token, `SQLFLOW_SERVE_TOKEN_BLUESKY_DEMO`, once, when
it creates the service. Copy it from the API's Environment page into the page
that calls the API. To rotate it, edit the variable, redeploy the API, and
update the page.

On every start, both services apply pending migrations, then run sqlflow. The
migration script takes a lock, so the two can start together. Render deploys
a commit only after CI passes.

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

The API reads four:

| Variable | Required | Default |
|---|---|---|
| `SQLFLOW_POSTGRES_URI` | yes | none |
| `SQLFLOW_SERVE_TOKEN_BLUESKY_DEMO` | yes | none |
| `SQLFLOW_SERVE_PORT` | no | `8080` |
| `SQLFLOW_LOG_LEVEL` | no | `INFO` |

Without either required variable, the API exits with code 2.

## Delivery semantics and known gaps

### Postgres writes are at-least-once

sqlflow writes a closed minute to Postgres before deleting it from DuckDB. The
same minute can be written twice: the sink retries a write that timed out,
which may already have landed, and a close whose delete conflicts with the
pipeline rolls back and publishes the minute again on the next poll. The upsert
on `(bucket, lang)` absorbs the duplicate, so counts stay correct.

### A late post is dropped

A post for a minute that already closed is late. sqlflow discards it and logs
`dropped late rows` with the count. The published minute keeps the count it
closed with.

The pipeline declares `late_rows: drop` on purpose. The alternative, `reemit`,
publishes the late posts as the minute's rows, and the upsert replaces the
minute's count with theirs. In a local run, one late post turned a published
count of 5 into 1.

### A restart loses data

The websocket source reconnects with backoff, but it does not send a Jetstream
cursor. It rejoins at the live head, so posts published while the worker is
down are never counted. DuckDB state is in memory, so a restart also loses the
minutes still open: the current minute and any minute inside its grace period.

On the demo page, a restart shows as missing minutes. The first minute after a
start is partial.

The fix belongs in sqlflow: persist the last event time and send it as the
Jetstream `cursor` parameter on reconnect.

### A rejected write stops the worker

When Postgres rejects a window write, sqlflow logs `table manager failed,
pipeline stopped` and exits with code 1. Render restarts the worker. A write
that can never succeed, such as a constraint violation, stops the worker again
at the first closed minute after each restart. The failure shows as a restart
loop in the Render dashboard, and each restart loses the minutes held in
memory.

### A post counts under its first language

A post tagged `["en", "ja"]` counts once, under `en`. A post with no language
tag counts under `unknown`.

### Nothing is deleted

Every minute adds about 33 rows, roughly 47,000 rows a day. The table has no
retention policy yet.

## What comes next

- The demo page, reading the API.
- A pool of DuckDB sessions in `sqlflow serve`. The API answers one request at
  a time, so a slow request makes every request behind it wait; at 16
  concurrent clients two thirds of requests timed out. The rollups cut what
  one request costs, and a pool is what lets several run at once.
- Retention on the minute table. Every minute adds about 33 rows, and the
  rollups survive a delete, so the coarse history keeps its shape.
- TurboStats, sqlflow's self-reported process state, to show real uptime
  instead of inferring it from missing minutes.
