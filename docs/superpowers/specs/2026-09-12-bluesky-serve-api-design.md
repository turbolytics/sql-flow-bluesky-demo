# Bluesky rollups over HTTP: design

A second Render service runs `sqlflow serve` against the same Postgres the
pipeline writes to, and exposes the rollups as a small JSON API. The page at
turbolytics.io/bluesky-demo calls it directly from the browser with a bearer
token. This repository holds the serve config, the rollup views, and the
deploy config for the new service. The page is not in this repository; the
API contract is.

Depends on sql-flow's `serve` command, designed in
`sql-flow/docs/superpowers/specs/2026-09-12-serve-design.md`. Implementation
here waits on the sql-flow release that ships it.

## Why

The pipeline PR made the data exist. This PR makes it reachable without a
database client. The point for a reader is that the config that streams the
data and the config that serves it are the same product: same file format,
same template variables, same `commands:` block, same image.

## Decisions

Made in the design conversation. Each one closes a fork.

| Decision | Choice | Rejected |
|---|---|---|
| Where it runs | A new Render web service, `sqlflow-bluesky-api`, from the same Dockerfile with a different command. | Adding HTTP to the worker. Only a web service gets a public URL, and the worker's job is to never stop streaming. |
| Where the coarse grains aggregate | Postgres views, one per grain, in a migration. The serve SQL is a filtered select over each view. | Aggregating in DuckDB from the minute table. Measured at 2.2 s for a 30-day daily rollup against 0.12 s through a view, because DuckDB pushes filters to Postgres but not `GROUP BY`. |
| Token | One identity, `bluesky-demo-page`. Render mints the value with `generateValue`. It is copied into the page's JS by hand. | Committing a token. The repository is public. A proxy holding a secret: the page then depends on a third service. |
| Migrations | Both services run `bin/migrate.sh` at start. | Worker only. The script takes an advisory lock and applies each file in one transaction, so two instances at once are safe, and the API then never waits on the worker's deploy to find its views. |
| Caching and rate limits | None in v1. `Cache-Control: no-store`. | Both are sql-flow follow-ups. The demo's load is a page poll, and the views keep each request cheap. |
| Time range | Each grain's SQL defaults and clamps its own range. | Server-side range limits. The SQL is the author's; a clamp in `coalesce` and `greatest` is one line. |
| Grains served | `5m`, `1h`, `1d`, each from a view. The minute table is not served. | A `1m` grain over the raw table. The API starts at the coarsest useful grain; a minute grain is a later addition if the page needs it, and it needs no new view. |

## Data flow

1. The browser sends `GET /v1/datasets/posts_by_lang?grain=1h&since=…` with
   `Authorization: Bearer …` to the API's Render URL.
2. sqlflow checks the token, parses `since` as a timestamp, binds it to the
   prepared statement for grain `1h`.
3. DuckDB scans `pg.posts_per_hour_by_lang` through the Postgres extension,
   pushing the `bucket >= …` filter into the view.
4. Postgres runs the view's `GROUP BY` under the filter and streams the
   result rows back.
5. sqlflow encodes the rows as JSON objects and answers.

Verified on DuckDB v1.5.2 against a 30-day synthetic table of 432k rows.
DuckDB sent this to Postgres for the daily grain:

```
COPY (SELECT "bucket", "lang", "posts" FROM "public"."posts_per_day_by_lang"
      WHERE "bucket" >= '2026-08-14 …') TO STDOUT (FORMAT "binary")
```

300 rows crossed the wire. Without the view, 432k rows crossed and DuckDB
grouped them.

Re-measured at implementation with `serve.yml` as written, through
`sqlflow serve`, against 40 days of synthetic minutes. DuckDB folds the
`greatest`, `coalesce`, `now()` and bound `since` into literals, so every grain
pushes its range into the view:

```
COPY (SELECT "bucket", "lang", "posts" FROM "public"."posts_per_hour_by_lang"
      WHERE ("bucket" >= '2026-09-10 20:26:27+00' AND "bucket" < '2026-09-13 20:26:27.664313+00'))
```

Each grain's default range answered in 33 to 193 ms. The `lang` filter does
not push down: `lang = coalesce($lang, lang)` is applied in DuckDB, so a
one-language request copies every language's rows for the range. At about 33
languages that is a few thousand rows, and not worth a second statement per
grain.

The API holds at most `pg_connection_limit` Postgres connections. After 30
sequential and then 30 concurrent requests, `pg_stat_activity` showed the same
four backends, opened on the first requests and reused. Requests share one
DuckDB connection and run one at a time.

## Files

```
serve.yml                                      new
migrations/0003_rollup_views.sql               new
bin/serve.sh                                   new
bin/entrypoint.sh                              unchanged
Dockerfile                                     copies serve.yml; bumps the pin
Makefile                                       bumps the pin; validate, serve
docker-compose.yml                             api service
render.yaml                                    api service
.github/workflows/ci.yml                       validate serve.yml; smoke job
README.md                                      "Query it over HTTP"
docs/superpowers/specs/2026-09-12-bluesky-serve-api-design.md
```

## migrations/0003_rollup_views.sql

Three views, `CREATE OR REPLACE` so the apply-twice check in CI passes:

```sql
CREATE OR REPLACE VIEW posts_per_5m_by_lang AS
SELECT date_bin('5 minutes', bucket, TIMESTAMPTZ '2000-01-01 00:00:00+00') AS bucket,
       lang,
       sum(posts)::bigint AS posts
FROM posts_per_minute_by_lang
GROUP BY 1, 2;

CREATE OR REPLACE VIEW posts_per_hour_by_lang AS
SELECT date_trunc('hour', bucket, 'UTC') AS bucket, lang, sum(posts)::bigint AS posts
FROM posts_per_minute_by_lang
GROUP BY 1, 2;

CREATE OR REPLACE VIEW posts_per_day_by_lang AS
SELECT date_trunc('day', bucket, 'UTC') AS bucket, lang, sum(posts)::bigint AS posts
FROM posts_per_minute_by_lang
GROUP BY 1, 2;
```

`date_trunc`'s two-argument form truncates a `TIMESTAMPTZ` in the session
zone, and nothing here controls the zone of the connections DuckDB opens. The
three-argument form, in Postgres since 12, truncates in UTC whatever the
session says. `date_bin` bins by an absolute interval from its origin and has
no zone to take. Measured on Postgres 18 from an `Asia/Kolkata` session: the
two-argument form put the day bucket at 18:30 UTC and the hour at :30, and the
three-argument form put both on the UTC boundary. CI reads the views back from
that zone.

A filter on a view's bucket reaches the minute table as a filter on the
view's expression, such as `date_trunc('hour', bucket, 'UTC') >= …`, which the
primary key on `bucket` cannot serve. Without help every request read the
whole table, so a 12-hour chart slowed with every day of history.
`migrations/0004_rollup_view_indexes.sql` adds one index per view on exactly
that expression; both functions are immutable in these forms. Measured on
880k minute rows over 40 days:

| Request | Without | With |
|---|---|---|
| `5m`, 12 h | 33 ms, full scan | 4.5 ms, 17.8k rows |
| `1h`, 12 h | 185 ms, full scan | 7 ms, 17.6k rows |
| `1h`, 7 d | 345 ms | 141 ms, 321k rows |
| `1d`, 30 d | 462 ms | 277 ms, still a scan of most of the table |

Each index was about 6 MB. CI plans each view with sequential scans off and
fails unless it uses its index, which catches an index expression that drifts
from its view's.

Known limit: a range covering most of the history still reads most of the
minute table, so a `1d` request over a year of data approaches seconds. The
fix when it matters is rollup tables the pipeline maintains. Not this PR.

## serve.yml

Template variables:

| Variable | Required | Default |
|---|---|---|
| `SQLFLOW_POSTGRES_URI` | yes | none |
| `SQLFLOW_SERVE_TOKEN_BLUESKY_DEMO` | yes | none |
| `SQLFLOW_SERVE_PORT` | no | `8080` |

```yaml
# Serves the rollups the pipeline writes. Read-only against the same Postgres.
commands:
  - name: pin the session timezone
    sql: |
      SET TimeZone='UTC';

  - name: load postgres extension
    sql: |
      INSTALL postgres;
      LOAD postgres;

  # One scan fans out across ctid ranges, each on its own Postgres
  # connection. The default cap is 64. The Render plan allows far fewer.
  - name: bound the postgres connections one scan may open
    sql: |
      SET pg_connection_limit = 4;

  # READ_ONLY is the guard. A write in a dataset's SQL fails here, not in
  # the table.
  - name: attach postgres
    sql: |
      ATTACH '{{ SQLFLOW_POSTGRES_URI }}' AS pg (TYPE POSTGRES, READ_ONLY);

serve:
  name: bluesky-demo-api

  http:
    addr: "0.0.0.0:{{ SQLFLOW_SERVE_PORT|default('8080') }}"
    cors:
      allowed_origins:
        - https://turbolytics.io
        - https://www.turbolytics.io

  auth:
    tokens:
      # The identity, not a secret. The page ships it in its JS. It exists
      # so the log names the caller and so the value can be rotated.
      - name: bluesky-demo-page
        token: "{{ SQLFLOW_SERVE_TOKEN_BLUESKY_DEMO }}"

  limits:
    max_rows: 10000
    timeout_seconds: 10

  datasets:
    - name: pipeline_status
      description: First and latest minute, last write, minutes observed, total posts. One row.
      sql: |
        SELECT first_bucket, latest_bucket, last_write_at,
               minutes_observed, total_posts
        FROM pg.pipeline_status

    - name: posts_by_lang
      description: Posts per bucket per language. since and until bound the range; lang filters to one language.
      params:
        - {name: since, type: timestamp}
        - {name: until, type: timestamp}
        - {name: lang,  type: string}
      grains:
        # Each grain reads a view, so the GROUP BY runs in Postgres. The
        # minute table is the pipeline's output and is not served; the
        # coarsest useful grain is where the API starts.
        #
        # Each grain defaults to a range it can afford and clamps a wider
        # request to a ceiling. The clamp is greatest(): a since earlier than
        # the ceiling is raised to it.
        5m:
          sql: |
            SELECT bucket, lang, posts
            FROM pg.posts_per_5m_by_lang
            WHERE bucket >= greatest(coalesce($since, now() - INTERVAL '12 hours'), now() - INTERVAL '7 days')
              AND bucket <  coalesce($until, now())
              AND lang = coalesce($lang, lang)
            ORDER BY bucket, lang
        1h:
          sql: |
            SELECT bucket, lang, posts
            FROM pg.posts_per_hour_by_lang
            WHERE bucket >= greatest(coalesce($since, now() - INTERVAL '7 days'),   now() - INTERVAL '30 days')
              AND bucket <  coalesce($until, now())
              AND lang = coalesce($lang, lang)
            ORDER BY bucket, lang
        1d:
          sql: |
            SELECT bucket, lang, posts
            FROM pg.posts_per_day_by_lang
            WHERE bucket >= greatest(coalesce($since, now() - INTERVAL '30 days'),  now() - INTERVAL '365 days')
              AND bucket <  coalesce($until, now())
              AND lang = coalesce($lang, lang)
            ORDER BY bucket, lang
```

Row budget per grain. Prod on 2026-09-13 carried 170 languages in a day,
with the top 10 at 95% of posts. Serving every language put a 7-day hourly
response at about 16,000 rows, past `max_rows`, within three days of data. So
`posts_by_lang` keeps the top `top` languages by posts over the requested
range, default 10 and clamped to 1 through 50, and sums the rest into
`lang: "other"`. Every bucket carries the same series, because the ranking is
over the whole range. A request with `lang` returns that language unfolded.
The fold is window functions in DuckDB over rows the view already filtered;
Postgres still receives one `COPY` with the range pushed in.

| Grain | Default | Rows at top 10 | Ceiling | Rows at top 10 |
|---|---|---|---|---|
| `5m` | 12 h | 1,584 | 7 d | 22,176 |
| `1h` | 7 d | 1,848 | 30 d | 7,920 |
| `1d` | 30 d | 330 | 365 d | 4,015 |

Every default fits under `max_rows` with room. Only the `5m` ceiling exceeds
it, so a caller asking for all 7 days at 5 minutes gets `truncated: true` and
the earliest 10,000 rows.

Verified against Postgres 18 with 161 languages: series per bucket is 11,
`top=3` gives 4, `top=0` clamps to 2, `top=999` to 51, `lang=tail007`
returns that language alone, and the API's total over a window and one
bucket's `other` both equal the same sums computed in Postgres.

## bin/serve.sh

```bash
#!/usr/bin/env bash
# The API's entrypoint: provision the schema, then serve.
set -euo pipefail

if [ -z "${SQLFLOW_POSTGRES_URI:-}" ]; then
  echo "SQLFLOW_POSTGRES_URI is not set" >&2
  exit 2
fi
if [ -z "${SQLFLOW_SERVE_TOKEN_BLUESKY_DEMO:-}" ]; then
  echo "SQLFLOW_SERVE_TOKEN_BLUESKY_DEMO is not set" >&2
  exit 2
fi

/app/bin/migrate.sh

exec sqlflow serve -c /app/serve.yml "$@"
```

Same shape as `entrypoint.sh`: check the variables whose absence sqlflow
reports badly, migrate, `exec` so SIGTERM reaches sqlflow and the drain runs.

## Dockerfile

Three changes: `COPY serve.yml /app/serve.yml`; the pin moves from `v1.2.0`
to `v1.3.0`, the sql-flow release that ships `serve`; and the base image is an
`ARG SQLFLOW_IMAGE` defaulting to that pin, so an unreleased sqlflow build can
be tried locally with `--build-arg`. Render and CI build with the default. The
`ENTRYPOINT` stays `entrypoint.sh`. Render's `dockerCommand` overrides it for
the API.

## render.yaml

A second entry under `services`:

```yaml
  - type: web
    name: sqlflow-bluesky-api
    runtime: docker
    dockerfilePath: ./Dockerfile
    dockerCommand: /app/bin/serve.sh
    plan: 0.5c-512mb
    region: virginia
    healthCheckPath: /healthz
    autoDeployTrigger: checksPass
    envVars:
      - key: SQLFLOW_POSTGRES_URI
        fromDatabase:
          name: sqlflow-demo-rollups
          property: connectionString
      - key: SQLFLOW_SERVE_TOKEN_BLUESKY_DEMO
        generateValue: true
      - key: SQLFLOW_SERVE_PORT
        value: "8080"
      - key: PORT
        value: "8080"
      - key: SQLFLOW_LOG_LEVEL
        value: INFO
```

`PORT` is set so Render routes to the port sqlflow listens on rather than
scanning for one. The plan is the worker's, and the whole file is checked
against Render's Blueprint JSON Schema at implementation, since plan names
have changed before. The worker entry is unchanged.

`generateValue` makes Render mint the token once, at service creation. It
shows in the dashboard, and that is where it is copied from into the page.
Rotating it is editing the variable and redeploying, then updating the page.

## docker-compose.yml

An `api` service:

```yaml
  api:
    build: .
    entrypoint: /app/bin/serve.sh
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      SQLFLOW_POSTGRES_URI: postgresql://bluesky:bluesky@postgres:5432/bluesky
      SQLFLOW_SERVE_TOKEN_BLUESKY_DEMO: local-dev-token
      SQLFLOW_LOG_LEVEL: INFO
    ports:
      - "127.0.0.1:${API_HOST_PORT:-8080}:8080"
```

The local token is a fixed string so the README's `curl` lines work as
written. It is not a secret and does not exist on Render.

## Makefile

- `SQLFLOW_IMAGE` bumps to the release.
- `validate` also runs `validate /app/serve.yml`.
- `serve`: `docker compose up --build postgres api`.
- `run` is unchanged and still starts every service, so `make run` brings up
  the pipeline and the API together.

## CI

`image` job: validate `serve.yml` beside `pipeline.yml`.

`migrations` job: the schema check also selects from each of the three views
with `WHERE false`, and asserts one row's day bucket is midnight UTC after
inserting a known minute.

New `api` job: build the image, start Postgres, run `bin/serve.sh` in the
container with a fixed token, wait on `/healthz`, then:

- `GET /v1/datasets` lists `pipeline_status` and `posts_by_lang` with three
  grains, `5m`, `1h`, `1d`, and no `1m`.
- After inserting three minutes in one hour, two `en` and one `ja`,
  `grain=1h&top=1` returns `en` as their sum and `ja` as `other`, and
  `lang=ja` returns `ja` unfolded.
- `pipeline_status` counts the inserted minutes and posts.
- `GET /v1/datasets/posts_by_lang` without a grain is `400 missing_grain`.
- No token is `401`.

Run locally before the PR, with the image built on a local sqlflow build of
main: every assertion passed, and changing the expected `other` sum made the
job fail.

This job is the only place the attachment path is exercised end to end.
sql-flow's own tests run without Postgres.

## README

A "Query it over HTTP" section between "What gets stored" and "Run it
locally". It is the contract the page's JS codes against:

- The base URL on Render and the header to send.
- One `curl` per route with its JSON, taken from a real run.
- The params table and each grain's default and ceiling.
- The error shape and the codes the page should handle: `401`, `400`, `504`.
- A sentence on `truncated`.

"Deploy to Render" gains the second service, where the token comes from, and
how to rotate it. "Run it locally" gains `make serve`.

## Verification

Before the PR opens:

- `make validate` passes both files against the pinned image.
- `make serve`, then each README `curl` line returns what the README shows.
- The CI `api` job passes.
- A request with `since` earlier than the grain's ceiling returns rows no
  older than the ceiling.
- `render.yaml` validates against Render's Blueprint schema.

After deploy:

- `/healthz` on the Render URL is `200`.
- A `fetch` from the browser console on turbolytics.io with the minted token
  returns rows, and the same `fetch` from another origin is blocked by CORS.

## Out of scope

- The page itself. It lives with turbolytics.io and codes against the README.
- Caching, rate limiting, a connection pool. sql-flow follow-ups.
- Rollup tables in place of views. When the scan cost shows.
- A second identity. The token list holds one until there is a reason.
