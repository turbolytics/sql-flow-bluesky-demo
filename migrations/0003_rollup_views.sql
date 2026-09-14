-- The coarse grains the API serves, one view each. The GROUP BY runs here, in
-- Postgres: DuckDB's postgres extension pushes a filter into a view but runs
-- GROUP BY itself, so aggregating in the serve SQL would copy every minute row
-- to DuckDB first. Measured on a 30-day table: 2.2 s that way, 0.12 s through
-- a view.
--
-- Each bucket is computed in UTC whatever the session's TimeZone.
-- date_trunc's two-argument form truncates in the session zone, and nothing
-- here controls the zone of the connections DuckDB opens. date_bin bins by an
-- absolute interval from its origin, so it has no zone to take.
--
-- Known limit: a filter on a view's grouped bucket does not use the minute
-- table's primary key, so each request scans the minute rows. That is tens of
-- milliseconds at a few weeks of data. Rollup tables are the fix when it shows.
CREATE OR REPLACE VIEW posts_per_5m_by_lang AS
SELECT date_bin('5 minutes', bucket, TIMESTAMPTZ '2000-01-01 00:00:00+00') AS bucket,
       lang,
       sum(posts)::bigint AS posts
FROM posts_per_minute_by_lang
GROUP BY 1, 2;

CREATE OR REPLACE VIEW posts_per_hour_by_lang AS
SELECT date_trunc('hour', bucket, 'UTC') AS bucket,
       lang,
       sum(posts)::bigint AS posts
FROM posts_per_minute_by_lang
GROUP BY 1, 2;

CREATE OR REPLACE VIEW posts_per_day_by_lang AS
SELECT date_trunc('day', bucket, 'UTC') AS bucket,
       lang,
       sum(posts)::bigint AS posts
FROM posts_per_minute_by_lang
GROUP BY 1, 2;
