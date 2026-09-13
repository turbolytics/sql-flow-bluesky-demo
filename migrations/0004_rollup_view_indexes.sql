-- One index per rollup view, on the expression that view groups by.
--
-- A filter on a view's bucket reaches the minute table as a filter on that
-- expression, such as date_trunc('hour', bucket, 'UTC') >= $since. The
-- primary key on bucket cannot serve it, so without these every request read
-- the whole table, and a 12-hour chart slowed down with every day of history.
-- Measured on 880k minute rows: a 12-hour hourly request went from a full
-- scan in 185 ms to an index scan of 17.6k rows in 7 ms.
--
-- Each expression must match its view's exactly, or the planner cannot use
-- the index. Both functions are immutable in these forms, which an index
-- requires: date_bin, and date_trunc with an explicit zone.
--
-- Not CONCURRENTLY: migrate.sh runs each migration in a transaction. A plain
-- build blocks the worker's inserts while it runs, which is under a second at
-- this table's size.
CREATE INDEX IF NOT EXISTS posts_per_minute_by_lang_5m_idx
  ON posts_per_minute_by_lang (date_bin('5 minutes', bucket, TIMESTAMPTZ '2000-01-01 00:00:00+00'));

CREATE INDEX IF NOT EXISTS posts_per_minute_by_lang_hour_idx
  ON posts_per_minute_by_lang (date_trunc('hour', bucket, 'UTC'));

CREATE INDEX IF NOT EXISTS posts_per_minute_by_lang_day_idx
  ON posts_per_minute_by_lang (date_trunc('day', bucket, 'UTC'));
