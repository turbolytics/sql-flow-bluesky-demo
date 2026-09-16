-- pipeline_status without scanning the minute table.
--
-- The view 0002 created read every minute for min, max, count and sum, so each
-- request cost more with every day of history: 214 ms at 2.5 days on Render.
-- The same columns and types, so CREATE OR REPLACE VIEW accepts it and the
-- API's contract does not change:
--
--   first_bucket, latest_bucket  one end of the minute table's primary key
--   last_write_at                the newest updated_at in the last hour of
--                                minutes; updated_at is a minute's first write,
--                                so the newest one is always among them
--   minutes_observed, total_posts  sums over posts_total_1d, one row per day,
--                                  kept by 0005's triggers
--
-- sum over bigint is numeric in Postgres, so the casts keep each column's type.
CREATE OR REPLACE VIEW pipeline_status AS
SELECT
  (SELECT min(bucket) FROM posts_per_minute_by_lang) AS first_bucket,
  (SELECT max(bucket) FROM posts_per_minute_by_lang) AS latest_bucket,
  (SELECT max(updated_at) FROM posts_per_minute_by_lang
     WHERE bucket >= (SELECT max(bucket) FROM posts_per_minute_by_lang) - INTERVAL '1 hour')
                                                               AS last_write_at,
  (SELECT coalesce(sum(minutes), 0)::bigint FROM posts_total_1d) AS minutes_observed,
  (SELECT coalesce(sum(posts), 0)::bigint FROM posts_total_1d)   AS total_posts;
