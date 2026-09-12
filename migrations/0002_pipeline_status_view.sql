-- The contract the demo page reads. Kept as its own migration so a later
-- change replaces the view without touching the table.
--
-- coalesce keeps total_posts numeric on an empty table, so a reader never
-- handles a null there.
CREATE OR REPLACE VIEW pipeline_status AS
SELECT
  min(bucket)             AS first_bucket,
  max(bucket)             AS latest_bucket,
  max(updated_at)         AS last_write_at,
  count(DISTINCT bucket)  AS minutes_observed,
  coalesce(sum(posts), 0) AS total_posts
FROM posts_per_minute_by_lang;
