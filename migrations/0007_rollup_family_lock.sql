-- Replaces the rollup trigger functions with the ones the pinned sqlflow
-- generates. 0005 already ran on every database that exists, and migrate.sh
-- applies a file exactly once, so regenerating 0005 alone would leave the live
-- triggers as they were.
--
-- What changed: each function now takes one advisory lock per rollup family
-- before its per-bucket locks, so two writers touching different buckets of
-- the same family cannot interleave into a grain that reads another grain's
-- table. See turbolytics/sql-flow#401.
--
-- The bodies below are the function and trigger halves of 0005, copied
-- verbatim. Deliberately not the whole file: the tables and their indexes
-- already exist, and 0005's backfill would rebuild every grain from the whole
-- minute table under a SHARE ROW EXCLUSIVE lock, blocking the worker's writes
-- for as long as that takes. Replacing a function needs no lock and no
-- rebuild, because the grains' contents do not change.

CREATE OR REPLACE FUNCTION "sqlflow_rollup_posts_by_lang_5m"() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  -- The lock serializes writers only because each statement in READ COMMITTED
  -- takes a new snapshot, which sees the writer the lock waited on.
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'sqlflow rollup posts_by_lang_5m needs READ COMMITTED, not %', current_setting('transaction_isolation');
  END IF;
  IF current_setting('sqlflow.rollup_backfill', true) IS DISTINCT FROM 'on' THEN
    PERFORM pg_advisory_xact_lock(hashtextextended('sqlflow_rollup:posts', 0));
    PERFORM pg_advisory_xact_lock(hashtextextended('posts_by_lang_5m:' || extract(epoch FROM touched.b)::bigint, 0))
    FROM (SELECT DISTINCT date_bin(INTERVAL '5 minutes', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed ORDER BY 1) AS touched;
  END IF;

  INSERT INTO "posts_by_lang_5m" ("bucket", "lang", "posts")
  SELECT date_bin(INTERVAL '5 minutes', f."bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00'), f."lang", sum(f."posts")::bigint
  FROM "posts_per_minute_by_lang" AS f
  JOIN (SELECT DISTINCT date_bin(INTERVAL '5 minutes', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed) AS touched
    ON f."bucket" >= touched.b AND f."bucket" < touched.b + INTERVAL '5 minutes'
  GROUP BY 1, 2
  ON CONFLICT ("bucket", "lang") DO UPDATE SET "posts" = excluded."posts";
  RETURN NULL;
END $fn$;
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_by_lang_5m_ins"
  AFTER INSERT ON "posts_per_minute_by_lang"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_by_lang_5m"();
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_by_lang_5m_upd"
  AFTER UPDATE ON "posts_per_minute_by_lang"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_by_lang_5m"();

CREATE OR REPLACE FUNCTION "sqlflow_rollup_posts_by_lang_15m"() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  -- The lock serializes writers only because each statement in READ COMMITTED
  -- takes a new snapshot, which sees the writer the lock waited on.
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'sqlflow rollup posts_by_lang_15m needs READ COMMITTED, not %', current_setting('transaction_isolation');
  END IF;
  IF current_setting('sqlflow.rollup_backfill', true) IS DISTINCT FROM 'on' THEN
    PERFORM pg_advisory_xact_lock(hashtextextended('sqlflow_rollup:posts', 0));
    PERFORM pg_advisory_xact_lock(hashtextextended('posts_by_lang_15m:' || extract(epoch FROM touched.b)::bigint, 0))
    FROM (SELECT DISTINCT date_bin(INTERVAL '15 minutes', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed ORDER BY 1) AS touched;
  END IF;

  INSERT INTO "posts_by_lang_15m" ("bucket", "lang", "posts")
  SELECT date_bin(INTERVAL '15 minutes', f."bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00'), f."lang", sum(f."posts")::bigint
  FROM "posts_by_lang_5m" AS f
  JOIN (SELECT DISTINCT date_bin(INTERVAL '15 minutes', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed) AS touched
    ON f."bucket" >= touched.b AND f."bucket" < touched.b + INTERVAL '15 minutes'
  GROUP BY 1, 2
  ON CONFLICT ("bucket", "lang") DO UPDATE SET "posts" = excluded."posts";
  RETURN NULL;
END $fn$;
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_by_lang_15m_ins"
  AFTER INSERT ON "posts_by_lang_5m"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_by_lang_15m"();
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_by_lang_15m_upd"
  AFTER UPDATE ON "posts_by_lang_5m"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_by_lang_15m"();

CREATE OR REPLACE FUNCTION "sqlflow_rollup_posts_by_lang_1h"() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  -- The lock serializes writers only because each statement in READ COMMITTED
  -- takes a new snapshot, which sees the writer the lock waited on.
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'sqlflow rollup posts_by_lang_1h needs READ COMMITTED, not %', current_setting('transaction_isolation');
  END IF;
  IF current_setting('sqlflow.rollup_backfill', true) IS DISTINCT FROM 'on' THEN
    PERFORM pg_advisory_xact_lock(hashtextextended('sqlflow_rollup:posts', 0));
    PERFORM pg_advisory_xact_lock(hashtextextended('posts_by_lang_1h:' || extract(epoch FROM touched.b)::bigint, 0))
    FROM (SELECT DISTINCT date_bin(INTERVAL '1 hours', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed ORDER BY 1) AS touched;
  END IF;

  INSERT INTO "posts_by_lang_1h" ("bucket", "lang", "posts")
  SELECT date_bin(INTERVAL '1 hours', f."bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00'), f."lang", sum(f."posts")::bigint
  FROM "posts_by_lang_15m" AS f
  JOIN (SELECT DISTINCT date_bin(INTERVAL '1 hours', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed) AS touched
    ON f."bucket" >= touched.b AND f."bucket" < touched.b + INTERVAL '1 hours'
  GROUP BY 1, 2
  ON CONFLICT ("bucket", "lang") DO UPDATE SET "posts" = excluded."posts";
  RETURN NULL;
END $fn$;
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_by_lang_1h_ins"
  AFTER INSERT ON "posts_by_lang_15m"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_by_lang_1h"();
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_by_lang_1h_upd"
  AFTER UPDATE ON "posts_by_lang_15m"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_by_lang_1h"();

CREATE OR REPLACE FUNCTION "sqlflow_rollup_posts_by_lang_6h"() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  -- The lock serializes writers only because each statement in READ COMMITTED
  -- takes a new snapshot, which sees the writer the lock waited on.
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'sqlflow rollup posts_by_lang_6h needs READ COMMITTED, not %', current_setting('transaction_isolation');
  END IF;
  IF current_setting('sqlflow.rollup_backfill', true) IS DISTINCT FROM 'on' THEN
    PERFORM pg_advisory_xact_lock(hashtextextended('sqlflow_rollup:posts', 0));
    PERFORM pg_advisory_xact_lock(hashtextextended('posts_by_lang_6h:' || extract(epoch FROM touched.b)::bigint, 0))
    FROM (SELECT DISTINCT date_bin(INTERVAL '6 hours', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed ORDER BY 1) AS touched;
  END IF;

  INSERT INTO "posts_by_lang_6h" ("bucket", "lang", "posts")
  SELECT date_bin(INTERVAL '6 hours', f."bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00'), f."lang", sum(f."posts")::bigint
  FROM "posts_by_lang_1h" AS f
  JOIN (SELECT DISTINCT date_bin(INTERVAL '6 hours', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed) AS touched
    ON f."bucket" >= touched.b AND f."bucket" < touched.b + INTERVAL '6 hours'
  GROUP BY 1, 2
  ON CONFLICT ("bucket", "lang") DO UPDATE SET "posts" = excluded."posts";
  RETURN NULL;
END $fn$;
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_by_lang_6h_ins"
  AFTER INSERT ON "posts_by_lang_1h"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_by_lang_6h"();
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_by_lang_6h_upd"
  AFTER UPDATE ON "posts_by_lang_1h"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_by_lang_6h"();

CREATE OR REPLACE FUNCTION "sqlflow_rollup_posts_by_lang_1d"() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  -- The lock serializes writers only because each statement in READ COMMITTED
  -- takes a new snapshot, which sees the writer the lock waited on.
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'sqlflow rollup posts_by_lang_1d needs READ COMMITTED, not %', current_setting('transaction_isolation');
  END IF;
  IF current_setting('sqlflow.rollup_backfill', true) IS DISTINCT FROM 'on' THEN
    PERFORM pg_advisory_xact_lock(hashtextextended('sqlflow_rollup:posts', 0));
    PERFORM pg_advisory_xact_lock(hashtextextended('posts_by_lang_1d:' || extract(epoch FROM touched.b)::bigint, 0))
    FROM (SELECT DISTINCT date_bin(INTERVAL '24 hours', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed ORDER BY 1) AS touched;
  END IF;

  INSERT INTO "posts_by_lang_1d" ("bucket", "lang", "posts")
  SELECT date_bin(INTERVAL '24 hours', f."bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00'), f."lang", sum(f."posts")::bigint
  FROM "posts_by_lang_6h" AS f
  JOIN (SELECT DISTINCT date_bin(INTERVAL '24 hours', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed) AS touched
    ON f."bucket" >= touched.b AND f."bucket" < touched.b + INTERVAL '24 hours'
  GROUP BY 1, 2
  ON CONFLICT ("bucket", "lang") DO UPDATE SET "posts" = excluded."posts";
  RETURN NULL;
END $fn$;
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_by_lang_1d_ins"
  AFTER INSERT ON "posts_by_lang_6h"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_by_lang_1d"();
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_by_lang_1d_upd"
  AFTER UPDATE ON "posts_by_lang_6h"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_by_lang_1d"();

CREATE OR REPLACE FUNCTION "sqlflow_rollup_posts_total_5m"() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  -- The lock serializes writers only because each statement in READ COMMITTED
  -- takes a new snapshot, which sees the writer the lock waited on.
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'sqlflow rollup posts_total_5m needs READ COMMITTED, not %', current_setting('transaction_isolation');
  END IF;
  IF current_setting('sqlflow.rollup_backfill', true) IS DISTINCT FROM 'on' THEN
    PERFORM pg_advisory_xact_lock(hashtextextended('sqlflow_rollup:posts', 0));
    PERFORM pg_advisory_xact_lock(hashtextextended('posts_total_5m:' || extract(epoch FROM touched.b)::bigint, 0))
    FROM (SELECT DISTINCT date_bin(INTERVAL '5 minutes', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed ORDER BY 1) AS touched;
  END IF;

  INSERT INTO "posts_total_5m" ("bucket", "minutes", "posts")
  SELECT date_bin(INTERVAL '5 minutes', f."bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00'), count(DISTINCT f."bucket"), sum(f."posts")::bigint
  FROM "posts_per_minute_by_lang" AS f
  JOIN (SELECT DISTINCT date_bin(INTERVAL '5 minutes', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed) AS touched
    ON f."bucket" >= touched.b AND f."bucket" < touched.b + INTERVAL '5 minutes'
  GROUP BY 1
  ON CONFLICT ("bucket") DO UPDATE SET "minutes" = excluded."minutes", "posts" = excluded."posts";
  RETURN NULL;
END $fn$;
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_total_5m_ins"
  AFTER INSERT ON "posts_per_minute_by_lang"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_total_5m"();
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_total_5m_upd"
  AFTER UPDATE ON "posts_per_minute_by_lang"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_total_5m"();

CREATE OR REPLACE FUNCTION "sqlflow_rollup_posts_total_15m"() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  -- The lock serializes writers only because each statement in READ COMMITTED
  -- takes a new snapshot, which sees the writer the lock waited on.
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'sqlflow rollup posts_total_15m needs READ COMMITTED, not %', current_setting('transaction_isolation');
  END IF;
  IF current_setting('sqlflow.rollup_backfill', true) IS DISTINCT FROM 'on' THEN
    PERFORM pg_advisory_xact_lock(hashtextextended('sqlflow_rollup:posts', 0));
    PERFORM pg_advisory_xact_lock(hashtextextended('posts_total_15m:' || extract(epoch FROM touched.b)::bigint, 0))
    FROM (SELECT DISTINCT date_bin(INTERVAL '15 minutes', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed ORDER BY 1) AS touched;
  END IF;

  INSERT INTO "posts_total_15m" ("bucket", "minutes", "posts")
  SELECT date_bin(INTERVAL '15 minutes', f."bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00'), sum(f."minutes")::bigint, sum(f."posts")::bigint
  FROM "posts_total_5m" AS f
  JOIN (SELECT DISTINCT date_bin(INTERVAL '15 minutes', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed) AS touched
    ON f."bucket" >= touched.b AND f."bucket" < touched.b + INTERVAL '15 minutes'
  GROUP BY 1
  ON CONFLICT ("bucket") DO UPDATE SET "minutes" = excluded."minutes", "posts" = excluded."posts";
  RETURN NULL;
END $fn$;
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_total_15m_ins"
  AFTER INSERT ON "posts_total_5m"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_total_15m"();
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_total_15m_upd"
  AFTER UPDATE ON "posts_total_5m"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_total_15m"();

CREATE OR REPLACE FUNCTION "sqlflow_rollup_posts_total_1h"() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  -- The lock serializes writers only because each statement in READ COMMITTED
  -- takes a new snapshot, which sees the writer the lock waited on.
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'sqlflow rollup posts_total_1h needs READ COMMITTED, not %', current_setting('transaction_isolation');
  END IF;
  IF current_setting('sqlflow.rollup_backfill', true) IS DISTINCT FROM 'on' THEN
    PERFORM pg_advisory_xact_lock(hashtextextended('sqlflow_rollup:posts', 0));
    PERFORM pg_advisory_xact_lock(hashtextextended('posts_total_1h:' || extract(epoch FROM touched.b)::bigint, 0))
    FROM (SELECT DISTINCT date_bin(INTERVAL '1 hours', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed ORDER BY 1) AS touched;
  END IF;

  INSERT INTO "posts_total_1h" ("bucket", "minutes", "posts")
  SELECT date_bin(INTERVAL '1 hours', f."bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00'), sum(f."minutes")::bigint, sum(f."posts")::bigint
  FROM "posts_total_15m" AS f
  JOIN (SELECT DISTINCT date_bin(INTERVAL '1 hours', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed) AS touched
    ON f."bucket" >= touched.b AND f."bucket" < touched.b + INTERVAL '1 hours'
  GROUP BY 1
  ON CONFLICT ("bucket") DO UPDATE SET "minutes" = excluded."minutes", "posts" = excluded."posts";
  RETURN NULL;
END $fn$;
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_total_1h_ins"
  AFTER INSERT ON "posts_total_15m"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_total_1h"();
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_total_1h_upd"
  AFTER UPDATE ON "posts_total_15m"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_total_1h"();

CREATE OR REPLACE FUNCTION "sqlflow_rollup_posts_total_6h"() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  -- The lock serializes writers only because each statement in READ COMMITTED
  -- takes a new snapshot, which sees the writer the lock waited on.
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'sqlflow rollup posts_total_6h needs READ COMMITTED, not %', current_setting('transaction_isolation');
  END IF;
  IF current_setting('sqlflow.rollup_backfill', true) IS DISTINCT FROM 'on' THEN
    PERFORM pg_advisory_xact_lock(hashtextextended('sqlflow_rollup:posts', 0));
    PERFORM pg_advisory_xact_lock(hashtextextended('posts_total_6h:' || extract(epoch FROM touched.b)::bigint, 0))
    FROM (SELECT DISTINCT date_bin(INTERVAL '6 hours', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed ORDER BY 1) AS touched;
  END IF;

  INSERT INTO "posts_total_6h" ("bucket", "minutes", "posts")
  SELECT date_bin(INTERVAL '6 hours', f."bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00'), sum(f."minutes")::bigint, sum(f."posts")::bigint
  FROM "posts_total_1h" AS f
  JOIN (SELECT DISTINCT date_bin(INTERVAL '6 hours', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed) AS touched
    ON f."bucket" >= touched.b AND f."bucket" < touched.b + INTERVAL '6 hours'
  GROUP BY 1
  ON CONFLICT ("bucket") DO UPDATE SET "minutes" = excluded."minutes", "posts" = excluded."posts";
  RETURN NULL;
END $fn$;
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_total_6h_ins"
  AFTER INSERT ON "posts_total_1h"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_total_6h"();
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_total_6h_upd"
  AFTER UPDATE ON "posts_total_1h"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_total_6h"();

CREATE OR REPLACE FUNCTION "sqlflow_rollup_posts_total_1d"() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  -- The lock serializes writers only because each statement in READ COMMITTED
  -- takes a new snapshot, which sees the writer the lock waited on.
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'sqlflow rollup posts_total_1d needs READ COMMITTED, not %', current_setting('transaction_isolation');
  END IF;
  IF current_setting('sqlflow.rollup_backfill', true) IS DISTINCT FROM 'on' THEN
    PERFORM pg_advisory_xact_lock(hashtextextended('sqlflow_rollup:posts', 0));
    PERFORM pg_advisory_xact_lock(hashtextextended('posts_total_1d:' || extract(epoch FROM touched.b)::bigint, 0))
    FROM (SELECT DISTINCT date_bin(INTERVAL '24 hours', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed ORDER BY 1) AS touched;
  END IF;

  INSERT INTO "posts_total_1d" ("bucket", "minutes", "posts")
  SELECT date_bin(INTERVAL '24 hours', f."bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00'), sum(f."minutes")::bigint, sum(f."posts")::bigint
  FROM "posts_total_6h" AS f
  JOIN (SELECT DISTINCT date_bin(INTERVAL '24 hours', "bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS b FROM changed) AS touched
    ON f."bucket" >= touched.b AND f."bucket" < touched.b + INTERVAL '24 hours'
  GROUP BY 1
  ON CONFLICT ("bucket") DO UPDATE SET "minutes" = excluded."minutes", "posts" = excluded."posts";
  RETURN NULL;
END $fn$;
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_total_1d_ins"
  AFTER INSERT ON "posts_total_6h"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_total_1d"();
CREATE OR REPLACE TRIGGER "sqlflow_rollup_posts_total_1d_upd"
  AFTER UPDATE ON "posts_total_6h"
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION "sqlflow_rollup_posts_total_1d"();
