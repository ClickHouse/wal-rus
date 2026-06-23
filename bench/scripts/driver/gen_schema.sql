-- gen_schema.sql
-- Workload schema for the wal-g vs walrus WAL-archiving benchmark.
--
-- Design goal: maximize WAL volume, dominated by full-page images (FPI).
--   * PostgreSQL writes a full-page image the first time a page is modified
--     after a checkpoint (full_page_writes is on by default). Every B-tree
--     index whose page is touched by an UPDATE also emits its own FPI.
--   * A WIDE row (so few rows per heap page, many distinct pages get dirtied)
--     plus MANY indexes (so a single UPDATE fans out into several index-page
--     FPIs) turns random UPDATE storms into a WAL firehose.
--   * fillfactor is lowered so HOT-update pruning still rewrites pages and the
--     heap stays spread across many pages rather than packing tightly.
--
-- Run against the 'walbench' database (created by pgbench_init.sh).

-- ---------------------------------------------------------------------------
-- wal_churn: wide, heavily-indexed table targeted by random UPDATE storms.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS wal_churn;

CREATE TABLE wal_churn (
    id          bigint      PRIMARY KEY,
    k1          bigint      NOT NULL,   -- indexed, frequently updated
    k2          bigint      NOT NULL,   -- indexed, frequently updated
    k3          integer     NOT NULL,   -- indexed, frequently updated
    tag         text        NOT NULL,   -- indexed, frequently updated
    updated_at  timestamptz NOT NULL,   -- indexed, frequently updated
    counter     bigint      NOT NULL DEFAULT 0,
    payload     text        NOT NULL    -- wide filler -> few rows per page
) WITH (fillfactor = 70);

-- Four secondary B-tree indexes (+ the primary key = five B-trees total).
-- Each UPDATE that changes an indexed column dirties the matching index pages,
-- multiplying FPI WAL output per modified row.
CREATE INDEX wal_churn_k1_idx  ON wal_churn (k1);
CREATE INDEX wal_churn_k2_idx  ON wal_churn (k2);
CREATE INDEX wal_churn_k3_idx  ON wal_churn (k3);
CREATE INDEX wal_churn_tag_idx ON wal_churn (tag);
CREATE INDEX wal_churn_updated_at_idx ON wal_churn (updated_at);

-- Seed rows. ROWS is substituted by the loader; default 2,000,000 (~ a few GB
-- of heap once the payload filler is included). Spread across many pages thanks
-- to the wide payload and fillfactor 70.
INSERT INTO wal_churn (id, k1, k2, k3, tag, updated_at, counter, payload)
SELECT
    g,
    (random() * 1e9)::bigint,
    (random() * 1e9)::bigint,
    (random() * 1e6)::integer,
    md5(g::text),
    now(),
    0,
    repeat(md5(random()::text), 8)   -- ~256 bytes of filler per row
FROM generate_series(1, :rows) AS g;

-- ---------------------------------------------------------------------------
-- wal_bulk: append/truncate target for large COPY bursts. Logged so every
-- COPY is fully WAL-logged. No indexes: COPY here is pure heap-insert WAL.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS wal_bulk;

CREATE TABLE wal_bulk (
    id          bigint      NOT NULL,
    batch       integer     NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    blob        text        NOT NULL
);

ANALYZE wal_churn;
ANALYZE wal_bulk;
