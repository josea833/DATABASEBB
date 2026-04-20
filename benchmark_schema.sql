-- Benchmark Schema Reference
-- This file is the single source of truth for DDL.
-- benchmark_db.py reads and executes these statements at bootstrap time.
--
-- Placeholder tokens (substituted at runtime by benchmark_db.py):
--   {db}    = database name  (default: benchmark_db)
--   {table} = table name     (default: products_wide)

-- ============================================================
-- MySQL (InnoDB)
-- ============================================================

CREATE DATABASE IF NOT EXISTS `{db}`;
USE `{db}`;

-- Clustered primary key on id; InnoDB physical row order follows id.
CREATE TABLE IF NOT EXISTS `{table}` (
    id           BIGINT          NOT NULL,
    sku          VARCHAR(32)     NOT NULL,
    product_name VARCHAR(255)    NOT NULL,
    category     VARCHAR(64)     NOT NULL,
    brand        VARCHAR(64)     NOT NULL,
    color_type   VARCHAR(64)     NOT NULL,
    size_label   VARCHAR(32)     NOT NULL,
    material     VARCHAR(64)     NOT NULL,
    price        DECIMAL(10, 2)  NOT NULL,
    weight_grams INT             NOT NULL,
    stock_quantity INT           NOT NULL,
    description  TEXT            NOT NULL,
    created_at   DATETIME        NOT NULL,
    PRIMARY KEY (id)
);

-- Composite index used by the range-filter + aggregation workload queries.
CREATE INDEX idx_created_at_stock
    ON `{table}` (created_at, stock_quantity);


-- ============================================================
-- ClickHouse (MergeTree)
-- ============================================================

CREATE DATABASE IF NOT EXISTS {db};

-- MergeTree sorted by (created_at, id) for efficient time-range scans.
-- Skip indexes omitted for compatibility with older local ClickHouse versions.
CREATE TABLE IF NOT EXISTS {db}.{table} (
    id             UInt64,
    sku            String,
    product_name   String,
    category       String,
    brand          String,
    color_type     String,
    size_label     String,
    material       String,
    price          Decimal(10, 2),
    weight_grams   UInt32,
    stock_quantity UInt32,
    description    String,
    created_at     DateTime
)
ENGINE = MergeTree
ORDER BY (created_at, id);


-- Notes
-- 1) {table} and {db} are filled in by _load_schema_sql() in benchmark_db.py.
-- 2) CREATE DATABASE and USE statements are skipped by the loader;
--    they are here only for reference and manual use.
-- 3) The default table name is products_wide; pass --table to override.
