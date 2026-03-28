-- ============================================================
-- Oil Price Pipeline — TimescaleDB Schema
-- ============================================================

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ─────────────────────────────────────
-- RAW TICK TABLE (hypertable)
-- ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS oil_ticks (
    time            TIMESTAMPTZ     NOT NULL,
    benchmark       TEXT            NOT NULL,   -- WTI, BRENT, DUBAI
    exchange        TEXT            NOT NULL,   -- NYMEX, ICE, ADNOC
    price           DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION NOT NULL,
    bid             DOUBLE PRECISION,
    ask             DOUBLE PRECISION,
    source          TEXT
);

-- Convert to hypertable partitioned by time (1-day chunks)
SELECT create_hypertable('oil_ticks', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Partition also by benchmark for query efficiency
SELECT add_dimension('oil_ticks', 'benchmark', number_partitions => 3,
    if_not_exists => TRUE);

-- ─────────────────────────────────────
-- OHLCV AGGREGATES
-- ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS oil_ohlcv_1m (
    time            TIMESTAMPTZ     NOT NULL,
    benchmark       TEXT            NOT NULL,
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION NOT NULL,
    vwap            DOUBLE PRECISION,
    tick_count      INTEGER
);

SELECT create_hypertable('oil_ohlcv_1m', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS oil_ohlcv_5m (
    LIKE oil_ohlcv_1m INCLUDING ALL
);

SELECT create_hypertable('oil_ohlcv_5m', 'time',
    chunk_time_interval => INTERVAL '30 days',
    if_not_exists => TRUE
);

-- ─────────────────────────────────────
-- ALERTS TABLE
-- ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS oil_alerts (
    time            TIMESTAMPTZ     NOT NULL,
    benchmark       TEXT            NOT NULL,
    alert_type      TEXT            NOT NULL,   -- SPIKE, DROP, SPREAD_WIDE, VOLATILITY
    price           DOUBLE PRECISION NOT NULL,
    delta           DOUBLE PRECISION,
    delta_pct       DOUBLE PRECISION,
    severity        TEXT,                       -- LOW, MEDIUM, HIGH, CRITICAL
    message         TEXT
);

SELECT create_hypertable('oil_alerts', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- ─────────────────────────────────────
-- VOLATILITY METRICS
-- ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS oil_volatility (
    time            TIMESTAMPTZ     NOT NULL,
    benchmark       TEXT            NOT NULL,
    rolling_std_30  DOUBLE PRECISION,
    rolling_std_1m  DOUBLE PRECISION,
    atr_14          DOUBLE PRECISION,
    realized_vol    DOUBLE PRECISION
);

SELECT create_hypertable('oil_volatility', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- ─────────────────────────────────────
-- CONTINUOUS AGGREGATES (materialized views)
-- ─────────────────────────────────────

-- Hourly OHLCV from ticks
CREATE MATERIALIZED VIEW IF NOT EXISTS oil_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    benchmark,
    first(price, time)          AS open,
    max(price)                  AS high,
    min(price)                  AS low,
    last(price, time)           AS close,
    sum(volume)                 AS volume,
    sum(price * volume) / sum(volume) AS vwap,
    count(*)                    AS tick_count
FROM oil_ticks
GROUP BY bucket, benchmark
WITH NO DATA;

-- Refresh policy: keep hourly view up-to-date
SELECT add_continuous_aggregate_policy('oil_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);

-- Daily summary
CREATE MATERIALIZED VIEW IF NOT EXISTS oil_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS bucket,
    benchmark,
    first(price, time)         AS open,
    max(price)                 AS high,
    min(price)                 AS low,
    last(price, time)          AS close,
    sum(volume)                AS volume,
    sum(price * volume) / sum(volume) AS vwap,
    stddev(price)              AS daily_volatility,
    count(*)                   AS tick_count
FROM oil_ticks
GROUP BY bucket, benchmark
WITH NO DATA;

SELECT add_continuous_aggregate_policy('oil_daily',
    start_offset => INTERVAL '3 days',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- ─────────────────────────────────────
-- INDEXES
-- ─────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_ticks_benchmark_time
    ON oil_ticks (benchmark, time DESC);

CREATE INDEX IF NOT EXISTS idx_ohlcv_1m_benchmark_time
    ON oil_ohlcv_1m (benchmark, time DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_benchmark_time
    ON oil_alerts (benchmark, time DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_type
    ON oil_alerts (alert_type, time DESC);

-- ─────────────────────────────────────
-- RETENTION POLICY
-- ─────────────────────────────────────
-- Keep raw ticks for 30 days, aggregates forever
SELECT add_retention_policy('oil_ticks',
    INTERVAL '30 days',
    if_not_exists => TRUE
);

-- ─────────────────────────────────────
-- COMPRESSION
-- ─────────────────────────────────────
ALTER TABLE oil_ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'benchmark',
    timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('oil_ticks',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO oiluser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO oiluser;

\echo 'TimescaleDB schema initialized successfully.'
