-- models/marts/oil_daily_analytics.sql
-- Daily analytics mart with price, volatility, and spread metrics

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['day_bucket', 'benchmark'], 'unique': True}
    ]
) }}

WITH daily_stats AS (
    SELECT
        day_bucket,
        benchmark,
        region,
        FIRST_VALUE(price) OVER w               AS open_price,
        MAX(price) OVER w                        AS high_price,
        MIN(price) OVER w                        AS low_price,
        LAST_VALUE(price) OVER w                 AS close_price,
        SUM(volume) OVER w                       AS total_volume,
        SUM(price * volume) OVER w /
            NULLIF(SUM(volume) OVER w, 0)        AS vwap,
        STDDEV(price) OVER w                     AS price_volatility,
        COUNT(*) OVER w                          AS tick_count,
        AVG(spread) OVER w                       AS avg_spread
    FROM {{ ref('stg_oil_ticks') }}
    WINDOW w AS (PARTITION BY day_bucket, benchmark
                 ORDER BY tick_time
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
),
deduplicated AS (
    SELECT DISTINCT ON (day_bucket, benchmark) *
    FROM daily_stats
    ORDER BY day_bucket DESC, benchmark
),
with_changes AS (
    SELECT
        d.*,
        LAG(close_price) OVER (PARTITION BY benchmark ORDER BY day_bucket) AS prev_close,
        close_price - LAG(close_price) OVER (PARTITION BY benchmark ORDER BY day_bucket)
            AS price_change,
        (close_price - LAG(close_price) OVER (PARTITION BY benchmark ORDER BY day_bucket))
            / NULLIF(LAG(close_price) OVER (PARTITION BY benchmark ORDER BY day_bucket), 0) * 100
            AS pct_change
    FROM deduplicated d
)
SELECT
    day_bucket,
    benchmark,
    region,
    ROUND(open_price::NUMERIC, 3)      AS open,
    ROUND(high_price::NUMERIC, 3)      AS high,
    ROUND(low_price::NUMERIC, 3)       AS low,
    ROUND(close_price::NUMERIC, 3)     AS close,
    ROUND(vwap::NUMERIC, 3)            AS vwap,
    ROUND(total_volume::NUMERIC, 0)    AS total_volume,
    ROUND(price_volatility::NUMERIC, 4) AS daily_volatility,
    ROUND(avg_spread::NUMERIC, 4)      AS avg_spread,
    ROUND(price_change::NUMERIC, 3)    AS price_change,
    ROUND(pct_change::NUMERIC, 4)      AS pct_change,
    tick_count,
    CASE
        WHEN ABS(pct_change) > 3    THEN 'HIGH'
        WHEN ABS(pct_change) > 1.5  THEN 'MEDIUM'
        ELSE 'NORMAL'
    END                                AS volatility_regime
FROM with_changes
ORDER BY day_bucket DESC, benchmark
