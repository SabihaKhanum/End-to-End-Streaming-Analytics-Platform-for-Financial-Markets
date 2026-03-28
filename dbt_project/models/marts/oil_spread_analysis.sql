-- models/marts/oil_spread_analysis.sql
-- WTI vs Brent vs Dubai spread analytics

{{ config(materialized='table') }}

WITH hourly_prices AS (
    SELECT
        hour_bucket,
        benchmark,
        AVG(price)  AS avg_price,
        MIN(price)  AS min_price,
        MAX(price)  AS max_price
    FROM {{ ref('stg_oil_ticks') }}
    GROUP BY hour_bucket, benchmark
),
pivoted AS (
    SELECT
        hour_bucket,
        MAX(CASE WHEN benchmark = 'WTI'   THEN avg_price END) AS wti,
        MAX(CASE WHEN benchmark = 'BRENT' THEN avg_price END) AS brent,
        MAX(CASE WHEN benchmark = 'DUBAI' THEN avg_price END) AS dubai
    FROM hourly_prices
    GROUP BY hour_bucket
    HAVING COUNT(DISTINCT benchmark) = 3
)
SELECT
    hour_bucket,
    ROUND(wti::NUMERIC, 3)             AS wti_price,
    ROUND(brent::NUMERIC, 3)           AS brent_price,
    ROUND(dubai::NUMERIC, 3)           AS dubai_price,
    ROUND((wti - brent)::NUMERIC, 3)   AS wti_brent_spread,
    ROUND((brent - dubai)::NUMERIC, 3) AS brent_dubai_spread,
    ROUND((wti - dubai)::NUMERIC, 3)   AS wti_dubai_spread,
    CASE
        WHEN ABS(wti - brent) > 5  THEN 'WIDE'
        WHEN ABS(wti - brent) > 3  THEN 'MODERATE'
        ELSE 'NORMAL'
    END                                AS spread_regime,
    -- 7-day rolling average spread
    AVG(wti - brent) OVER (
        ORDER BY hour_bucket
        ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
    )                                  AS wti_brent_7d_avg_spread
FROM pivoted
ORDER BY hour_bucket DESC
