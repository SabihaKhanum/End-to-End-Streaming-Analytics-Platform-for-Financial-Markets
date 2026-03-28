-- models/staging/stg_oil_ticks.sql
-- Staging: clean and type-cast raw ticks

{{ config(materialized='view') }}

SELECT
    time                                        AS tick_time,
    benchmark,
    exchange,
    price::DOUBLE PRECISION                     AS price,
    volume::DOUBLE PRECISION                    AS volume,
    bid::DOUBLE PRECISION                       AS bid,
    ask::DOUBLE PRECISION                       AS ask,
    (ask - bid)                                 AS spread,
    CASE
        WHEN benchmark = 'WTI'   THEN 'NORTH_AMERICA'
        WHEN benchmark = 'BRENT' THEN 'EUROPE'
        WHEN benchmark = 'DUBAI' THEN 'MIDDLE_EAST'
        ELSE 'OTHER'
    END                                         AS region,
    source,
    DATE_TRUNC('hour', time)                    AS hour_bucket,
    DATE_TRUNC('day',  time)                    AS day_bucket
FROM {{ source('public', 'oil_ticks') }}
WHERE time >= NOW() - INTERVAL '30 days'
