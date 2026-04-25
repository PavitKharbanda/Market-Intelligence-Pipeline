-- Gold: Price Anomaly Flags
--
-- Detects volume and price range anomalies per ticker using
-- a rolling z-score approach:
--   z = (value - rolling_mean) / rolling_stddev
-- A z-score > 2.0 means the value is more than 2 standard deviations
-- above the recent average — flagged as anomalous.
--
-- This is the table the LangGraph agent queries in Phase 3 when answering
-- "are there any volume anomalies for NVDA this week?"

{{ config(
    materialized='table',
    file_format='delta',
    location='/delta_lake/gold/price_anomaly_flags'
) }}

WITH base AS (
    SELECT
        ticker,
        bar_time_ts,
        DATE(bar_time_ts)   AS bar_date,
        open,
        high,
        low,
        close,
        volume,
        price_range
    FROM delta.`/delta_lake/silver/prices`
    WHERE bar_time_ts IS NOT NULL
),

-- Rolling stats per ticker over last 20 bars
-- ROWS BETWEEN 19 PRECEDING AND CURRENT ROW = 20-bar rolling window
rolling AS (
    SELECT
        *,
        AVG(volume) OVER (
            PARTITION BY ticker
            ORDER BY bar_time_ts
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_volume_20,

        STDDEV(volume) OVER (
            PARTITION BY ticker
            ORDER BY bar_time_ts
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS stddev_volume_20,

        AVG(price_range) OVER (
            PARTITION BY ticker
            ORDER BY bar_time_ts
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_range_20,

        STDDEV(price_range) OVER (
            PARTITION BY ticker
            ORDER BY bar_time_ts
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS stddev_range_20
    FROM base
),

-- Compute z-scores and flag anomalies
flagged AS (
    SELECT
        *,
        -- z-score: how many std devs is this bar's volume from its rolling mean?
        CASE
            WHEN stddev_volume_20 > 0
            THEN (volume - avg_volume_20) / stddev_volume_20
            ELSE 0
        END AS volume_zscore,

        CASE
            WHEN stddev_range_20 > 0
            THEN (price_range - avg_range_20) / stddev_range_20
            ELSE 0
        END AS range_zscore
    FROM rolling
)

SELECT
    ticker,
    bar_time_ts,
    bar_date,
    open, high, low, close,
    volume,
    price_range,
    ROUND(volume_zscore, 3)         AS volume_zscore,
    ROUND(range_zscore, 3)          AS range_zscore,
    -- Flag if either z-score exceeds 2.0 (2 standard deviations)
    CASE WHEN ABS(volume_zscore) > 2.0 OR ABS(range_zscore) > 2.0
         THEN TRUE ELSE FALSE
    END                             AS is_anomaly,
    CURRENT_TIMESTAMP()             AS gold_processed_at
FROM flagged
ORDER BY ticker, bar_time_ts