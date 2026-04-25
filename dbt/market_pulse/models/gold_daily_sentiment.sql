-- Gold: Daily News Sentiment Proxy
-- 
-- We don't have a sentiment model yet (that's Phase 3 with OpenAI embeddings).
-- For now we count article volume per day as a proxy for market attention.
-- High article volume on a ticker = elevated market attention.
-- This table gets enriched with real sentiment scores in Phase 3.
--
-- Source: silver/news (read via Spark Thrift Server)

{{ config(
    materialized='table',
    file_format='delta',
    location='/delta_lake/gold/daily_sentiment'
) }}

WITH news_with_date AS (
    SELECT
        title,
        description,
        url,
        source_name,
        published_at_ts,
        -- Extract date from timestamp for daily aggregation
        DATE(published_at_ts) AS published_date,
        silver_processed_at
    FROM delta.`/delta_lake/silver/news`
    WHERE published_at_ts IS NOT NULL
),

-- Count articles per day as attention proxy
daily_counts AS (
    SELECT
        published_date,
        COUNT(*)                        AS article_count,
        COUNT(DISTINCT source_name)     AS source_diversity,
        MAX(silver_processed_at)        AS last_updated
    FROM news_with_date
    GROUP BY published_date
)

SELECT
    published_date,
    article_count,
    source_diversity,
    -- Simple attention score: normalized article count
    -- Will be replaced with real sentiment in Phase 3
    ROUND(article_count / 10.0, 2)     AS attention_score_proxy,
    last_updated
FROM daily_counts
ORDER BY published_date DESC