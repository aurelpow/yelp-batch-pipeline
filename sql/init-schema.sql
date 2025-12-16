-- =============================================================================
-- Yelp Batch Analytics - PostgreSQL Gold Layer Schema
-- =============================================================================
-- This script initializes the PostgreSQL database for the Gold layer analytics
-- Author: Aurélien
-- Date: 2025-11-25
-- Description: Creates schema and tables for business analytics KPIs
-- =============================================================================

-- =============================================================================
-- EXECUTION BEHAVIOR & SAFETY
-- =============================================================================
--
-- DOCKER BEHAVIOR:
-- This script runs ONLY ONCE when mounted to /docker-entrypoint-initdb.d/
-- - First container start with empty volume → Script executes
-- - Subsequent restarts → Script SKIPS (data persists)
-- - After `docker compose down --volumes` → Script executes again
--
-- IDEMPOTENCY:
-- This script uses "CREATE IF NOT EXISTS" and "ON CONFLICT DO NOTHING"
-- - Safe to re-run multiple times
-- - Won't drop existing tables or lose data
-- - Won't fail if objects already exist
--
-- MANUAL EXECUTION:
-- To manually run this script:
--   psql -h localhost -p 5433 -U airflow -d airflow -f init-db.sql
--
-- COMPLETE RESET (if needed):
--   DROP SCHEMA gold CASCADE;
--   -- Then re-run this script
--
-- SCHEMA MIGRATION (adding columns/indexes):
--   ALTER TABLE gold.business_popularity ADD COLUMN IF NOT EXISTS new_col TEXT;
--   CREATE INDEX IF NOT EXISTS idx_new ON gold.business_popularity(new_col);
--
-- =============================================================================

-- =============================================================================
-- DATABASE CREATION
-- =============================================================================

-- Check if yelp_analytics database exists, create if not
-- This uses the \gexec meta-command to conditionally execute SQL
SELECT 'CREATE DATABASE yelp_analytics OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'yelp_analytics')\gexec

-- Connect to yelp_analytics database
\c yelp_analytics

-- =============================================================================
-- SCHEMA CREATION
-- =============================================================================

-- Create gold schema for analytics tables
CREATE SCHEMA IF NOT EXISTS gold;

-- Set search path to gold schema
SET search_path TO gold, public;

-- =============================================================================
-- TABLE 1: BUSINESS POPULARITY METRICS
-- =============================================================================
-- Purpose: Monthly aggregated business popularity metrics with normalized scores
-- Granularity: One row per business per month
-- Key Metrics: Review ratings, checkin frequency, tip engagement, recency
-- Use Case: Business ranking, popularity analysis, city-level comparisons
-- =============================================================================

-- Note: Using IF NOT EXISTS makes this script idempotent and safe to re-run
-- For complete schema reset, manually run: DROP SCHEMA gold CASCADE;
CREATE TABLE IF NOT EXISTS gold.business_popularity (
    -- Primary Keys
    day                 DATE            NOT NULL,
    period_month        VARCHAR(7)      NOT NULL,       -- Format: YYYY-MM
    granularity         INTEGER         NOT NULL,       -- 0: Daily, 2: Monthly
    business_id         VARCHAR(50)     NOT NULL,

    -- Business Attributes
    name                VARCHAR(255),
    city                VARCHAR(100),
    state               VARCHAR(10),
    categories          TEXT,                           -- Comma-separated categories

    -- Popularity Metrics & Score
    popularity_score    DOUBLE PRECISION,               -- Weighted composite score (0-1)
    city_rank           INTEGER,                        -- Rank within city by popularity

    -- Audit Columns
    _gold_ingest_ts     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT pk_business_popularity PRIMARY KEY (business_id, day, granularity),
    CONSTRAINT chk_granularity CHECK (granularity IN (0, 2)),
    CONSTRAINT chk_popularity_score CHECK (popularity_score >= 0 AND popularity_score <= 1),
    CONSTRAINT chk_city_rank CHECK (city_rank > 0)
);

-- Indexes for query optimization
CREATE INDEX idx_bp_period_month ON gold.business_popularity(period_month);
CREATE INDEX idx_bp_city_rank ON gold.business_popularity(city, city_rank);
CREATE INDEX idx_bp_popularity_score ON gold.business_popularity(popularity_score DESC);
CREATE INDEX idx_bp_state_city ON gold.business_popularity(state, city);
CREATE INDEX idx_bp_day ON gold.business_popularity(day);

-- Table comments for documentation
COMMENT ON TABLE gold.business_popularity IS
'Monthly business popularity metrics with normalized scores and city rankings.
Computed at end-of-month for comprehensive monthly analysis.';

COMMENT ON COLUMN gold.business_popularity.popularity_score IS
'Weighted composite score: (norm_stars * 0.30) + (norm_review_count * 0.25) +
(recency_boost * 0.25) + ((norm_checkin + norm_tip) * 0.10). Range: 0-1';

COMMENT ON COLUMN gold.business_popularity.city_rank IS
'Business rank within city ordered by popularity_score DESC. Rank 1 = most popular.';

COMMENT ON COLUMN gold.business_popularity.granularity IS
'Time granularity: 0 = Daily, 2 = Monthly';

-- =============================================================================
 -- TABLE 2: FACT REVIEW & TIP METRICS (NARROW FORMAT)
-- =============================================================================
-- Purpose: Daily/Monthly aggregated review and tip metrics by business
-- Granularity: One row per business, date, granularity, and measure combination
-- Key Metrics: Review count, stars, tip count, compliments, user engagement
-- Use Case: Business performance tracking, trend analysis, KPI monitoring
-- Format: Narrow format - stacked metrics with measure identifier
-- =============================================================================

-- Note: Using IF NOT EXISTS makes this script idempotent and safe to re-run
CREATE TABLE IF NOT EXISTS gold.fact_review_tip_metrics (
    -- Primary Keys
    day                 DATE            NOT NULL,
    period_month        VARCHAR(7)      NOT NULL,       -- Format: YYYY-MM
    granularity         INTEGER         NOT NULL,       -- 0: Daily, 2: Monthly
    business_id         VARCHAR(50)     NOT NULL,
    measure             INTEGER         NOT NULL,       -- Metric type (see below)

    -- Metrics
    units               DOUBLE PRECISION,               -- Metric value

    -- Audit Columns
    _gold_ingest_ts     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT pk_fact_metrics PRIMARY KEY (business_id, day, granularity, measure),
    CONSTRAINT chk_fact_granularity CHECK (granularity IN (0, 2)),
    CONSTRAINT chk_measure_type CHECK (measure BETWEEN 1 AND 9)
);

-- Indexes for query optimization
CREATE INDEX idx_frm_day ON gold.fact_review_tip_metrics(day);
CREATE INDEX idx_frm_period_month ON gold.fact_review_tip_metrics(period_month);
CREATE INDEX idx_frm_business_measure ON gold.fact_review_tip_metrics(business_id, measure);
CREATE INDEX idx_frm_measure_units ON gold.fact_review_tip_metrics(measure, units DESC);
CREATE INDEX idx_frm_granularity ON gold.fact_review_tip_metrics(granularity);

-- Table comments for documentation
COMMENT ON TABLE gold.fact_review_tip_metrics IS
'Fact table containing aggregated review and tip metrics in narrow/long format.
Each row represents a specific metric (measure) for a business on a given date.
Supports both daily (granularity=0) and monthly (granularity=2) aggregations.
Narrow format allows flexible metric addition without schema changes.';

COMMENT ON COLUMN gold.fact_review_tip_metrics.measure IS
'Metric type codes:
1 = review_count (total reviews)
2 = tip_count (total tips received)
3 = compliment_count_sum (sum of all tip compliments)
4 = avg_stars (average review star rating, 1-5)
5 = total_useful (sum of useful votes on reviews)
6 = total_funny (sum of funny votes on reviews)
7 = total_cool (sum of cool votes on reviews)
8 = distinct_users_review (unique review authors)
9 = distinct_users_tip (unique tip authors)';

COMMENT ON COLUMN gold.fact_review_tip_metrics.units IS
'Metric value. Interpretation depends on measure type.
For counts: integer values (stored as double)
For averages: decimal values (0-5 for stars)';

COMMENT ON COLUMN gold.fact_review_tip_metrics.granularity IS
'Time granularity: 0 = Daily metrics, 2 = Monthly metrics (computed at EOM)';

-- =============================================================================
-- REFERENCE TABLE: MEASURE CODES
-- =============================================================================
-- Purpose: Lookup table for measure type codes used in fact table
-- =============================================================================

-- Note: Using IF NOT EXISTS makes this script idempotent and safe to re-run
CREATE TABLE IF NOT EXISTS gold.measure_codes (
    measure_id          INTEGER         PRIMARY KEY,
    measure_name        VARCHAR(50)     NOT NULL UNIQUE,
    measure_description TEXT            NOT NULL,
    measure_unit        VARCHAR(20),
    measure_category    VARCHAR(30)     NOT NULL,
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE
);

-- Insert measure definitions (idempotent - won't fail if data already exists)
INSERT INTO gold.measure_codes (measure_id, measure_name, measure_description, measure_unit, measure_category) VALUES
(1, 'review_count', 'Total number of reviews received', 'count', 'Review Metrics'),
(2, 'avg_review_stars', 'Average star rating across all reviews', 'stars (1-5)', 'Review Metrics'),
(3, 'tip_count', 'Total number of tips received', 'count', 'Tip Metrics'),
(4, 'avg_review_stars_sum', 'Sum of all star ratings (for aggregation)', 'stars', 'Review Metrics'),
(5, 'avg_tip_compliments', 'Average compliment count per tip', 'count', 'Tip Metrics'),
(6, 'tip_compliments_sum', 'Total sum of compliments received', 'count', 'Tip Metrics'),
(7, 'positive_sentiment_pct', 'Percentage of reviews with positive sentiment (stars >= 4)', 'percentage', 'Sentiment Metrics'),
(8, 'negative_sentiment_pct', 'Percentage of reviews with negative sentiment (stars <= 2)', 'percentage', 'Sentiment Metrics')
ON CONFLICT (measure_id) DO NOTHING;

COMMENT ON TABLE gold.measure_codes IS
'Reference table defining all metric types used in fact_review_tip_metrics table.';

-- =============================================================================
-- ANALYTICAL VIEWS
-- =============================================================================

-- View 1: Top 10 Popular Businesses by City (Latest Month)
CREATE OR REPLACE VIEW gold.v_top_businesses_by_city AS
SELECT
    city,
    state,
    business_id,
    name,
    categories,
    popularity_score,
    city_rank,
    period_month
FROM gold.business_popularity
WHERE city_rank <= 10
  AND period_month = (SELECT MAX(period_month) FROM gold.business_popularity)
ORDER BY city, city_rank;

COMMENT ON VIEW gold.v_top_businesses_by_city IS
'Top 10 most popular businesses per city for the latest available month.';

-- View 2: Review Metrics Pivot (Latest Day)
CREATE OR REPLACE VIEW gold.v_latest_review_metrics AS
SELECT
    business_id,
    day,
    period_month,
    MAX(CASE WHEN measure = 1 THEN units END) AS review_count,
    MAX(CASE WHEN measure = 4 THEN units END) AS avg_stars,
    MAX(CASE WHEN measure = 2 THEN units END) AS tip_count,
    MAX(CASE WHEN measure = 5 THEN units END) AS total_useful,
    MAX(CASE WHEN measure = 8 THEN units END) AS distinct_users_review,
    MAX(CASE WHEN measure = 9 THEN units END) AS distinct_users_tip
FROM gold.fact_review_tip_metrics
WHERE granularity = 0  -- Daily metrics
  AND day = (SELECT MAX(day) FROM gold.fact_review_tip_metrics WHERE granularity = 0)
GROUP BY business_id, day, period_month;

COMMENT ON VIEW gold.v_latest_review_metrics IS
'Pivoted view of latest daily review and tip metrics per business.';

-- View 3: Monthly Business Performance Summary
CREATE OR REPLACE VIEW gold.v_monthly_business_summary AS
SELECT
    f.business_id,
    f.period_month,
    f.day,
    MAX(CASE WHEN f.measure = 1 THEN f.units END) AS total_reviews,
    MAX(CASE WHEN f.measure = 2 THEN f.units END) AS avg_stars,
    MAX(CASE WHEN f.measure = 3 THEN f.units END) AS total_tips,
    MAX(CASE WHEN f.measure = 7 THEN f.units END) AS positive_sentiment_pct,
    bp.popularity_score,
    bp.city_rank,
    bp.name,
    bp.city,
    bp.state
FROM gold.fact_review_tip_metrics_wide f
LEFT JOIN gold.business_popularity bp
    ON f.business_id = bp.business_id
    AND f.day = bp.day
    AND f.granularity = bp.granularity
WHERE f.granularity = 2  -- Monthly metrics
GROUP BY f.business_id, f.period_month, f.day,
         bp.popularity_score, bp.city_rank, bp.name, bp.city, bp.state;

COMMENT ON VIEW gold.v_monthly_business_summary IS
'Comprehensive monthly business metrics combining fact metrics and popularity scores.';

-- =============================================================================
-- GRANTS & PERMISSIONS
-- =============================================================================

-- Grant read access to gold schema (adjust user as needed)
-- GRANT USAGE ON SCHEMA gold TO analytics_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA gold TO analytics_user;
-- GRANT SELECT ON ALL SEQUENCES IN SCHEMA gold TO analytics_user;

-- =============================================================================
-- MAINTENANCE & MONITORING
-- =============================================================================

-- Function to get table statistics
CREATE OR REPLACE FUNCTION gold.get_table_stats()
RETURNS TABLE (
    table_name VARCHAR,
    row_count BIGINT,
    total_size TEXT,
    last_modified TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        t.table_name::VARCHAR,
        (SELECT COUNT(*) FROM gold.business_popularity WHERE t.table_name = 'business_popularity')::BIGINT
            + (SELECT COUNT(*) FROM gold.fact_review_tip_metrics WHERE t.table_name = 'fact_review_tip_metrics')::BIGINT,
        pg_size_pretty(pg_total_relation_size('gold.' || t.table_name))::TEXT,
        CURRENT_TIMESTAMP
    FROM information_schema.tables t
    WHERE t.table_schema = 'gold'
      AND t.table_type = 'BASE TABLE';
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gold.get_table_stats() IS
'Returns statistics for all tables in gold schema including row counts and sizes.';

-- =============================================================================
-- DATA QUALITY CHECKS
-- =============================================================================

-- Check for duplicate keys in business_popularity
CREATE OR REPLACE VIEW gold.v_dq_duplicate_business_popularity AS
SELECT business_id, day, granularity, COUNT(*) as duplicate_count
FROM gold.business_popularity
GROUP BY business_id, day, granularity
HAVING COUNT(*) > 1;

-- Check for missing mandatory fields
CREATE OR REPLACE VIEW gold.v_dq_missing_fields AS
SELECT
    'business_popularity' as table_name,
    COUNT(*) as null_count,
    'business_id' as column_name
FROM gold.business_popularity
WHERE business_id IS NULL
UNION ALL
SELECT
    'fact_review_tip_metrics',
    COUNT(*),
    'units'
FROM gold.fact_review_tip_metrics
WHERE units IS NULL;

-- =============================================================================
-- SAMPLE QUERIES FOR ANALYTICS
-- =============================================================================

/*
-- Query 1: Top 5 cities by average business popularity
SELECT
    city,
    state,
    COUNT(DISTINCT business_id) as business_count,
    AVG(popularity_score) as avg_popularity,
    MAX(period_month) as latest_period
FROM gold.business_popularity
GROUP BY city, state
ORDER BY avg_popularity DESC
LIMIT 5;

-- Query 2: Monthly review trend for a specific business
SELECT
    period_month,
    MAX(CASE WHEN measure = 1 THEN units END) AS review_count,
    MAX(CASE WHEN measure = 2 THEN units END) AS avg_stars,
    MAX(CASE WHEN measure = 7 THEN units END) AS positive_pct
FROM gold.fact_review_tip_metrics_wide
WHERE business_id = 'YOUR_BUSINESS_ID'
  AND granularity = 2
GROUP BY period_month
ORDER BY period_month;

-- Query 3: Business performance comparison within city
SELECT
    b.name,
    b.city_rank,
    b.popularity_score,
    f.units as review_count
FROM gold.business_popularity b
JOIN gold.fact_review_tip_metrics_wide f
    ON b.business_id = f.business_id
    AND b.day = f.day
    AND b.granularity = f.granularity
WHERE b.city = 'Philadelphia'
  AND f.measure = 1
  AND b.period_month = '2020-01'
ORDER BY b.city_rank
LIMIT 20;
*/

-- =============================================================================
-- SCRIPT COMPLETION
-- =============================================================================

SELECT 'Gold schema initialization completed successfully!' AS status;
SELECT 'Created 2 fact tables, 1 reference table, 3 analytical views' AS summary;