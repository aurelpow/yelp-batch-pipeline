# PostgreSQL Gold Layer - Complete Guide

> **📂 Project Files**: This folder contains all PostgreSQL-related files:
> - `init-schema.sql` - Database initialization script (auto-runs on first Docker start)
> - `README.md` - This comprehensive guide (setup, queries, troubleshooting)

## 📋 Quick Navigation
- [Overview](#overview)
- [Database Schema](#database-schema)
- [Setup Instructions](#setup-instructions)
- [Connection Details](#connection-details)
- [Common Queries](#common-queries)
- [Schema Management](#schema-management)
- [Troubleshooting](#troubleshooting)

---

## Overview

This project uses PostgreSQL to persist Gold layer analytics data alongside Delta Lake. The schema automatically initializes when the PostgreSQL container starts for the first time.

### Architecture
```
Raw JSON/MongoDB → Bronze (Delta) → Silver (Delta) → Gold (Delta + PostgreSQL)
                                                              ↓
                                                     SQL Analytics & BI
```

---

## Database Schema

### Schema: `gold`

Dedicated schema for analytics tables, separate from Airflow's operational tables.

### Tables

#### 1. `business_popularity`
**Monthly business popularity metrics with city rankings**

| Column | Type | Description |
|--------|------|-------------|
| business_id | VARCHAR(50) | Unique business identifier (PK) |
| day | DATE | Snapshot date (PK) |
| period_month | VARCHAR(7) | YYYY-MM format |
| granularity | INTEGER | 0=Daily, 2=Monthly (PK) |
| name | VARCHAR(255) | Business name |
| city | VARCHAR(100) | City location |
| state | VARCHAR(10) | State code |
| categories | TEXT | Business categories |
| popularity_score | DOUBLE PRECISION | Composite score (0-1) |
| city_rank | INTEGER | Rank within city |
| _gold_ingest_ts | TIMESTAMP | Audit timestamp |

**Popularity Score Formula**:
```
(norm_stars × 0.30) + (norm_review_count × 0.25) + 
(recency_boost × 0.25) + ((norm_checkin + norm_tip) × 0.10)
```

**Example Query**:
```sql
-- Top 10 businesses in a city
SELECT name, city_rank, popularity_score, categories
FROM gold.business_popularity
WHERE city = 'Philadelphia' AND period_month = '2020-01'
ORDER BY city_rank
LIMIT 10;
```

#### 2. `fact_review_tip_metrics_wide`
**Daily/Monthly review and tip metrics in measure-based format**

| Column | Type | Description |
|--------|------|-------------|
| business_id | VARCHAR(50) | Business identifier (PK) |
| day | DATE | Metric date (PK) |
| period_month | VARCHAR(7) | YYYY-MM format |
| granularity | INTEGER | 0=Daily, 2=Monthly (PK) |
| measure | INTEGER | Metric type 1-8 (PK) |
| units | DOUBLE PRECISION | Metric value |
| _gold_ingest_ts | TIMESTAMP | Audit timestamp |

**Measure Codes**:
| ID | Measure | Description |
|----|---------|-------------|
| 1 | review_count | Total reviews |
| 2 | avg_review_stars | Average rating (1-5) |
| 3 | tip_count | Total tips |
| 4 | avg_review_stars_sum | Sum of ratings |
| 5 | avg_tip_compliments | Avg compliments/tip |
| 6 | tip_compliments_sum | Total compliments |
| 7 | positive_sentiment_pct | % reviews ≥4 stars |
| 8 | negative_sentiment_pct | % reviews ≤2 stars |

**Example Query**:
```sql
-- Monthly trends for a business (pivot format)
SELECT 
    period_month,
    MAX(CASE WHEN measure = 1 THEN units END) AS review_count,
    MAX(CASE WHEN measure = 2 THEN units END) AS avg_stars,
    MAX(CASE WHEN measure = 7 THEN units END) AS positive_pct
FROM gold.fact_review_tip_metrics_wide
WHERE business_id = 'YOUR_ID' AND granularity = 2
GROUP BY period_month
ORDER BY period_month;
```

#### 3. `measure_codes`
**Reference table for metric definitions** (8 rows)

### Analytical Views

- **`v_top_businesses_by_city`** - Top 10 per city (latest month)
- **`v_latest_review_metrics`** - Current daily metrics (pivoted)
- **`v_monthly_business_summary`** - Comprehensive KPIs

---

## Setup Instructions

### Option 1: Automatic (Docker - Recommended)

The schema initializes automatically on first container start:

```powershell
# Fresh setup (creates schema)
docker compose down --volumes
docker compose up -d

# Wait 30 seconds, then verify
docker exec -it yelp-batch-project-postgres-1 psql -U airflow -d airflow -c "\dt gold.*"
```

### Option 2: Manual Execution

If you need to run the schema script manually:

```powershell
# Copy and execute
docker cp sql/init-schema.sql yelp-batch-project-postgres-1:/tmp/
docker exec -it yelp-batch-project-postgres-1 psql -U airflow -d airflow -f /tmp/init-schema.sql
```

### Enable PostgreSQL in Pipeline

Update your config file (`src/main/resources/dev.conf` or `local.conf`):

```hocon
postgresql {
  enabled = true
  host = "host.docker.internal"  # or "postgres" from Airflow
  port = 5432
  database = "airflow"
}
```

Run with credentials:
```powershell
bin\run-local.cmd --process gold_business_popularity --env dev --run_date 2020-01-31 --pg_user airflow --pg_password airflow
```

---

## Connection Details

### From Docker Containers (Airflow/Spark)
```
Host: postgres
Port: 5432
Database: airflow
User: airflow
Password: airflow
Schema: gold
```

### From Host Machine (pgAdmin, psql, BI tools)
```
Host: localhost
Port: 5433
Database: airflow
User: airflow
Password: airflow
Schema: gold
```

### JDBC URL (Spark)
```scala
// From Airflow containers
jdbc:postgresql://postgres:5432/airflow?currentSchema=gold

// From host machine
jdbc:postgresql://host.docker.internal:5432/airflow?currentSchema=gold
jdbc:postgresql://localhost:5433/airflow?currentSchema=gold
```

### Connection Examples

**psql (Command Line)**:
```powershell
# From host
psql -h localhost -p 5433 -U airflow -d airflow

# From Docker
docker exec -it yelp-batch-project-postgres-1 psql -U airflow -d airflow
```

**pgAdmin 4**:
- Create new server
- General → Name: Yelp Analytics
- Connection → Host: localhost, Port: 5433, Database: airflow, Username: airflow

---

## Common Queries

### Quick Reference

**List tables**:
```sql
\dt gold.*
```

**Table structure**:
```sql
\d+ gold.business_popularity
```

**Row counts**:
```sql
SELECT 
    'business_popularity' as table_name, COUNT(*) as rows 
FROM gold.business_popularity
UNION ALL
SELECT 'fact_review_tip_metrics_wide', COUNT(*) 
FROM gold.fact_review_tip_metrics_wide;
```

**Latest data**:
```sql
SELECT 
    MAX(_gold_ingest_ts) as last_update,
    COUNT(DISTINCT period_month) as months_available,
    MIN(day) as first_date,
    MAX(day) as last_date
FROM gold.business_popularity;
```

### Analytical Queries

**Top performers across cities**:
```sql
SELECT city, state, name, city_rank, popularity_score
FROM gold.business_popularity
WHERE period_month = '2020-01' AND city_rank <= 5
ORDER BY city, city_rank;
```

**Business performance dashboard**:
```sql
WITH latest AS (
    SELECT MAX(period_month) as month FROM gold.business_popularity
)
SELECT 
    bp.city,
    COUNT(DISTINCT bp.business_id) as businesses,
    AVG(bp.popularity_score) as avg_popularity,
    AVG(CASE WHEN f.measure = 2 THEN f.units END) as avg_rating
FROM gold.business_popularity bp
JOIN gold.fact_review_tip_metrics_wide f 
    ON bp.business_id = f.business_id AND bp.day = f.day
WHERE bp.period_month = (SELECT month FROM latest)
  AND f.granularity = 2
GROUP BY bp.city
ORDER BY avg_popularity DESC
LIMIT 20;
```

**Time series analysis**:
```sql
SELECT 
    period_month,
    COUNT(DISTINCT business_id) as active_businesses,
    AVG(popularity_score) as avg_popularity
FROM gold.business_popularity
WHERE city = 'Philadelphia'
GROUP BY period_month
ORDER BY period_month;
```

**Review growth trends**:
```sql
SELECT 
    period_month,
    MAX(CASE WHEN measure = 1 THEN units END) as reviews,
    LAG(MAX(CASE WHEN measure = 1 THEN units END)) 
        OVER (ORDER BY period_month) as prev_month
FROM gold.fact_review_tip_metrics_wide
WHERE business_id = 'YOUR_ID' AND granularity = 2 AND measure = 1
GROUP BY period_month
ORDER BY period_month;
```

---

## Schema Management

### Understanding init-schema.sql Execution

**Docker Behavior (Automatic)**:
- Scripts in `/docker-entrypoint-initdb.d/` run **ONCE** on first start
- Subsequent restarts: Script **SKIPS** (data preserved in volume)
- After `docker compose down --volumes`: Script **RUNS AGAIN** (fresh start)

**Idempotent Design**:
- Uses `CREATE TABLE IF NOT EXISTS` (safe to re-run)
- Uses `ON CONFLICT DO NOTHING` (won't duplicate data)
- **Never drops existing tables** (data preserved)

### Common Scenarios

#### Fresh Setup
```powershell
docker compose up -d
# init-schema.sql runs automatically
```

#### Restart (Preserve Data)
```powershell
docker compose down         # Keep volumes
docker compose up -d        # Data intact
```

#### Complete Reset
```powershell
docker compose down --volumes  # Delete data
docker compose up -d           # Fresh database
```

#### Add New Column (Production)
```sql
-- Safe migration (no data loss)
ALTER TABLE gold.business_popularity 
ADD COLUMN IF NOT EXISTS region VARCHAR(50) DEFAULT 'Unknown';

-- Create index
CREATE INDEX IF NOT EXISTS idx_bp_region 
ON gold.business_popularity(region);

-- Update statistics
ANALYZE gold.business_popularity;
```

#### Manual Schema Reset
```sql
-- Only if you need to completely recreate
DROP SCHEMA gold CASCADE;
-- Then re-run init-schema.sql
```

### Production Migration Best Practices

**Before Migration**:
```powershell
# 1. Backup
docker exec yelp-batch-project-postgres-1 pg_dump -U airflow airflow > backup.sql

# 2. Test in transaction
docker exec -it yelp-batch-project-postgres-1 psql -U airflow -d airflow
```

```sql
BEGIN;
    -- Your changes
    ALTER TABLE gold.business_popularity ADD COLUMN test TEXT;
    SELECT * FROM gold.business_popularity LIMIT 1;
ROLLBACK;  -- Test only
```

**After Migration**:
```sql
-- Update statistics
ANALYZE gold.business_popularity;

-- Verify
SELECT COUNT(*) FROM gold.business_popularity;
```

---

## Troubleshooting

### Database Connection Issues

**Error**: `database "airflow" does not exist`

**Solution**: Reset volume and reinitialize
```powershell
docker compose down --volumes
docker compose up -d
```

---

**Error**: `schema "gold" does not exist`

**Solution**: Check if init script ran
```powershell
docker logs yelp-batch-project-postgres-1 | Select-String "gold"
# If not found, manually run init-schema.sql
```

---

**Error**: `connection refused`

**Solution**: Verify container is running
```powershell
docker ps | Select-String postgres
# If not running: docker compose up -d postgres
```

---

### Data Issues

**Problem**: No data in tables

**Check**: Run gold processes with PostgreSQL enabled
```powershell
bin\run-local.cmd --process gold_business_popularity --env dev --run_date 2020-01-31 --pg_user airflow --pg_password airflow
```

---

**Problem**: Re-running the same date causes duplicate key errors

**Solution**: The pipeline automatically handles this with an **upsert strategy** (delete-before-insert)

When you re-run the same date:
1. **Deletes** existing records for that day/period
2. **Inserts** fresh data

**Example**:
```powershell
# First run - inserts data ✅
bin\run-local.cmd --process gold_business_popularity --env dev --run_date 2020-01-31 --pg_user airflow --pg_password airflow

# Second run (same date) - deletes old + inserts new ✅ (no error!)
bin\run-local.cmd --process gold_business_popularity --env dev --run_date 2020-01-31 --pg_user airflow --pg_password airflow
```

**Delete keys used**:
- `business_popularity`: Deletes by `day`, `period_month`
- `fact_review_tip_metrics_wide`: Deletes by `day`, `period_month`, `granularity`

**Verify no duplicates**:
```sql
-- Should return 0 rows
SELECT business_id, day, granularity, COUNT(*) 
FROM gold.business_popularity 
GROUP BY business_id, day, granularity 
HAVING COUNT(*) > 1;
```

---

**Problem**: Lost data after restart

**Cause**: Used `--volumes` flag
```powershell
# Wrong (deletes data)
docker compose down --volumes

# Correct (preserves data)
docker compose down
```

---

### Performance Issues

**Slow queries**: Update statistics
```sql
ANALYZE gold.business_popularity;
ANALYZE gold.fact_review_tip_metrics_wide;
```

**Check indexes**:
```sql
SELECT 
    tablename, indexname, idx_scan as times_used
FROM pg_stat_user_indexes 
WHERE schemaname = 'gold'
ORDER BY idx_scan DESC;
```

**Query optimization**:
```sql
EXPLAIN ANALYZE 
SELECT * FROM gold.business_popularity 
WHERE city = 'Philadelphia' AND period_month = '2020-01';
```

---

## Maintenance

### Regular Tasks

**Vacuum and analyze** (weekly):
```sql
VACUUM ANALYZE gold.business_popularity;
VACUUM ANALYZE gold.fact_review_tip_metrics_wide;
```

**Check table sizes**:
```sql
SELECT 
    tablename,
    pg_size_pretty(pg_total_relation_size('gold.' || tablename)) as total_size
FROM pg_tables 
WHERE schemaname = 'gold'
ORDER BY pg_total_relation_size('gold.' || tablename) DESC;
```

**Monitor queries**:
```sql
SELECT pid, query, state, wait_event 
FROM pg_stat_activity 
WHERE datname = 'airflow' AND query NOT LIKE '%pg_stat%';
```

### Backup & Restore

**Backup**:
```powershell
# Full database
docker exec yelp-batch-project-postgres-1 pg_dump -U airflow airflow > backup.sql

# Gold schema only
docker exec yelp-batch-project-postgres-1 pg_dump -U airflow -n gold airflow > gold_backup.sql
```

**Restore**:
```powershell
Get-Content backup.sql | docker exec -i yelp-batch-project-postgres-1 psql -U airflow -d airflow
```

---

## Quick Command Reference

### Connection
```powershell
# Connect to database
docker exec -it yelp-batch-project-postgres-1 psql -U airflow -d airflow

# List schemas
\dn

# List tables
\dt gold.*

# Describe table
\d+ gold.business_popularity

# Exit
\q
```

### Data Checks
```sql
-- Row counts
SELECT COUNT(*) FROM gold.business_popularity;

-- Latest ingestion
SELECT MAX(_gold_ingest_ts) FROM gold.business_popularity;

-- Date range
SELECT MIN(day), MAX(day) FROM gold.business_popularity;

-- Top businesses
SELECT * FROM gold.v_top_businesses_by_city LIMIT 10;
```

### Container Management
```powershell
# Start services
docker compose up -d

# Stop (preserve data)
docker compose down

# Stop (delete data)
docker compose down --volumes

# Check status
docker ps | Select-String postgres

# View logs
docker logs yelp-batch-project-postgres-1 --tail 50
```

---

## Advanced Topics

### Indexes Explained

The schema creates strategic indexes for common query patterns:

- `idx_bp_city_rank` - City rankings queries
- `idx_bp_popularity_score` - Top performers queries
- `idx_frm_day` - Time-series analysis
- `idx_frm_business_measure` - Specific business metrics

### Data Quality Views

**Check duplicates**:
```sql
SELECT * FROM gold.v_dq_duplicate_business_popularity;
```

**Check missing fields**:
```sql
SELECT * FROM gold.v_dq_missing_fields;
```

### Custom Analytics

**Create your own view**:
```sql
CREATE OR REPLACE VIEW gold.v_my_analysis AS
SELECT 
    city,
    AVG(popularity_score) as avg_score,
    COUNT(*) as business_count
FROM gold.business_popularity
WHERE period_month = '2020-01'
GROUP BY city;
```

---

## Files Structure

```
sql/
├── init-schema.sql           # Database initialization script
└── README.md                 # This guide
```

**Note**: The `init-schema.sql` file is automatically mounted to PostgreSQL's `/docker-entrypoint-initdb.d/` directory via docker-compose.yaml.

---

**Author**: Aurélien  
**Last Updated**: November 25, 2025  
**Version**: 2.0

