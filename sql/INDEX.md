# SQL Files Quick Reference

## Files in this folder

### 1. `init-schema.sql` (17 KB)
**Purpose**: PostgreSQL database initialization script

**Contents**:
- Schema creation (`gold`)
- 2 fact tables: `business_popularity`, `fact_review_tip_metrics_wide`
- 1 reference table: `measure_codes`
- 3 analytical views
- 2 data quality views
- 10 strategic indexes
- 1 monitoring function

**Execution**: Runs automatically when PostgreSQL container starts for the first time (via Docker's `/docker-entrypoint-initdb.d/`)

**Manual run**:
```bash
docker exec -it yelp-batch-project-postgres-1 psql -U airflow -d airflow -f /tmp/init-schema.sql
```

---

### 2. `README.md` (15 KB)
**Purpose**: Complete setup and usage guide

**Contents**:
- ✅ Database schema documentation
- ✅ Setup instructions (automatic + manual)
- ✅ Connection details (Docker, host, JDBC)
- ✅ Common SQL queries (quick reference)
- ✅ Schema management best practices
- ✅ Troubleshooting guide
- ✅ Maintenance procedures

**Read this first** if you're new to the PostgreSQL integration.

---

## Quick Start

### 1. Initialize Database
```powershell
docker compose down --volumes
docker compose up -d
```

### 2. Verify Setup
```powershell
docker exec -it yelp-batch-project-postgres-1 psql -U airflow -d airflow -c "\dt gold.*"
```

### 3. Query Data
```sql
SELECT * FROM gold.business_popularity LIMIT 5;
SELECT * FROM gold.v_top_businesses_by_city LIMIT 10;
```

---

## Need Help?

- **Setup issues?** → See [README.md#setup-instructions](README.md#setup-instructions)
- **Connection problems?** → See [README.md#connection-details](README.md#connection-details)
- **Query examples?** → See [README.md#common-queries](README.md#common-queries)
- **Errors?** → See [README.md#troubleshooting](README.md#troubleshooting)

---

**Last Updated**: November 25, 2025

