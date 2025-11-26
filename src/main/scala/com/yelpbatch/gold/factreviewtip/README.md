# Fact Review & Tip (Business) — Aggregation

Location: `src/main/scala/com/yelpbatch/gold/factreviewtip/README.md`

## Purpose
- Build a gold fact table combining review and tip metrics per business and period (daily and monthly).
- Produce an idempotent, partitioned Delta table in **long format** (one row per business + measure) consumable by analytics and downstream jobs.
- Support both daily (granularity=0) and monthly (granularity=2) aggregations.

## Files
- `FactReviewTipBusinessAgg.scala` — main aggregator (run → readData → transform → persist).
- `FactReviewTipBusinessUtils.scala` — helper functions for metrics aggregation, column selection, and filters.
- `FactReviewTipBusinessConstants.scala` — measure IDs and column name constants.
- `README.md` — this document.

### Function Structure (read → transform → persist → run)

- **`run(spark, config, runDateStr, forceMonthOpt, skipDaily, dryRun)`** — Public orchestrator
  - Validates `runDate` and computes date ranges
  - Executes daily metrics (unless `skipDaily=true`)
  - Executes monthly metrics when `isEndOfMonth` or `forceMonthOpt` matches
  - Applies Spark configurations from config
  - Calls `readData` → `transform` → `persist` for each granularity

- **`readData(spark, silverPath, dateStart, dateEnd)`** — Private
  - Loads Silver inputs: `review_snapshot`, `tip_snapshot`, `business_snapshot`
  - Filters by date range `[dateStart, dateEnd]`
  - For `business`: filters by `day == dateEnd` and `is_open == 1` to avoid duplicate business versions
  - Returns `Map[String, DataFrame]`

- **`transform(dataframes, runDate, granularity)`** — Private
  - Aggregates review metrics (review_count, avg_stars, total_useful, total_funny, total_cool, distinct_users_review)
  - Aggregates tip metrics (tip_count, compliment_count_sum, distinct_users_tip)
  - Produces **long format**: one row per `business_id` + `measure`
  - Adds metadata: `day`, `period_month`, `granularity`
  - Returns DataFrame ready for persistence

- **`persist(spark, df, outputPath)`** — Private
  - Adds `_gold_ingest_ts` timestamp
  - Upserts into Delta using `IOUtils.upsertDeltaByKey`
  - **Upsert keys**: `business_id` + `day` + `granularity` + `measure`
  - **Partition columns**: `day` + `granularity`

## How to Run
From `Runner.scala`:
```bash
--process gold_fact_review_tip --env <env> --run_date YYYY-MM-DD [--force_monthly YYYY-MM] [--skip_daily] [--dry_run]
```

- Daily metrics run for every `runDate` (unless `--skip_daily`)
- Monthly metrics run when `runDate` is end-of-month OR `--force_monthly YYYY-MM` matches

## Input / Output

### Inputs (Silver)
- **`review_snapshot`** — columns: `review_id`, `business_id`, `user_id`, `date`, `stars`, `useful`, `funny`, `cool`
- **`tip_snapshot`** — columns: `business_id`, `user_id`, `date`, `compliment_count`
- **`business_snapshot`** — columns: `business_id`, `name`, `city`, `state`, `categories`, `day`, `is_open`

### Output (Gold)
- **Delta table**: `gold/fact_review_tip_metrics`
- **Format**: Long format (one row per business + measure)
- **Partitioned by**: `day` + `granularity`
- **Schema**:
  - `day`: date (YYYY-MM-DD, partition key)
  - `period_month`: string (YYYY-MM)
  - `granularity`: integer (0=Daily, 2=Monthly, partition key)
  - `business_id`: string (upsert key)
  - `measure`: integer (metric ID, upsert key)
  - `units`: double (metric value)
  - `_gold_ingest_ts`: timestamp

### Measure IDs (from `MeasureFactReviewTip`)
| Measure ID | Metric Name                | Description                          |
|------------|----------------------------|--------------------------------------|
| 1          | review_count               | Count of distinct reviews            |
| 2          | tip_count                  | Count of tips                        |
| 3          | compliment_count_sum       | Sum of compliment counts from tips   |
| 4          | avg_stars                  | Average review stars                 |
| 5          | total_useful               | Sum of useful votes                  |
| 6          | total_funny                | Sum of funny votes                   |
| 7          | total_cool                 | Sum of cool votes                    |
| 8          | distinct_users_review      | Count of distinct users who reviewed |
| 9          | distinct_users_tip         | Count of distinct users who tipped   |

### Example Output
```
+----------+------------+-----------+--------------------+-------+-----+--------------------+
|       day|period_month|granularity|         business_id|measure|units|     _gold_ingest_ts|
+----------+------------+-----------+--------------------+-------+-----+--------------------+
|2020-01-31|     2020-01|          0|JzQsy7_G0p-UZGYFM...|      1|  1.0|2025-11-16 12:16:...|
|2020-01-31|     2020-01|          0|JzQsy7_G0p-UZGYFM...|      8|  1.0|2025-11-16 12:16:...|
|2020-01-31|     2020-01|          0|JzQsy7_G0p-UZGYFM...|      4|  5.0|2025-11-16 12:16:...|
|2020-01-31|     2020-01|          0|JzQsy7_G0p-UZGYFM...|      5|  0.0|2025-11-16 12:16:...|
|2020-01-31|     2020-01|          0|JzQsy7_G0p-UZGYFM...|      6|  0.0|2025-11-16 12:16:...|
+----------+------------+-----------+--------------------+-------+-----+--------------------+
```

**Interpretation**: 
- For business `JzQsy7...` on `2020-01-31` (daily, granularity=0), 
each row represents a specific metric (e.g., review count, average stars, tip count) for that business and day.
- The `measure_id` column identifies the metric, and the `value` column gives its value. 
- This long format allows flexible aggregation and analysis across businesses and time periods.

---

## PostgreSQL Integration

### Upsert Strategy (Handling Re-runs)

When writing to PostgreSQL, the process uses a **delete-before-insert** strategy to handle duplicate keys:

**Delete Keys**: `day`, `period_month`, `granularity`

**How it works**:
1. **Identifies** all unique combinations of `(day, period_month, granularity)` in the DataFrame
2. **Deletes** all existing PostgreSQL records matching those keys
3. **Inserts** the new data

**Example**: Running for `2020-01-31` with both daily (`granularity=0`) and monthly (`granularity=2`) metrics:
- Deletes all rows where `day='2020-01-31'` AND `period_month='2020-01'` AND `granularity IN (0, 2)`
- Inserts fresh metrics for all businesses

**Benefits**:
- ✅ **Idempotent**: Safe to re-run the same date multiple times
- ✅ **No duplicates**: Old data removed before new data inserted
- ✅ **Granular**: Only deletes the specific day/period/granularity being processed

**Logs**:
```
[PostgreSQLWriter] Upserting 150000 rows to PostgreSQL table: gold.fact_review_tip_metrics_wide
[PostgreSQLWriter] Delete keys: day, period_month, granularity
[PostgreSQLWriter] Deleting existing records from gold.fact_review_tip_metrics_wide
[PostgreSQLWriter] Deleted 150000 existing rows
[PostgreSQLWriter] Inserting 150000 new rows
[PostgreSQLWriter] Successfully upserted 150000 rows
```
