# Business Popularity Aggregation

## Purpose
Compute a monthly business popularity score and write results to the Gold layer. The process implements a clear separation of concerns:
- `read` — load Silver inputs for the target month
- `transform` — aggregate metrics, normalize, compute recency and score
- `persist` — idempotent write/upsert to Gold
- `run` — orchestrator that executes only for End-Of-Month dates

## Files
- `BusinessPopularityAgg.scala` — main aggregator with `readData`, `transform`, `persist`, `run`
- `BusinessPopularityUtils.scala` — helpers (e.g. `minMaxNormalize`)
- `BusinessPopularityConstants.scala` — column names and weights

## Function responsibilities
- `readData(spark, silverPath, dateStart, dateEnd)`
    - Read required Silver sources: `business_snapshot`, `review_snapshot`, `tip_snapshot`, `checkin_snapshot`
    - Filter ranges for reviews/tips/checkins by `[dateStart, dateEnd]`
    - IMPORTANT: for `business` use `day == dateEnd` (or the run day) and `dropDuplicates(Seq("business_id"))` to avoid multiple business versions creating duplicated output rows

- `transform(dataFrames, dateEnd, periodMonth)`
    - Aggregate metrics per `business_id` (review count, avg stars, checkin count, compliment sum)
    - Apply min-max normalization (uses `minMaxNormalize`)
    - Compute recency boost and final `popularity_score`
    - Compute `city` rank and add `period_month` column

- `persist(spark, df, goldPath)`
    - Add ingestion timestamp and upsert into Delta using `IOUtils.upsertDeltaByKey`
    - Upsert keys: `business_id` + `day` + `granularity`
    - Partition by `day` and `granularity` for efficient writes

- `run(spark, config, runDate)`
    - Validates `runDate`
    - Executes only when `DateUtils.isEndOfMonthStr(runDate)` is true
    - Builds `firstDayOfMonth`, uses `runDate` as last day, calls `readData` -> `transform` -> `persist`

## How to run
From `Runner.scala` call:
```bash
--process gold_business_popularity --env <env> --run_date YYYY-MM-DD
```
Only YYYY-MM-DD that is end-of-month will execute.

## Input/Output
- **Inputs (Silver)**:
  - `business_snapshot` (filtered by `day == runDate`)
  - `review_snapshot` (filtered by `date` in `[firstDayOfMonth, runDate]`)
  - `tip_snapshot` (filtered by `date` in `[firstDayOfMonth, runDate]`)
  - `checkin_snapshot` (filtered by `checkin_date` in `[firstDayOfMonth, runDate]`)

- **Output (Gold)**:
  - Delta table: `gold/business_popularity`
  - Schema:
    - `day`: date (YYYY-MM-DD, partition key)
    - `period_month`: string (YYYY-MM)
    - `granularity`: integer (2 = Monthly, partition key)
    - `business_id`: string (upsert key)
    - `name`: string
    - `city`: string
    - `state`: string
    - `categories`: string
    - `popularity_score`: double
    - `city_rank`: integer (rank within city by popularity_score)
    - `_gold_ingest_ts`: timestamp
  
Output is **partitioned by `day` and `granularity`**

### Example output:
```
+----------+------------+-----------+--------------------+--------------------+--------+-----+--------------------+--------------------+---------+--------------------+
|       day|period_month|granularity|         business_id|                name|    city|state|          categories|    popularity_score|city_rank|     _gold_ingest_ts|
+----------+------------+-----------+--------------------+--------------------+--------+-----+--------------------+--------------------+---------+--------------------+
|2020-01-31|     2020-01|          2|C2vKa-eZFVBrfFGYX...|         Senor Salsa|Abington|   PA|Seafood, Restaura...|  0.5459113395261017|        1|2025-11-16 12:17:...|
|2020-01-31|     2020-01|          2|VaHG9BwrSZ-oNv0mG...|         First Watch|Abington|   PA|American (Traditi...|  0.5453409973207786|        2|2025-11-16 12:17:...|
|2020-01-31|     2020-01|          2|5N-93oMmm0MUt8Hvy...|       Boston Market|Abington|   PA|Restaurants, Fast...|  0.4599956127522668|        3|2025-11-16 12:17:...|
|2020-01-31|     2020-01|          2|bSKXUH3M69T5FWPCP...|Bonnet Lane Famil...|Abington|   PA|Breakfast & Brunc...|7.604562737642586E-4|        4|2025-11-16 12:17:...|
|2020-01-31|     2020-01|          2|H1FXzbmnMPXRfOrTo...|         Kung Fu Tea|Abington|   PA|Specialty Food, M...|5.703422053231939E-4|        5|2025-11-16 12:17:...|
+----------+------------+-----------+--------------------+--------------------+--------+-----+--------------------+--------------------+---------+--------------------+
```

## Debugging tips
- Run counts and distinct counts on each source and aggregated frames (e.g. `reviewAggDF.select("business_id").distinct().count()`) to find where multiplicity appears.
- Check logs from `IOUtils.upsertDeltaByKey` to see whether a merge or overwrite was used.
- Sample query to inspect a business:
```scala
df.filter(col("business_id") === "FEXhWNCMkv22qG04E83Qjg").show()
``` 

