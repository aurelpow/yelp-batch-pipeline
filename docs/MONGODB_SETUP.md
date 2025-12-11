# MongoDB Setup Guide

This guide explains how to set up MongoDB as an optional data source for the Yelp Batch ETL Pipeline.

## Overview

MongoDB serves as an alternative data source to the file system. Instead of reading raw JSON files directly from `data/raw/`, the pipeline can ingest data from MongoDB collections.

**Why use MongoDB?**
- ✅ Better for production environments with centralized data storage
- ✅ Supports concurrent reads by multiple Spark executors
- ✅ Enables incremental updates and change data capture
- ✅ Provides better data governance and access control

**When to use file system instead?**
- 🔹 Local development and testing
- 🔹 One-time data processing
- 🔹 Simpler setup without database overhead

---

## Prerequisites

- Docker and Docker Compose installed
- MongoDB container running (included in `docker-compose.yml`)
- Raw Yelp JSON files downloaded from Kaggle (see main README)

---

## Quick Start

### 1. Start MongoDB Container

```powershell
# Start MongoDB service only
docker compose up -d mongo

# Verify it's running
docker ps --filter "name=mongo"
```

The MongoDB service will:
- Run on port `27017` (mapped to host)
- Store data persistently in Docker volume `mongo_data`
- Be accessible from both host and Airflow containers

### 2. Import Yelp Data

Use the automated import script to load all JSON files:

```powershell
# Run from project root
.\scripts\import-mongo-data.ps1
# If you get permission errors, run with bypass:
powershell -ExecutionPolicy Bypass -File .\scripts\import-mongo-data.ps1
```

**What the script does:**
1. ✅ Detects running MongoDB container automatically
2. ✅ Validates JSON files exist in `data/raw/`
3. ✅ Copies files to container's `/tmp` directory
4. ✅ Imports each file using `mongoimport` (newline-delimited JSON format)
5. ✅ Verifies document counts for each collection
6. ✅ Cleans up temporary files

**Expected output:**
```
=== Importing Yelp Data into MongoDB Container ===

Checking MongoDB container...
[OK] Container is running: yelp-batch-project-mongo-1

--- Importing business ---
  File: yelp_academic_dataset_business.json (118.86 MB)
  → Copying file to container...
  → Importing to MongoDB (this may take a while)...
  [OK] Imported successfully: 150346 documents

--- Importing review ---
  File: yelp_academic_dataset_review.json (5.34 GB)
  → Copying file to container...
  → Importing to MongoDB (this may take 5-10 minutes)...
  [OK] Imported successfully: 6990280 documents

... (similar for tip, checkin, user)

=== Verification ===
Database: yelpAcademicDatasets
Collections:
  - business: 150346 documents
  - checkin: 131930 documents
  - review: 6990280 documents
  - tip: 908915 documents
  - user: 1987897 documents

[SUCCESS] Import complete!
```

**Import time estimates:**
- `business.json` (119 MB): ~30 seconds
- `checkin.json` (287 MB): ~1 minute
- `tip.json` (181 MB): ~45 seconds
- `user.json` (3.36 GB): ~5-8 minutes
- `review.json` (5.34 GB): ~8-12 minutes

**Total import time**: ~15-20 minutes

### 3. Enable MongoDB in Configuration

Edit your environment config file based on how you run the pipeline:

**For Docker/Airflow** (`src/main/resources/local.conf`):
```hocon
# local.conf - for Airflow/Docker execution
mongodb {
  enabled = true
  uri = "mongodb://mongo:27017"           # Docker service name
  database = "yelpAcademicDatasets"
}
```

**For Local Development** (`src/main/resources/dev.conf`):
```hocon
# dev.conf - for local Windows execution (bin\run-local.cmd)
mongodb {
  enabled = true
  uri = "mongodb://localhost:27017"      # Host machine connection
  database = "yelpAcademicDatasets"
}
```

**Connection URI guide:**
- **`dev.conf`** (Airflow/Docker): `mongodb://mongo:27017` (container-to-container)
- **`local.conf`** (Local Windows): `mongodb://localhost:27017` (host machine)
- **`prod.conf`** (Production): External MongoDB URI

### 4. Verify Setup

Check MongoDB connection and data:

```powershell
# Connect to MongoDB shell
docker exec -it yelp-batch-project-mongo-1 mongosh

# Inside mongosh:
use yelpAcademicDatasets
db.getCollectionNames()
db.business.countDocuments()
db.business.findOne()
exit
```

---

## Manual Import (Alternative)

If you prefer manual control or the script fails:

```powershell
# 1. Copy files to container
docker cp data/raw/yelp_academic_dataset_business.json yelp-batch-project-mongo-1:/tmp/

# 2. Import using mongoimport
docker exec yelp-batch-project-mongo-1 mongoimport \
  --db yelpAcademicDatasets \
  --collection business \
  --file /tmp/yelp_academic_dataset_business.json

# 3. Verify
docker exec yelp-batch-project-mongo-1 mongosh --quiet \
  --eval "db.getSiblingDB('yelpAcademicDatasets').business.countDocuments()"

# 4. Repeat for other collections (review, tip, checkin, user)
```

---

## Data Persistence

### ⚠️ Important: Docker Volume Configuration

By default, MongoDB data is **persistent** across container restarts, but **will be lost** if you run `docker compose down` without a named volume.

**Current setup** (in `docker-compose.yml`):
```yaml
services:
  mongo:
    image: mongo:7
    volumes:
      - mongo_data:/data/db  # Named volume for persistence

volumes:
  mongo_data:
    driver: local
```

> **⚠️ First-time setup**: After adding the volume configuration to `docker-compose.yml`, you must recreate containers:
> ```powershell
> docker compose down
> docker compose up -d mongo
> ```
> This creates the `mongo_data` volume. Then re-import your data.

**Volume behavior:**
- ✅ `docker compose stop` → Data **preserved**
- ✅ `docker compose restart mongo` → Data **preserved**
- ✅ `docker compose down` (default) → Data **preserved** (volume remains)
- ❌ `docker compose down --volumes` → Data **deleted** (removes volumes)
- ❌ Removing volume manually → Data **deleted**

**Check existing volumes:**
```powershell
docker volume ls | Select-String "mongo"
docker volume inspect yelp-batch-project_mongo_data
```

**Backup strategy:**
```powershell
# Export all collections to JSON
docker exec yelp-batch-project-mongo-1 mongodump \
  --db yelpAcademicDatasets \
  --out /tmp/backup

# Copy backup from container to host
docker cp yelp-batch-project-mongo-1:/tmp/backup ./mongo_backup

# Restore from backup
docker exec yelp-batch-project-mongo-1 mongorestore \
  --db yelpAcademicDatasets \
  /tmp/backup/yelpAcademicDatasets
```

---

## Running the Pipeline with MongoDB

Once MongoDB is configured and populated:

### Local Execution (Testing)

Run from Windows terminal **without Docker**:

```powershell
# Uses dev.conf (local Windows paths + localhost MongoDB)
bin\run-local.cmd --process bronze_ingest --env dev --tables "business" --run_date 2020-01-31
```

**Configuration check** (`src/main/resources/dev.conf`):
```hocon
# dev.conf - for local Windows execution
mongodb {
  enabled = true
  uri = "mongodb://localhost:27017"  # Host machine connection
  database = "yelpAcademicDatasets"
}

paths {
  rootDir = "C:/Users/aureb/Documents/yelp-batch-project"  # Windows paths
  rawDir = ${paths.rootDir}"/data/raw"
}
```

### Airflow Execution (Docker)

Trigger DAG via Airflow UI with configuration:

```json
{
  "env": "local",
  "start_date": "2020-01-25",
  "end_date": "2020-01-31",
  "tables": ["business", "review", "tip"],
  "skip_bronze": "false"
}
```

**Configuration check** (`src/main/resources/local.conf`):
```hocon
# local.conf - for Airflow/Docker execution
mongodb {
  enabled = true
  uri = "mongodb://mongo:27017"  # Docker service name (container-to-container)
  database = "yelpAcademicDatasets"
}

paths {
  rootDir = "/opt/airflow"  # Docker container paths
  rawDir = ${paths.rootDir}"/data/raw"
}
```

---

## Troubleshooting

### Quick Reference

| Problem | Quick Fix |
|---------|-----------|
| Script blocked (UnauthorizedAccess) | `powershell -ExecutionPolicy Bypass -File .\scripts\import-mongo-data.ps1` |
| Volume not found (no such volume) | Rebuild: `docker compose down; docker compose up -d mongo` then re-import |
| Connection refused | Check MongoDB is running: `docker ps --filter "name=mongo"` |
| Collection returns 0 | Re-run import or check database name |
| Data lost after restart | Check volume exists: `docker volume ls \| Select-String "mongo"` |

---

### Problem: "Connection refused" error

**Symptoms:**
```
com.mongodb.MongoSocketOpenException: Exception opening socket
Caused by: java.net.ConnectException: Connection refused
```

**Solutions:**
```powershell
# 1. Check MongoDB is running
docker ps --filter "name=mongo"

# 2. Check connection from inside Airflow container
docker exec yelp-batch-project-airflow-scheduler-1 ping mongo
docker exec yelp-batch-project-airflow-scheduler-1 nc -zv mongo 27017

# 3. Verify correct URI in config
# - From Airflow: mongodb://mongo:27017 (service name)
# - From host: mongodb://localhost:27017

# 4. Check MongoDB logs
docker logs yelp-batch-project-mongo-1 --tail 50
```

### Problem: "Database/collection not found"

**Symptoms:**
```
MongoSparkException: Partitioning failed
db.getSiblingDB('yelpAcademicDatasets').business.countDocuments() returns 0
```

**Solutions:**
```powershell
# 1. List all databases
docker exec yelp-batch-project-mongo-1 mongosh --quiet \
  --eval "db.adminCommand('listDatabases')"

# 2. Check which database has the data
docker exec yelp-batch-project-mongo-1 mongosh --quiet --eval "
db.adminCommand('listDatabases').databases.forEach(function(d) {
    var dbObj = db.getSiblingDB(d.name);
    print(d.name + ': ' + dbObj.getCollectionNames());
});
"

# 3. If data is in wrong database, rename or re-import
docker exec yelp-batch-project-mongo-1 mongosh --quiet --eval "
db.getSiblingDB('wrongName').business.renameCollection('business', {dropTarget: true});
"

# 4. Re-run import script
.\scripts\import-mongo-data.ps1
```

### Problem: Import script fails on large files

**Symptoms:**
```
mongoimport: error reading separator after document
```

**Solutions:**
```powershell
# 1. Verify JSON file format (newline-delimited, not array)
# Correct format:
# {"business_id": "abc", ...}
# {"business_id": "def", ...}

# Incorrect format:
# [{"business_id": "abc", ...}, {"business_id": "def", ...}]

# 2. Increase Docker memory limit (Settings → Resources → Memory)
# Recommended: 8GB+ for large files

# 3. Import in batches using --limit
docker exec yelp-batch-project-mongo-1 mongoimport \
  --db yelpAcademicDatasets \
  --collection review \
  --file /tmp/yelp_academic_dataset_review.json \
  --numInsertionWorkers 4
```

### Problem: Volume not found / "no such volume" error

**Symptoms:**
```powershell
docker volume ls | Select-String "mongo"
# No output

docker volume inspect yelp-batch-project_mongo_data
# Error: no such volume
```

**Cause:** Volume configuration added to `docker-compose.yml` but containers not recreated yet.

**Solution:**
```powershell
# 1. Stop and remove existing containers
docker compose down

# 2. Recreate with new volume configuration
docker compose up -d mongo

# 3. Verify volume was created
docker volume ls | Select-String "mongo"
# Should show: yelp-batch-project_mongo_data

# 4. Re-import data (volume starts empty)
powershell -ExecutionPolicy Bypass -File .\scripts\import-mongo-data.ps1
```

### Problem: Data lost after `docker compose down`

**Cause:** Ran `docker compose down --volumes` or removed volume manually

**Solution:**
```powershell
# 1. Always use named volumes (already configured)
# Check docker-compose.yml has:
#   volumes:
#     - mongo_data:/data/db

# 2. Avoid --volumes flag unless you want fresh start
docker compose down            # ✅ Keeps data
docker compose down --volumes  # ❌ Deletes data

# 3. Re-import data
.\scripts\import-mongo-data.ps1
```

---

## Performance Tuning

### MongoDB Container Resources

Edit `docker-compose.yml`:
```yaml
services:
  mongo:
    image: mongo:7
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 2G
```

### Spark MongoDB Connector Options

In your Scala code (`BronzeIngest.scala`):
```scala
val df = spark.read
  .format("mongodb")
  .option("connection.uri", mongoUri)
  .option("database", database)
  .option("collection", collectionName)
  .option("partitioner", "MongoSamplePartitioner")     // Faster partitioning
  .option("partitionerOptions.partitionSizeMB", "64")  // Adjust partition size
  .option("partitionerOptions.samplesPerPartition", "10")
  .load()
```

### Indexing (Optional)

For better query performance when using MongoDB as source:
```javascript
// Connect to MongoDB
docker exec -it yelp-batch-project-mongo-1 mongosh

use yelpAcademicDatasets

// Create indexes for common query patterns
db.business.createIndex({ business_id: 1 })
db.review.createIndex({ business_id: 1, date: -1 })
db.tip.createIndex({ business_id: 1, date: -1 })
db.checkin.createIndex({ business_id: 1 })
```

---

## Switching Between MongoDB and File System

You can toggle between MongoDB and file system without code changes by modifying the appropriate config file:

### Use MongoDB

**For Airflow/Docker** (`local.conf`):
```hocon
mongodb {
  enabled = true
  uri = "mongodb://mongo:27017"  # Docker service name
  database = "yelpAcademicDatasets"
}
```

**For Local Development** (`dev.conf`):
```hocon
mongodb {
  enabled = true
  uri = "mongodb://localhost:27017"  # Host machine
  database = "yelpAcademicDatasets"
}
```

### Use File System

Set `enabled = false` in the appropriate config file:
```hocon
# local.conf or dev.conf
mongodb {
  enabled = false  # Falls back to data/raw/*.json files
}
```

The pipeline automatically detects the `mongodb.enabled` flag and chooses the appropriate data source.

---

## Verification Checklist

Before running the pipeline, verify:

- [ ] MongoDB container is running: `docker ps --filter "name=mongo"`
- [ ] Data is imported: `docker exec ... mongosh --eval "db.business.countDocuments()"`
- [ ] Volume is configured: `docker volume ls | grep mongo`
- [ ] Config has correct URI: `mongodb://mongo:27017` (from Airflow) or `mongodb://localhost:27017` (from host)
- [ ] Config has `enabled = true`
- [ ] Can ping MongoDB from Airflow: `docker exec airflow-scheduler-1 ping mongo`

**Quick validation script:**
```powershell
# With bypass if needed
powershell -ExecutionPolicy Bypass -File .\scripts\check-mongo-setup.ps1

# Or directly
.\scripts\check-mongo-setup.ps1
```

---

## Additional Resources

- **MongoDB Documentation**: https://docs.mongodb.com/
- **MongoDB Spark Connector**: https://www.mongodb.com/docs/spark-connector/current/
- **Docker Volumes Guide**: https://docs.docker.com/storage/volumes/

---

## FAQ

**Q: Should I use MongoDB or file system for local development?**  
A: File system is simpler for local dev. Use MongoDB when testing the full production setup.

**Q: Can I use both MongoDB and file system simultaneously?**  
A: No, the pipeline uses one source at a time based on `mongodb.enabled` flag.

**Q: How much disk space does MongoDB need?**  
A: ~15-20 GB for all collections (business: 0.5GB, checkin: 1GB, tip: 0.7GB, user: 5GB, review: 10GB).

**Q: Is the MongoDB data compressed?**  
A: MongoDB uses WiredTiger compression by default (~3:1 ratio). Raw JSON is ~9.3 GB, stored size is ~3-4 GB.

**Q: Can I use an external MongoDB instance (not Docker)?**  
A: Yes, just update the `uri` in your config to point to the external instance.

