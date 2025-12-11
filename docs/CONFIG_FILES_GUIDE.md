# Configuration Files Guide

## Quick Reference

The Yelp Batch Pipeline uses **environment-specific configuration files** to handle different execution contexts. Understanding which config file to use is crucial for proper pipeline operation.

---

## Configuration Files Overview

| File | Purpose | When to Use | Execution Method |
|------|---------|-------------|------------------|
| **`local.conf`** | Local Windows development | Running from Windows terminal WITHOUT Docker | `bin\run-local.cmd --env local` |
| **`dev.conf`** | Docker/Airflow environment | Running via Airflow DAG or Docker containers | Airflow trigger with `"env": "dev"` |
| **`prod.conf`** | Production deployment | Production environment with cloud infrastructure | Airflow trigger with `"env": "prod"` |

---

## Detailed Breakdown

### 1. `dev.conf` - Local Windows Development

**Use this when:**
- ✅ Testing locally on your Windows machine
- ✅ Running `bin\run-local.cmd` from PowerShell/CMD
- ✅ Not using Docker/Airflow
- ✅ Quick development iterations

**Configuration characteristics:**
```hocon
# File paths - Windows style
paths {
  rootDir = "C:/Users/aureb/Documents/yelp-batch-project"
  rawDir = ${paths.rootDir}"/data/raw"
  bronzeDir = ${paths.rootDir}"/data/bronze"
}

# MongoDB - host machine connection
mongodb {
  enabled = true
  uri = "mongodb://localhost:27017"
  database = "yelpAcademicDatasets"
}

# PostgreSQL - host machine or Docker mapped port
postgresql {
  enabled = true
  host = "localhost"              # or "host.docker.internal"
  port = 5433                     # Docker mapped port
  database = "yelp_analytics"
  user = "postgres"
  password = "your_password"
}

# Spark
spark {
  deployMode = "client"
}
```

**Example usage:**
```powershell
# Run bronze ingestion locally
bin\run-local.cmd --process bronze_ingest --env local --tables "business" --run_date 2020-01-31

# Run gold processing locally
bin\run-local.cmd --process gold_business_popularity --env local --run_date 2020-01-31
```

---

### 2. `dev.conf` - Docker/Airflow Environment

**Use this when:**
- ✅ Running jobs via Airflow DAG
- ✅ Testing the full Docker stack locally
- ✅ Simulating production-like environment
- ✅ Using Docker containers for execution

**Configuration characteristics:**
```hocon
# File paths - Docker container style
paths {
  rootDir = "/opt/airflow"
  rawDir = ${paths.rootDir}"/data/raw"
  bronzeDir = ${paths.rootDir}"/data/bronze"
}

# MongoDB - Docker service name (container-to-container)
mongodb {
  enabled = true
  uri = "mongodb://mongo:27017"
  database = "yelpAcademicDatasets"
}

# PostgreSQL - Docker service name (internal network)
postgresql {
  enabled = true
  host = "postgres"               # Docker service name
  port = 5432                     # Internal Docker port (NOT 5433!)
  database = "airflow"
  user = "airflow"
  password = "airflow"
}

# Spark
spark {
  deployMode = "client"
}
```

**Example usage (via Airflow UI):**
```json
{
  "env": "dev",
  "start_date": "2020-01-25",
  "end_date": "2020-01-31",
  "tables": ["business", "review"],
  "skip_bronze": "false"
}
```

---

### 3. `prod.conf` - Production Deployment

**Use this when:**
- ✅ Running in production environment
- ✅ Using cloud storage (S3, HDFS, etc.)
- ✅ External/managed databases
- ✅ Cluster mode deployment

**Configuration characteristics:**
```hocon
# File paths - cloud storage
paths {
  rootDir = "s3a://your-bucket/yelp-pipeline"
  rawDir = ${paths.rootDir}"/data/raw"
  bronzeDir = ${paths.rootDir}"/data/bronze"
}

# MongoDB - external/managed instance
mongodb {
  enabled = true
  uri = "mongodb://prod-mongo-cluster:27017"
  database = "yelpAcademicDatasets"
}

# PostgreSQL - external/managed instance
postgresql {
  enabled = true
  host = "prod-postgres.rds.amazonaws.com"
  port = 5432
  database = "yelp_analytics_prod"
  user = "${env.DB_USER}"           # From environment variable
  password = "${env.DB_PASSWORD}"   # From environment variable
}

# Spark - cluster mode for production
spark {
  deployMode = "cluster"
}
```

---

## Common Mistakes to Avoid

### ❌ Mistake 1: Using `dev.conf` for Windows local testing
```powershell
# WRONG - This will fail with path errors
bin\run-local.cmd --process bronze_ingest --env dev --run_date 2020-01-31
# Error: Cannot find path "/opt/airflow/data/raw" (Docker path doesn't exist on Windows)
```

**✅ Correct:**
```powershell
bin\run-local.cmd --process bronze_ingest --env local --run_date 2020-01-31
```

---

### ❌ Mistake 2: Using `local.conf` for Airflow DAG
```json
{
  "env": "local",
  "run_date": "2020-01-31"
}
```
This will fail because Airflow containers can't access Windows paths like `C:/Users/aureb/...`

**✅ Correct:**
```json
{
  "env": "dev",
  "run_date": "2020-01-31"
}
```

---

### ❌ Mistake 3: Wrong MongoDB URI in config
```hocon
# In local.conf (Docker) - WRONG
mongodb {
  uri = "mongodb://localhost:27017"  # ❌ Wrong - "localhost" in Docker points to container itself
}

# In dev.conf (Windows) - WRONG
mongodb {
  uri = "mongodb://mongo:27017"      # ❌ Wrong - "mongo" service name only exists in Docker network
}
```

**✅ Correct:**
```hocon
# dev.conf (Docker)
mongodb {
  uri = "mongodb://mongo:27017"      # ✅ Docker service name
}

# local.conf (Windows)
mongodb {
  uri = "mongodb://localhost:27017"  # ✅ Host machine
}
```

---

### ❌ Mistake 4: Wrong PostgreSQL port
```hocon
# In local.conf (Docker) - WRONG
postgresql {
  host = "postgres"
  port = 5433                         # ❌ Wrong - 5433 is the MAPPED port on host
}

# In dev.conf (Windows) - WRONG
postgresql {
  host = "localhost"
  port = 5432                         # ❌ Wrong - 5432 is used by local PostgreSQL
}
```

**✅ Correct:**
```hocon
# dev.conf (Docker - internal network)
postgresql {
  host = "postgres"
  port = 5432                         # ✅ Internal Docker port
}

# local.conf (Windows - host machine)
postgresql {
  host = "localhost"
  port = 5433                         # ✅ Mapped port to avoid conflicts
}
```

---

## Configuration Selection Flow

```
┌─────────────────────────────────────┐
│ How are you running the pipeline?  │
└──────────────┬──────────────────────┘
               │
               ├──→ From Windows terminal (bin\run-local.cmd)
               │   → Use: --env local
               │   → Config: local.conf
               │   → Paths: C:/Users/...
               │   → MongoDB: localhost:27017
               │
               ├──→ From Airflow Docker (DAG trigger)
               │   → Use: "env": "dev"
               │   → Config: dev.conf
               │   → Paths: /opt/airflow/...
               │   → MongoDB: mongo:27017
               │
               └──→ Production deployment
                   → Use: "env": "prod"
                   → Config: prod.conf
                   → Paths: s3a://...
                   → MongoDB: external URI
```

---

## Verification Checklist

Before running the pipeline, verify you're using the correct config:

### For Local Windows Development (`dev.conf`)
- [ ] Running from: Windows PowerShell/CMD
- [ ] Command: `bin\run-local.cmd --env dev`
- [ ] Paths: Windows style (`C:/Users/...`)
- [ ] MongoDB: `mongodb://localhost:27017`
- [ ] PostgreSQL: `localhost:5433`

### For Docker/Airflow (`local.conf`)
- [ ] Running from: Airflow UI or Docker exec
- [ ] Command: Trigger with `"env": "local"`
- [ ] Paths: Docker style (`/opt/airflow/...`)
- [ ] MongoDB: `mongodb://mongo:27017`
- [ ] PostgreSQL: `postgres:5432`

---

## Testing Your Configuration

### Test `dev.conf` (Windows)
```powershell
# 1. Verify MongoDB is accessible from Windows
docker exec yelp-batch-project-mongo-1 mongosh --quiet --eval "db.version()"

# 2. Run a simple bronze ingestion
bin\run-local.cmd --process bronze_ingest --env dev --tables "business" --run_date 2020-01-31

# 3. Check output in Windows paths
ls data/bronze/business
```

### Test `local.conf` (Docker/Airflow)
```powershell
# 1. Verify all containers are running
docker ps

# 2. Trigger DAG via Airflow UI with config:
# {
#   "env": "local",
#   "run_date": "2020-01-31",
#   "tables": ["business"]
# }

# 3. Check logs in Airflow UI or container logs
docker logs yelp-batch-project-airflow-scheduler-1 --tail 100
```

---

## Environment Variables vs Config Files

**Config Files** (`.conf`):
- ✅ Application-level settings (paths, databases, Spark config)
- ✅ Selected via `--env` parameter or DAG trigger `"env": "..."`
- ✅ Version controlled (except sensitive values)

**Environment Variables** (`.env`):
- ✅ Infrastructure-level settings (Fernet key, Docker UID)
- ✅ Loaded by Docker Compose automatically
- ❌ Never committed to version control

**Airflow Variables** (UI → Admin → Variables):
- ✅ Runtime Airflow settings (`spark_deploy_mode`)
- ✅ Stored in Airflow database
- ✅ Can be changed without redeploying code

---

## Summary

| Scenario | Config File | Command/Trigger |
|----------|-------------|----------------|
| Quick local test on Windows | `dev.conf` | `bin\run-local.cmd --env dev` |
| Full pipeline test with Airflow | `local.conf` | Airflow UI: `"env": "local"` |
| Production deployment | `prod.conf` | Airflow UI: `"env": "prod"` |

**Remember:** The `--env` or `"env"` parameter determines which `.conf` file is loaded. Always match your execution environment to the correct config file!

---

## Additional Resources

- [MongoDB Setup Guide](MONGODB_SETUP.md) - Detailed MongoDB configuration
- [Spark Deploy Mode Guide](SPARK_DEPLOY_MODE_SETUP.md) - Spark infrastructure config
- [Main README](../README.md) - Overall project documentation
