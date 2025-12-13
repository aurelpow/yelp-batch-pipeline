# Spark Deploy Mode Configuration Guide

## Overview
This guide explains how Spark deploy mode is managed across different environments (local, dev, prod) in the Yelp Batch Pipeline project.

## Configuration Files Quick Reference

The project uses different config files for different execution environments:

| Config File      | Usage | Execution Environment            | Paths | MongoDB URI |
|------------------|-------|----------------------------------|-------|-------------|
| **`local.conf`** | Local Windows development | `bin\run-local.cmd --env local`  | `C:/Users/aureb/...` | `mongodb://localhost:27017` |
| **`dev.conf`**   | Docker/Airflow testing | Airflow DAG with `"env": "dev"`  | `/opt/airflow/...` | `mongodb://mongo:27017` |
| **`prod.conf`**  | Production deployment | Airflow DAG with `"env": "prod"` | Cloud storage paths | External MongoDB URI |

**Key Point**: 
- Use **`local.conf`** when running Spark jobs **directly from Windows** (no Docker)
- Use **`dev.conf`** when running Spark jobs **inside Docker containers** (via Airflow)
- Use **`prod.conf`** for **production** deployments

## What is Spark Deploy Mode?

Spark deploy mode determines where the Spark driver process runs:

- **`client` mode**: Driver runs on the machine that submits the job (e.g., Airflow scheduler)
  - ✅ Better for development/debugging (logs visible in submitter)
  - ✅ Good for small jobs with interactive workflows
  - ❌ Limited by the client machine's resources
  - ❌ Job fails if client disconnects

- **`cluster` mode**: Driver runs on a worker node in the cluster
  - ✅ Scalable - driver uses cluster resources
  - ✅ Job continues even if client disconnects
  - ✅ Best for production environments
  - ❌ Harder to debug (logs are on the cluster)

## Configuration Structure

### 1. Configuration Files (Scala)

Deploy mode is set per environment in the config files:

**`application.conf`** (default/fallback):
```hocon
spark {
  deployMode = "client"
}
```

**`local.conf`** (local Windows execution - without Docker):
```hocon
spark {
  deployMode = "client"
}
```

**`dev.conf`** (Docker/Airflow execution):
```hocon
spark {
  deployMode = "client"
}
```

**`prod.conf`** (production):
```hocon
spark {
  deployMode = "cluster"
}
```

### 2. Airflow DAG Configuration

The DAG uses **Airflow Variables** for Spark infrastructure settings:

```python
# Spark infrastructure config (where driver runs) - from Airflow Variable
DEPLOY_MODE = Variable.get("spark_deploy_mode", default_var="client")  # client | cluster

# Application config (which .conf file to load) - from DAG run trigger or default
DEFAULT_ENV = "local"  # default if not specified in trigger
```

Each SparkSubmitOperator uses these values:
```python
SparkSubmitOperator(
    task_id="bronze",
    application=JAR_PATH,
    java_class=CLASS,
    deploy_mode=DEPLOY_MODE,  # ← Airflow Variable for Spark infrastructure
    application_args=[
        "--process", "bronze_ingest",
        "--env", "{{ dag_run.conf.get('env', 'local') }}",  # ← From DAG trigger, passed to Runner
        ...
    ],
)
```

**Key Distinction:**
- `deploy_mode` = **Spark infrastructure** (where driver runs) - set once via Airflow Variable
- `--env` argument = **Application config** (which config file Runner loads) - can vary per DAG run

## Setup Instructions

### Step 1: Set Airflow Variables

**Only ONE Airflow Variable is needed** - for Spark infrastructure configuration:

| Key | Value | Description |
|-----|-------|-------------|
| `spark_deploy_mode` | `client` or `cluster` | Where Spark driver runs (Spark infrastructure) |

> **Note**: `env` (which config file to load) is **NOT** an Airflow Variable - it's passed per DAG run trigger via JSON config.

**Via Airflow UI:**
1. Navigate to: `Admin` → `Variables`
2. Click `+` to add new variable
3. Add:
   - **Key**: `spark_deploy_mode`, **Value**: `client`

**Via Airflow CLI (inside container):**
```bash
docker exec -it yelp-batch-project-airflow-scheduler-1 bash
airflow variables set spark_deploy_mode "client"
exit
```

**Via Python script:**
```python
from airflow.models import Variable
Variable.set("spark_deploy_mode", "client")
```

### Step 2: Configure Spark Connection (Optional)

For more advanced configuration, you can set deploy mode in the Spark connection:

1. Navigate to: `Admin` → `Connections`
2. Edit connection: `spark_default`
3. In the **Extra** field, add:
   ```json
   {
     "deploy-mode": "client",
     "spark-submit": "spark-submit"
   }
   ```

> **Note**: The DAG's `deploy_mode` parameter overrides the connection's setting.

## Environment-Specific Setup

### Docker/Airflow Environment (dev.conf)

**Deploy Mode**: `client` (Spark infrastructure - driver runs in Airflow container)  
**Environment Config**: `dev` (Application config - loads `dev.conf`)  
**Reason**: Airflow scheduler and Spark run in the same Docker container  
**Paths**: Docker container paths (`/opt/airflow/...`)  
**MongoDB URI**: `mongodb://mongo:27017` (Docker service name)

**Airflow Variables:**
```
spark_deploy_mode = "client"
```

**Trigger DAG via Airflow UI:**
```json
{
  "env": "dev",
  "run_date": "2020-01-31",
  "tables": ["business"]
}
```

### Local Windows Development (local.conf)

**Deploy Mode**: `client` (Spark infrastructure - for local development)  
**Environment Config**: `local` (Application config - loads `local.conf`)  
**Reason**: Running spark-submit from Windows machine without Docker  
**Paths**: Windows file paths (`C:/Users/aureb/...`)  
**MongoDB URI**: `mongodb://localhost:27017` (host machine)

**Airflow Variables (if using Airflow):**
```
spark_deploy_mode = "client"
```

**Command Line (without Airflow):**
```powershell
bin\run-local.cmd --process bronze_ingest --env local --run_date 2020-01-31
```

**DAG Trigger (with Airflow):**
```json
{
  "env": "dev",
  "run_date": "2020-01-31",
  "tables": ["business", "review"]
}
```

### Production Environment (AWS EMR / Databricks / Google Cloud Platform)

**Deploy Mode**: `cluster` (Spark infrastructure - driver runs on cluster)  
**Environment Config**: `prod` (Application config - loads prod.conf)  
**Reason**: Driver runs on cluster, better resource utilization

**Airflow Variables:**
```
spark_deploy_mode = "cluster"
```

**DAG Trigger:**
```json
{
  "env": "prod",
  "run_date": "2024-01-01",
  "tables": ["business", "review", "tip", "checkin"]
}
```

**Additional Spark Configuration:**
```python
SparkSubmitOperator(
    task_id="bronze",
    application="s3://bucket/jars/app.jar",  # JAR on shared storage
    java_class=CLASS,
    deploy_mode="cluster",  # From Airflow Variable
    conf={
        "spark.executor.instances": "10",
        "spark.executor.memory": "4g",
        "spark.driver.memory": "2g",
    },
    application_args=[
        "--process", "bronze_ingest",
        "--env", "{{ dag_run.conf.get('env', 'prod') }}",  # From trigger
        ...
    ],
)
```

## Verification & Troubleshooting

### Check Current Configuration

**1. Verify Airflow Variables:**
```bash
docker exec -it yelp-batch-project-airflow-scheduler-1 airflow variables list
```

**2. Check Spark Connection:**
```bash
docker exec -it yelp-batch-project-airflow-scheduler-1 airflow connections get spark_default
```

**3. Test Spark Submit Command:**
```bash
docker exec -it yelp-batch-project-airflow-scheduler-1 spark-submit --version
```

### Common Issues

#### Issue 1: "yarn master requires HADOOP_CONF_DIR or YARN_CONF_DIR"

**Cause**: Airflow connection defaulted to `yarn` master instead of `local`

**Solution**: 
- Set `deploy_mode="client"` explicitly in SparkSubmitOperator
- Or update spark_default connection to use `local[*]` master

#### Issue 2: Driver runs but job fails immediately

**Cause**: In cluster mode, driver can't access local files

**Solution**:
- Ensure JAR and dependencies are on shared storage (S3, HDFS, etc.)
- Use `--jars` and `--driver-class-path` for external JARs

#### Issue 3: "Cannot load main class"

**Cause**: Driver can't find the application JAR

**Solution**:
```python
SparkSubmitOperator(
    application="s3://bucket/path/to/app.jar",  # Use remote path
    java_class="com.yelpbatch.app.Runner",
    deploy_mode="cluster",
)
```

## Best Practices

### ✅ DO:
- Use `client` mode for development and debugging
- Use `cluster` mode for production long-running jobs
- Set deploy mode via Airflow Variables for flexibility
- Keep JAR files on shared storage for cluster mode
- Monitor driver memory usage in cluster mode

### ❌ DON'T:
- Don't use cluster mode locally (driver won't start)
- Don't hardcode deploy mode in DAG (use variables)
- Don't run large jobs in client mode (resource limits)
- Don't forget to update variables when switching environments

## Spark Submit Command Examples

**Client Mode (Local Airflow):**
```bash
spark-submit \
  --master local[*] \
  --deploy-mode client \
  --class com.yelpbatch.app.Runner \
  /opt/airflow/jars/yelp-batch-project-assembly-0.1.0-SNAPSHOT.jar \
  --process bronze_ingest --env local
```

**Cluster Mode (Production):**
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class com.yelpbatch.app.Runner \
  s3://bucket/jars/yelp-batch-project-assembly-0.1.0-SNAPSHOT.jar \
  --process bronze_ingest --env prod
```

## Monitoring

### Client Mode Logs
- **Location**: Airflow task logs (`/opt/airflow/logs/dag_id=yelp_batch_pipeline/...`)
- **View**: Airflow UI → Task Instance → Logs

### Cluster Mode Logs
- **Location**: Cluster manager (YARN, Mesos, K8s)
- **View**: 
  - YARN: `yarn logs -applicationId <app_id>`
  - Spark UI: `http://<cluster>:4040`
  - Cloud provider UI (AWS EMR, Databricks, etc.)

## References

- [Spark Submitting Applications](https://spark.apache.org/docs/latest/submitting-applications.html)
- [Airflow SparkSubmitOperator](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html#sparksubmitoperator)
- [Spark Deploy Mode Comparison](https://spark.apache.org/docs/latest/cluster-overview.html#components)

---

**Last Updated**: 2025-12-09  
**Author**: Yelp Batch Pipeline Team

