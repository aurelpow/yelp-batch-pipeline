# Yelp Batch ETL Pipeline


## Project Overview
This repository contains a batch ETL (Extract, Transform, Load) pipeline designed to process Yelp dataset files. 
The pipeline extracts data from JSON files(from storage or MongoDB), transforms it into a structured format(Parquet), and loads it into a relational database for further analysis.
The ETL process is split into multiple stages, following the Medallion architecture (Bronze, Silver, Gold) to ensure data quality and consistency.

## Dataset Source

The data used in this project comes from the **Yelp Open Dataset**, available on Kaggle:
- **Source**: [Yelp Dataset on Kaggle](https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset/)
- **Coverage**: Businesses across 8 metropolitan areas in the USA and Canada

### Required Files
Download the following 5 JSON files from Kaggle:

1. **yelp_academic_dataset_business.json** (118.86 MB)
   - Business information including location, categories, ratings, and attributes

2. **yelp_academic_dataset_checkin.json** (286.96 MB)
   - Check-in data for businesses over time

3. **yelp_academic_dataset_review.json** (5.34 GB)
   - Full review text data with user ratings and timestamps

4. **yelp_academic_dataset_tip.json** (180.6 MB)
   - Tips written by users about businesses

5. **yelp_academic_dataset_user.json** (3.36 GB)
   - User profile information and social network data

**Total dataset size**: ~9.3 GB

### Setup Options

You have two options for setting up the data source:

#### Option 1: File System (Default)
Place the downloaded JSON files in the `data/raw/` directory:
```
data/raw/
├── yelp_academic_dataset_business.json
├── yelp_academic_dataset_checkin.json
├── yelp_academic_dataset_review.json
├── yelp_academic_dataset_tip.json
└── yelp_academic_dataset_user.json
```

**Configuration** (`src/main/resources/application.conf`):
```hocon
mongodb {
  enabled = false  # File system mode
}
```

#### Option 2: MongoDB (Optional)
Load the JSON files into MongoDB collections for production-grade data ingestion.

**Quick Setup:**

1. **Start MongoDB container**:
   ```bash
   docker compose up -d mongo
   ```

2. **Run automated import script**:
   ```powershell
   # If script execution is blocked, use bypass flag
   powershell -ExecutionPolicy Bypass -File .\scripts\import-mongo-data.ps1
   
   # Or if scripts are allowed
   .\scripts\import-mongo-data.ps1
   ```
   This will import all 5 JSON files (~15-20 minutes total).

3. **Enable MongoDB in configuration**:
   - For **Airflow/Docker** (`src/main/resources/dev.conf`):
     ```hocon
     mongodb {
       enabled = true
       uri = "mongodb://mongo:27017"        # Docker service name
       database = "yelpAcademicDatasets"
     }
     ```
   - For **Local Windows** (`src/main/resources/local.conf`):
     ```hocon
     mongodb {
       enabled = true
       uri = "mongodb://localhost:27017"   # Host machine
       database = "yelpAcademicDatasets"
     }
     ```

4. **Verify data**:
   ```powershell
   docker exec -it yelp-batch-project-mongo-1 mongosh --quiet --eval "db.getSiblingDB('yelpAcademicDatasets').business.countDocuments()"
   ```

**📖 For complete setup guide, troubleshooting, and performance tuning, see**: [`docs/MONGODB_SETUP.md`](docs/MONGODB_SETUP.md)

> **Note**: Due to the large file sizes, these files are not included in the repository. You must download them separately from Kaggle before running the pipeline.

## Directory Structure

```
├── build.sbt
├── airflow
    ├── config
    │   └── airflow.cfg
    ├── dags
    │   └── yelp_batch_dag.py
    └── jars
        └── custom_operator.jar
├── project
│   ├── build.properties
│   └── plugins.sbt
├── src
│   ├── main
│   │   ├── resources
│   │   │   ├── log4j.properties
│   │   │   ├── application.conf
│   │   │   ├── dev.conf
│   │   │   ├── local.conf
│   │   │   └── prod.conf
│   │   ├── scala
│   │   │   └── com/yelpbatch
│   │   │       ├── app
│   │   │       ├── bronze
│   │   │       ├── silver
│   │   │       ├── gold
│   │   │       │   ├── businesspopularity
│   │   │       │   ├── factreviewtip
│   │   │       ├── utils
│   │   │   
│   └── test
│       └── scala
├── requirements.txt
├── README.md
├── .gitignore
├── docker-compose.yml
```

## Technologies Used
- **Apache Spark**: For distributed data processing and transformation.
- **Delta Lake**: For reliable data storage and management.
- **Apache Airflow:** For orchestrating and scheduling ETL workflows.
- **Scala**: Primary programming language for the ETL pipeline.
- **Python**: For Airflow DAGs and operators.
- **SBT**: Build tool for Scala projects.
- **Docker**: Containerization of Airflow environment.
- **MongoDB**: Optional data source for raw JSON data.

## ETL Pipeline Breakdown
The ETL pipeline is divided into three main stages:
1. **Bronze Layer**: Raw data ingestion from JSON files or MongoDB.
2. **Silver Layer**: Data cleaning and transformation into structured Parquet format.
3. **Gold Layer**: Final aggregation and preparation of data for analytics.

## How to Run the Project

### Prerequisites
Ensure the following tools are installed on your system:

- **Java**: 17.x (JDK)
- **Scala**: 2.12.20
- **SBT**: 1.9.9
- **Python**: 3.12.x
- **Docker**: 28.x or higher
- **Apache Spark**: 3.5.1 (included as dependency)
- **Apache Airflow**: 3.0.0 (runs in Docker)

### Installation Steps

#### 1. Clone the Repository
```bash
git clone <repository-url>
cd yelp-batch-project
```

#### 2. Build the Scala/Spark Application
```bash
# Clean and compile
sbt clean compile

# Run tests
sbt test

# Create assembly JAR
sbt assembly
```
The assembly JAR will be created at `target/scala-2.12/yelp-batch-project-assembly-0.1.0-SNAPSHOT.jar`

#### 3. Set Up Configuration Files
Edit the configuration files in `src/main/resources/` for your environment:
- **`local.conf`** - for local Windows development (without Docker)
- **`dev.conf`** - for Docker/Airflow environment 
- **`prod.conf`** - for production environment

> **📖 Important**: Understanding which config file to use is crucial! See [Configuration Files Guide](docs/CONFIG_FILES_GUIDE.md) for detailed explanations and common mistakes to avoid.

Key configurations to update:
- MongoDB connection strings (if using MongoDB as source)
- Data paths (bronze, silver, gold layers)
- Spark tuning parameters

#### 4. Run Locally (Without Airflow)

##### Bronze Layer - Ingest Raw Data
```bash
# Windows
bin\run-local.cmd --process bronze_ingest --env local --run_date 2020-01-31

# Linux/Mac
bin/run-local.sh --process bronze_ingest --env local --run_date 2020-01-31
```

##### Silver Layer - Transform and Clean Data
```bash
# Process specific date
bin\run-local.cmd --process silver_ingest --env local --run_date 2020-01-31

# Process date range
bin\run-local.cmd --process silver_ingest --env local --start_date 2020-01-01 --end_date 2020-01-31

# Process specific tables only
bin\run-local.cmd --process silver_ingest --env local --run_date 2020-01-31 --tables "business,review"
```

##### Gold Layer - Business Popularity Aggregation
```bash
# Runs only on end-of-month dates
bin\run-local.cmd --process gold_business_popularity --env local --run_date 2020-01-31
```

##### Gold Layer - Fact Review & Tip Metrics
```bash
# Daily metrics
bin\run-local.cmd --process gold_fact_review_tip --env local --run_date 2020-01-31

# Force monthly computation
bin\run-local.cmd --process gold_fact_review_tip --env local --run_date 2020-01-31 --force_monthly 2020-01

# Skip daily, run monthly only
bin\run-local.cmd --process gold_fact_review_tip --env local --run_date 2020-01-31 --skip_daily
```

#### 5. Run with Airflow (Orchestrated)

##### ⚠️ Security Setup (First Time Only)

Before starting Airflow, you need a Fernet key for encrypting credentials.

**Automated Setup (Recommended)** 🚀
```powershell
# Run the setup script to generate key and create .env file
python bin/setup.py

# Follow the prompts - it will:
# 1. Generate a secure Fernet key
# 2. Create .env file automatically
# 3. Show you next steps

# See the script: bin/setup.py
```

**Manual Setup (Alternative)**
```powershell
# 1. Copy the template
cp .env.example .env

# 2. Generate a Fernet key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 3. Edit .env and paste your generated key
# AIRFLOW_FERNET_KEY=<paste-your-key-here>
```

**Quick Start (Local Development Only)**
```powershell
# Use the pre-filled development key
cp .env.example .env
# ⚠️ Not secure for production!
```

**Verify** (after starting Airflow):
```powershell
# Check the Fernet key is loaded
docker exec yelp-batch-project-airflow-scheduler-1 env | Select-String "FERNET"
```

**⚠️ Security Note**: 
- The `.env` file is **gitignored** and never committed
- For production, always generate a new unique key

##### Start Airflow with Docker
```bash

# Build and start Airflow services
docker-compose up -d

# Check running services
docker-compose ps

# View logs
docker-compose logs -f airflow-scheduler
```

##### Access Airflow Web UI
- URL: `http://localhost:8080`
- Default credentials: `airflow` / `airflow`

##### Configure Spark Connection

You need to manually create the Spark connection in Airflow UI for the DAG to work properly.

**Step-by-step instructions:**

1. **Access Airflow UI**: Navigate to `http://localhost:8080`
2. **Login** with credentials: `airflow` / `airflow`
3. **Go to Connections**: Click **Admin** → **Connections** in the top menu
4. **Add Connection**: Click the **+** button to create a new connection
5. **Fill in the form**:
   - **Connection Id**: `spark_default`
   - **Connection Type**: Select `Spark` from the dropdown
   - **Host**: `local[*]`
   - **Port**: Leave empty
   - **Extra**: Click on "Edit as JSON" and paste:
     ```json
     {
     "queue": null,
     "deploy-mode": "client",
     "spark-binary": "spark-submit",
     "namespace": null
     }
     ```
6. **Save**: Click the **Save** button

**Important notes for Airflow 3.x:**
- ⚠️ Do NOT use `spark-home` in the Extra field (deprecated in Airflow 3.x)
- ✅ The `spark-submit` binary is already available at `/opt/spark/bin/spark-submit` in the Docker container
- ✅ Use `local[*]` as Host (not just `local`) to utilize all available CPU cores

**Verify the connection:**
- After saving, the connection should appear in the connections list
- The URI format should be: `spark://local[*]?deploy-mode=client&spark-binary=spark-submit`

**Troubleshooting:**
- If you see "Could not load connection string spark_default, defaulting to yarn" in logs, the connection wasn't created properly
- Make sure the Connection Id is exactly `spark_default` (lowercase, with underscore)
- Ensure Connection Type is set to `Spark` (not `HTTP` or other types)

##### Configure Airflow Variables

Set Spark infrastructure variable (one-time setup):

1. **Access Airflow UI**: Navigate to `http://localhost:8080`
2. **Go to Variables**: Click **Admin** → **Variables** in the top menu
3. **Add Variable**: Click the **+** button and add:

| Key | Value | Description |
|-----|-------|-------------|
| `spark_deploy_mode` | `client` | Where Spark driver runs (client=local, cluster=remote) |

> **Note**: The `env` parameter selects which config file to load:
> - `env: "local"` → uses `local.conf` (for local Windows execution)
> - `env: "dev"` → uses `dev.conf` (for Docker/Airflow)
> - `env: "prod"` → uses `prod.conf` (for production)
> 
> This is **NOT** an Airflow Variable - it's specified per DAG run via trigger JSON.

**📖 For detailed configuration guide, see**: [`docs/SPARK_DEPLOY_MODE_SETUP.md`](docs/SPARK_DEPLOY_MODE_SETUP.md)

##### Copy JAR to Airflow
```bash
# Copy assembled JAR to Airflow jars directory
copy target\scala-2.12\yelp-batch-project-assembly-0.1.0-SNAPSHOT.jar airflow\jars\
```

##### Trigger DAG from UI
1. Navigate to `http://localhost:8080`
2. Find the `yelp_batch_pipeline` DAG
3. Click "Trigger DAG" and configure with JSON like this one:
```json
{
  "start_date": "2020-01-25",
  "end_date": "2020-01-31",
  "tables": ["business", "review", "tip", "checkin"],
  "skip_bronze": "false"
}
```

##### Trigger DAG from CLI
```bash
# Trigger with date range
docker-compose exec airflow-scheduler airflow dags trigger yelp_batch_pipeline \
  -c '{"start_date":"2020-01-25","end_date":"2020-01-31","tables":["business","review"]}'

# Trigger for single date
docker-compose exec airflow-scheduler airflow dags trigger yelp_batch_pipeline \
  -c '{"run_date":"2020-01-31"}'
```

### Data Flow
```
Raw JSON/MongoDB → Bronze (Delta) → Silver (Delta) → Gold (Delta)
     ↓                  ↓                ↓               ↓
  Ingestion        Cleaning &       Aggregation    Analytics
                   Validation                       Ready
```

### Output Locations
After successful runs, data will be available at:
- **Bronze**: `data/bronze/{table}/_delta_log/`
- **Silver**: `data/silver/{table}_snapshot/day=YYYY-MM-DD/`
- **Gold**: 
  - `data/gold/business_popularity/day=YYYY-MM-DD/granularity=2/`
  - `data/gold/fact_review_tip_metrics/day=YYYY-MM-DD/granularity=0|2/`

### PostgreSQL Integration

The Gold layer can optionally persist data to PostgreSQL for SQL-based analytics and BI tool integration.

#### Quick Start

1. **Initialize the database schema** (automatic on first run):
```powershell
# Start fresh with schema initialization
docker compose down --volumes
docker compose up -d
```

The schema automatically creates:
- `gold` schema with 2 fact tables + 1 reference table
- 3 analytical views + 2 data quality views
- 10 strategic indexes for performance

2. **Enable PostgreSQL writes** in your configuration:

   **For Airflow/Docker** (`src/main/resources/dev.conf`):
   ```hocon
   postgresql {
     enabled = true
     host = "postgres"              # Docker service name
     port = 5432
     database = "yelp_analytics"
   }
   ```

   **For Local Windows** (`src/main/resources/local.conf`):
   ```hocon
   postgresql {
     enabled = true
     host = "localhost"   
     port = 5432 # Local PostgreSQL instance port
     database = "yelp_analytics"
   }
   ```

3. **Run Gold processes with PostgreSQL credentials**:
```powershell
# Local execution
bin\run-local.cmd --process gold_business_popularity --env dev --run_date 2020-01-31 --pg_user airflow --pg_password airflow

# Via Airflow (credentials injected automatically)
# Just trigger the DAG normally
```

#### Verify Data

```powershell
# Connect to PostgreSQL
docker exec -it yelp-batch-project-postgres-1 psql -U airflow -d airflow

# Check data
SELECT COUNT(*) FROM gold.business_popularity;
SELECT * FROM gold.v_top_businesses_by_city LIMIT 10;
```

#### Handling Re-runs (Upsert Strategy)

The pipeline implements a **delete-before-insert** strategy to handle duplicate keys when re-running the same date:

- **Business Popularity**: Deletes all records for the same `day` and `period_month` before inserting
- **Fact Metrics**: Deletes all records for the same `day`, `period_month`, and `granularity` before inserting

This makes the pipeline **idempotent** - you can safely re-run the same date multiple times without errors:

```powershell
# First run - inserts data
bin\run-local.cmd --process gold_business_popularity --env local --run_date 2020-01-31 --pg_user postgres --pg_password postgres

# Second run (same date) - deletes old data, inserts fresh data (no duplicate key error!)
bin\run-local.cmd --process gold_business_popularity --env local --run_date 2020-01-31 --pg_user postgres --pg_password postgres
```

**Logs will show**:
```
[PostgreSQLWriter] Upserting 119698 rows to PostgreSQL table: gold.business_popularity
[PostgreSQLWriter] Delete keys: day, period_month
[PostgreSQLWriter] Deleting existing records from gold.business_popularity
[PostgreSQLWriter] Deleted 119698 existing rows
[PostgreSQLWriter] Inserting 119698 new rows
[PostgreSQLWriter] Successfully upserted 119698 rows
```

📖 **For complete setup guide, queries, and troubleshooting, see [sql/README.md](./sql/README.md)**

### Security Best Practices

#### Environment Variables & Secrets

This project uses environment variables for sensitive configuration:

**Files tracked by git**:
- ✅ `.env.example` - Template with placeholder values
- ✅ `airflow.cfg.template` - Configuration template without secrets
- ✅ `docker-compose.yaml` - References environment variables

**Files NOT tracked by git** (in `.gitignore`):
- 🔒 `.env` - Your actual environment variables
- 🔒 `airflow/config/airflow.cfg` - Airflow config with Fernet key

#### Fernet Key Management

**Recommended: Automated Setup** 🚀
```powershell
# Generate key and create .env automatically
python bin/setup.py

# The script will:
# - Generate a cryptographically secure Fernet key
# - Create .env file with the key
# - Verify setup is correct
```

**Manual Setup** (if preferred):
```powershell
# 1. Generate a new Fernet key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 2. Copy template and update with your key
cp .env.example .env
# Edit .env: AIRFLOW_FERNET_KEY=your-generated-key
```

**Verification**:
```powershell
# After starting Airflow, verify key is loaded
docker exec yelp-batch-project-airflow-scheduler-1 env | Select-String "FERNET"
```

**⚠️ Important**:
- Never commit `.env` or `airflow.cfg` to version control
- Generate a new key for each environment (dev/staging/prod)
- If you change the key, re-create all Airflow connections

**What Fernet Key Protects**:
- Airflow database connection passwords
- API keys stored in Airflow connections
- Sensitive variables in Airflow

**⚠️ Security Warning**: If you change the Fernet key after storing connections, you must re-create all encrypted connections in Airflow UI.

---

### Troubleshooting

#### Common Issues

**1. Java version mismatch**
```bash
# Verify Java 17 is installed
java -version
```

**2. SBT assembly fails**
```bash
# Clean project and rebuild
sbt clean
sbt compile
sbt assembly
```

**3. Airflow containers not starting**
```bash
# Check Docker is running
docker ps

# Restart Airflow
docker-compose down
docker-compose up -d
```

**4. Delta table errors (file not found)**
```bash
# Re-run bronze ingestion for the table
bin\run-local.cmd --process bronze_ingest --env local --tables "checkin"
```

**5. Duplicate data in Gold layer**
```bash
# Caused by multiple business versions - already handled in readData
# Check logs for deduplication: "dropDuplicates(Seq(business_id))"
```

**6. MongoDB connection issues**
```powershell
# Verify MongoDB setup
.\scripts\check-mongo-setup.ps1

# Common fixes:
# - Check container is running: docker ps --filter "name=mongo"
# - Verify data is imported: see docs/MONGODB_SETUP.md
# - Check URI in config matches environment (mongo vs localhost)
```

### Monitoring
- **Airflow UI**: Monitor DAG runs, task status, and logs at `http://localhost:8080`
- **Spark UI**: When running locally, access Spark UI at `http://localhost:4040`
- **Logs**: Check `airflow/logs/` for detailed execution logs

### Stopping Airflow
```bash
# Stop all services
docker-compose down

# Stop and remove volumes (cleans database)
docker-compose down -v
```