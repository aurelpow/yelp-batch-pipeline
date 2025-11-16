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
Load the JSON files into MongoDB collections:

1. **Start MongoDB** (via Docker or local installation):
   ```bash
   docker run -d -p 27017:27017 --name mongodb mongo:7
   ```

2. **Import data** using `mongoimport`:
   ```bash
   mongoimport --db yelpAcademicDatasets --collection business --file yelp_academic_dataset_business.json
   mongoimport --db yelpAcademicDatasets --collection checkin --file yelp_academic_dataset_checkin.json
   mongoimport --db yelpAcademicDatasets --collection review --file yelp_academic_dataset_review.json
   mongoimport --db yelpAcademicDatasets --collection tip --file yelp_academic_dataset_tip.json
   mongoimport --db yelpAcademicDatasets --collection user --file yelp_academic_dataset_user.json
   ```

3. **Enable MongoDB in configuration** (`src/main/resources/application.conf`):
   ```hocon
   mongodb {
     enabled = true
     uri = "mongodb://localhost:27017"
     database = "yelpAcademicDatasets"
   }
   ```

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
- `local.conf` - for local development
- `dev.conf` - for development environment
- `prod.conf` - for production environment

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

##### Copy JAR to Airflow
```bash
# Copy assembled JAR to Airflow jars directory
copy target\scala-2.12\yelp-batch-project-assembly-0.1.0-SNAPSHOT.jar airflow\jars\
```

##### Trigger DAG from UI
1. Navigate to `http://localhost:8080`
2. Find the `yelp_batch_pipeline` DAG
3. Click "Trigger DAG" and configure with JSON:
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