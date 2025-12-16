from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.models import Variable

# Constants
JAR_PATH = "/opt/airflow/jars/yelp-batch-project-assembly-0.1.0-SNAPSHOT.jar"
CLASS = "com.yelpbatch.app.Runner"

# Spark Infrastructure Configuration (where Spark driver runs)
# This is for Airflow/Spark interaction, not passed to the application
DEPLOY_MODE = Variable.get("spark_deploy_mode", default_var="client")  # client | cluster

# Application Configuration (which config file Runner loads: local.conf, dev.conf, prod.conf)
# Default value for when not specified in trigger
DEFAULT_ENV = "dev"  # local | dev | prod

@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["yelp", "spark", "delta"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
)
def yelp_batch_pipeline():
    bronze = SparkSubmitOperator(
        task_id="bronze",
        application=JAR_PATH,
        java_class=CLASS,
        deploy_mode=DEPLOY_MODE,
        application_args=["--process","bronze_ingest",
                          "--env","{{ dag_run.conf.get('env', '" + DEFAULT_ENV + "') }}",
                          "--run_date","{{ dag_run.conf.get('run_date', ds) }}",
                          "--tables", "{{ dag_run.conf.get('tables', 'business,review,user,checkin,tip') }}",
                          "--skip_bronze", "{{ dag_run.conf.get('skip_bronze', 'false') }}"],
    )

    silver = SparkSubmitOperator(
        task_id="silver",
        application=JAR_PATH,
        java_class=CLASS,
        deploy_mode=DEPLOY_MODE,
        application_args=["--process","silver_ingest",
                          "--env","{{ dag_run.conf.get('env', '" + DEFAULT_ENV + "') }}",
                          "--start_date", "{{ dag_run.conf.get('start_date', dag_run.conf.get('run_date', ds)) }}",
                          "--end_date",   "{{ dag_run.conf.get('end_date',   dag_run.conf.get('run_date', ds)) }}",
                          "--tables", "{{ dag_run.conf.get('tables', 'business,review,user,checkin,tip') }}",
                          "--full_load", "{{ dag_run.conf.get('full_load', 'false') }}"],
    )

    # Dummy task to group gold fact tasks
    gold = EmptyOperator(task_id="gold")

    gold_fact_review_tip = SparkSubmitOperator(
        task_id="gold_fact_review_tip",
        application=JAR_PATH,
        java_class=CLASS,
        deploy_mode=DEPLOY_MODE,
        jars="/opt/airflow/jars/postgresql-42.7.8.jar",  # Add PostgreSQL driver
        driver_class_path="/opt/airflow/jars/postgresql-42.7.8.jar",  # Add to driver classpath
        application_args=["--process","gold_fact_review_tip",
                          "--env","{{ dag_run.conf.get('env', '" + DEFAULT_ENV + "') }}",
                          "--start_date", "{{ dag_run.conf.get('start_date', dag_run.conf.get('run_date', ds)) }}",
                          "--end_date",   "{{ dag_run.conf.get('end_date',   dag_run.conf.get('run_date', ds)) }}",
                          "--tables", "{{ dag_run.conf.get('tables', 'business,review,user,checkin,tip') }}",
                          "--force_monthly", "{{ dag_run.conf.get('force_monthly', '') }}",
                          "--skip_daily", "{{ dag_run.conf.get('skip_daily', 'false') }}",
                          "--dry_run", "{{ dag_run.conf.get('dry_run', 'false') }}",
                          "--pg_user", "{{dag_run.conf.get('pg_user', 'airflow')}}",
                          "--pg_password", "{{dag_run.conf.get('pg_password', 'airflow')}}"],
    )

    gold_business_popularity = SparkSubmitOperator(
        task_id="gold_business_popularity",
        application=JAR_PATH,
        java_class=CLASS,
        deploy_mode=DEPLOY_MODE,
        jars= "/opt/airflow/jars/postgresql-42.7.8.jar",  # Add PostgreSQL driver
        driver_class_path="/opt/airflow/jars/postgresql-42.7.8.jar",  # Add to driver classpath
        application_args=["--process","gold_business_popularity",
                            "--env","{{ dag_run.conf.get('env', '" + DEFAULT_ENV + "') }}",
                            "--start_date", "{{ dag_run.conf.get('start_date', dag_run.conf.get('run_date', ds)) }}",
                            "--end_date",   "{{ dag_run.conf.get('end_date',   dag_run.conf.get('run_date', ds)) }}",
                            "--pg_user", "{{dag_run.conf.get('pg_user', 'airflow')}}",
                            "--pg_password", "{{dag_run.conf.get('pg_password', 'airflow')}}"
                          ],
    )

    bronze >> silver >> gold >> [gold_fact_review_tip, gold_business_popularity]

yelp_batch_pipeline()