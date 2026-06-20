# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.python import PythonOperator
# pyrefly: ignore [missing-import]
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from src.ingestion.crawler.engine import GenericCrawler

# ==========================================
# CONFIGURATION SPÉCIFIQUE À L'UNIVERSITÉ
# ==========================================
UNIVERSITY_ID = "FSAC"
SEED_URL = "https://fsac.univh2c.ma"
ALLOWED_DOMAINS = ["fsac.univh2c.ma"]
MAX_DEPTH = 3
CRAWL_DELAY = 1.0


# Construction automatique du pattern de domaine pour MinIO
DOMAIN_PATTERN = f"*{ALLOWED_DOMAINS[0]}*"

default_args = {
    'owner': 'data_platform_team',
    'start_date': datetime(2023, 10, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def run_ingestion_pipeline():
    crawler = GenericCrawler(
        seed_url=SEED_URL,
        allowed_domains=ALLOWED_DOMAINS,
        max_depth=MAX_DEPTH,
        university_id=UNIVERSITY_ID,
        crawl_delay=CRAWL_DELAY
    )
    crawler.start_crawl()

with DAG(
    dag_id=f"ingest_raw_{UNIVERSITY_ID.lower()}",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["ingestion", "raw", UNIVERSITY_ID]
) as dag:

    # Task 1: Crawler Ingestion
    crawl_task = PythonOperator(
        task_id="extract_and_route_to_minio",
        python_callable=run_ingestion_pipeline,
        execution_timeout=timedelta(hours=2)
    )

    # Task 2: Spark Data Processing via Docker Exec
    # Uses {{ ds }} to pass the logical execution date (YYYY-MM-DD) for incremental loads
    spark_submit_command = f"""
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master local[*] \
        --driver-memory 4g \
        --executor-memory 4g \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
        /app/processing/jobs/main.py \
        --config /app/processing/config/universal_routing.yaml \
        --execution_date {{{{ ds }}}} \
        --source_system {UNIVERSITY_ID} \
        --domain_pattern '{DOMAIN_PATTERN}'
    """

    spark_process_task = BashOperator(
        task_id="process_and_route_spark",
        bash_command=spark_submit_command,
        execution_timeout=timedelta(hours=4)
    )

    # Define Execution Order
    crawl_task >> spark_process_task