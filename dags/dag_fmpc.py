"""
DAG d'Ingestion et Traitement pour cette Université
---------------------------------------------------
Ce DAG Airflow orchestre deux tâches principales :
1. extract_and_route_to_minio : Crawl du site web (HTML, PDF, etc.) et sauvegarde brute dans MinIO.
2. process_spark : Job Spark local qui lit les données brutes, les nettoie, et les sauvegarde au format Hudi.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from src.ingestion.crawler.engine import GenericCrawler

# ==========================================
# CONFIGURATION SPÉCIFIQUE À L'UNIVERSITÉ
# ==========================================
UNIVERSITY_ID = "FMPC"
SEED_URL = "https://www.fmpc.ac.ma/"
ALLOWED_DOMAINS = ["fmpc.ac.ma"]
MAX_DEPTH = 3
CRAWL_DELAY = 1.0

# Spark processing parameters
DOMAIN_PATTERN = "*fmpc.ac.ma*"
CONFIG_PATH = "/app/processing/config/universal_routing.yaml"


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
    tags=["ingestion", "processing", "raw", UNIVERSITY_ID],
    description=f"Pipeline complet (ingestion + processing Spark) pour {UNIVERSITY_ID}",
) as dag:

    # ---- Task 1 : Ingestion (Crawl web → MinIO raw zone) ----
    crawl_task = PythonOperator(
        task_id="extract_and_route_to_minio",
        python_callable=run_ingestion_pipeline,
        execution_timeout=timedelta(hours=2)
    )

    # ---- Task 2 : Processing Spark (raw zone → curated zone via Hudi) ----
    #   Uses local[*] mode — all processing runs in the driver (Airflow worker).
    #   This matches the original spark-submit command and avoids executor OOM
    #   issues on resource-constrained WSL2/Docker environments.
    process_spark_task = SparkSubmitOperator(
        task_id="process_spark",
        application="/app/processing/jobs/main.py",
        conn_id="spark_default",
        name=f"UniversityData_Process_{UNIVERSITY_ID}",
        application_args=[
            "--config", CONFIG_PATH,
            "--execution_date", "{{ ds }}",
            "--source_system", UNIVERSITY_ID,
            "--domain_pattern", DOMAIN_PATTERN,
        ],
        conf={
            # --- Local mode (matches original spark-submit) ---
            "spark.master": "local[*]",
            # --- S3A / MinIO ---
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            # --- Serialization ---
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            # --- Hudi ---
            "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        },
        driver_memory="4g",
        verbose=True,
        execution_timeout=timedelta(hours=3),
    )

    # ---- Pipeline : ingestion → processing ----
    crawl_task >> process_spark_task
