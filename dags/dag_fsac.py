# ==============================================================================
# © 2026 University Data Platform
# Ce fichier fait partie du système central de la Plateforme Data Universitaire.
# ==============================================================================

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
UNIVERSITY_ID = "FSAC"
SEED_URL = "https://fsac.univh2c.ma"
ALLOWED_DOMAINS = ["fsac.univh2c.ma"]
MAX_DEPTH = 3
CRAWL_DELAY = 1.0

# Spark processing parameters
DOMAIN_PATTERN = "*fsac.univh2c.ma*"
CONFIG_PATH = "/app/processing/config/universal_routing.yaml"

from utils.alerts import global_default_args as default_args

def run_ingestion_pipeline(**kwargs):
    execution_date = kwargs.get('ds')
    crawler = GenericCrawler(
        seed_url=SEED_URL,
        allowed_domains=ALLOWED_DOMAINS,
        max_depth=MAX_DEPTH,
        university_id=UNIVERSITY_ID,
        crawl_delay=CRAWL_DELAY,
        execution_date=execution_date
    )
    crawler.start_crawl()

with DAG(
    dag_id=f"e2e_lakehouse_{UNIVERSITY_ID.lower()}",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["e2e", "lakehouse", "academic", UNIVERSITY_ID],
    description=f"Pipeline Lakehouse End-to-End pour {UNIVERSITY_ID}",
) as dag:

    # ---- Task 1 : Ingestion (Crawl web → MinIO raw zone) ----
    ingestion_task = PythonOperator(
        task_id="extract_html_to_bronze_datalake",
        python_callable=run_ingestion_pipeline,
        execution_timeout=timedelta(hours=2),
    )
    # ---- Task 2 : Phase SILVER (Parsing Tika, Nettoyage, Hudi & ElasticSearch) ----
    silver_hudi_task = SparkSubmitOperator(
        task_id="spark_transform_to_silver_hudi",
        application="/app/processing/jobs/process_raw_to_hudi.py",
        conn_id="spark_default",
        name=f"UniversityData_Silver_{UNIVERSITY_ID}",
        application_args=[
            "--config", CONFIG_PATH,
            "--execution_date", "{{ ds }}",
            "--source_system", UNIVERSITY_ID,
            "--domain_pattern", DOMAIN_PATTERN,
        ],
        conf={
            "spark.master": "local[16]",
            "spark.sql.shuffle.partitions": "16",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
            "hive.metastore.uris": "thrift://hive-metastore:9083",
        },
        driver_memory="4g",
        verbose=True,
        execution_timeout=timedelta(hours=2),
    )

    # ---- Task 3 : Phase GOLD (Lecture Hudi Incrémental, NLP Chunking, Qdrant) ----
    gold_search_task = SparkSubmitOperator(
        task_id="nlp_chunking_and_gold_indexing",
        application="/app/processing/jobs/process_hudi_to_search.py",
        conn_id="spark_gold",
        name=f"UniversityData_GoldNLP_{UNIVERSITY_ID}",
        application_args=[
            "--config", CONFIG_PATH,
            "--execution_date", "{{ ds }}",
            "--source_system", UNIVERSITY_ID,
        ],
        conf={
            "spark.master": "local[16]",
            "spark.sql.shuffle.partitions": "16",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
            "hive.metastore.uris": "thrift://hive-metastore:9083",
        },
        driver_memory="4g",
        verbose=True,
        execution_timeout=timedelta(hours=2),
    )

    # Zone Analytique : Hive Metastore Sync
    sync_catalog_task = SparkSubmitOperator(
        task_id='sync_hive_catalog_for_bi',
        conn_id='spark_gold',
        application='/app/processing/jobs/sync_hudi_catalog.py',
        name=f"sync_catalog_{UNIVERSITY_ID}",
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
            "hive.metastore.uris": "thrift://hive-metastore:9083",
        },
        driver_memory="512m",
        verbose=True,
    )

    # ---- Pipeline Medallion : Ingestion -> Silver -> Gold -> Analytics ----
    ingestion_task >> silver_hudi_task >> gold_search_task >> sync_catalog_task
