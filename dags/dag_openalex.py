"""
DAG d'Ingestion OpenAlex
------------------------
Ce DAG se connecte à l'API OpenAlex pour extraire les publications scientifiques
liées aux universités, et les sauvegarde pour traitement analytique ultérieur.
Le traitement utilise l'approche CDC Batch + Explode pour dédupliquer chaque 
publication au niveau Hudi.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from src.ingestion.openalex.fetcher import OpenAlexFetcher

# ==========================================
# CONFIGURATION CENTRALISÉE OPENALEX
# ==========================================
INSTITUTIONS_TO_CRAWL = [
    "I99297268",  # Université Hassan II de Casablanca
]
CONFIG_PATH = "/app/processing/config/universal_routing.yaml"

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ingestion_openalex_api',
    default_args=default_args,
    description='Pipeline complet (Ingestion + Hudi) pour l\'API OpenAlex',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ingestion', 'processing', 'api', 'openalex'],
) as dag:

    @task
    def fetch_openalex_data(**kwargs):
        if not INSTITUTIONS_TO_CRAWL:
            raise ValueError("La liste INSTITUTIONS_TO_CRAWL est vide.")
            
        execution_date = kwargs.get('ds')
        fetcher = OpenAlexFetcher(execution_date=execution_date)
        
        for inst_id in INSTITUTIONS_TO_CRAWL:
            fetcher.fetch_institution_works(institution_id=inst_id)
            
    ingestion_task = fetch_openalex_data()

    # ---- Task 2 : Phase SILVER (Parsing JSON Explode, Nettoyage, Hudi) ----
    silver_hudi_task = SparkSubmitOperator(
        task_id="parse_and_normalize_to_hudi",
        application="/app/processing/jobs/process_raw_to_hudi.py",
        conn_id="spark_default",
        name="UniversityData_Silver_OpenAlex",
        application_args=[
            "--config", CONFIG_PATH,
            "--execution_date", "{{ ds }}",
            "--source_system", "OpenAlex",
            "--domain_pattern", "openalex/*",
            "--force_target_table", "research_publications"
        ],
        conf={
            "spark.master": "local[1]",
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
        driver_memory="1g",
        verbose=True,
        execution_timeout=timedelta(hours=2),
    )

    # ---- Task 3 : Phase GOLD (Lecture Hudi Incrémental, NLP Chunking) ----
    gold_search_task = SparkSubmitOperator(
        task_id="embed_and_index_to_search",
        application="/app/processing/jobs/process_hudi_to_search.py",
        conn_id="spark_gold",
        name="UniversityData_GoldNLP_OpenAlex",
        application_args=[
            "--config", CONFIG_PATH,
            "--execution_date", "{{ ds }}",
            "--source_system", "OpenAlex",
        ],
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
        driver_memory="1g",
        verbose=True,
        execution_timeout=timedelta(hours=2),
    )

    # Zone Analytique : Hive Metastore Sync
    sync_catalog_task = SparkSubmitOperator(
        task_id='sync_hudi_to_hive_metastore',
        conn_id='spark_gold',
        application='/app/processing/jobs/sync_hudi_catalog.py',
        name="sync_catalog_OpenAlex",
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

    # Orchestration
    ingestion_task >> silver_hudi_task >> gold_search_task >> sync_catalog_task
