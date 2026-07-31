# ==============================================================================
# © 2026 University Data Platform
# Ce fichier fait partie du système central de la Plateforme Data Universitaire.
# ==============================================================================

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
# Mapping entre l'identifiant OpenAlex et l'institution_name métier
INSTITUTIONS_TO_CRAWL = {
    "I99297268": "UH2C",  # Université Hassan II de Casablanca
}
CONFIG_PATH = "/app/processing/config/universal_routing.yaml"

from utils.alerts import global_default_args as default_args

with DAG(
    'e2e_lakehouse_openalex',
    default_args=default_args,
    description='Pipeline Lakehouse End-to-End pour l\'API OpenAlex',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['e2e', 'lakehouse', 'research', 'openalex'],
) as dag:

    @task(task_id="extract_api_to_bronze_datalake")
    def fetch_openalex_data(**kwargs):
        if not INSTITUTIONS_TO_CRAWL:
            raise ValueError("Le dictionnaire INSTITUTIONS_TO_CRAWL est vide.")
            
        execution_date = kwargs.get('ds')
        fetcher = OpenAlexFetcher(execution_date=execution_date)
        
        for inst_id, inst_name in INSTITUTIONS_TO_CRAWL.items():
            fetcher.fetch_institution_works(institution_id=inst_id, institution_name=inst_name)
            
    ingestion_task = fetch_openalex_data()

    # On récupère tous les noms d'institutions pour les passer au script Spark
    all_inst_names = ",".join(INSTITUTIONS_TO_CRAWL.values())

    # ---- Task 2 : Phase SILVER (Parsing JSON Explode, Nettoyage, Hudi) ----
    # Une seule tâche pour traiter TOUTES les institutions téléchargées.
    silver_hudi_task = SparkSubmitOperator(
        task_id="spark_transform_to_silver_hudi",
        application="/app/processing/jobs/process_raw_to_hudi.py",
        conn_id="spark_default",
        name="UniversityData_Silver_OpenAlex",
        application_args=[
            "--config", CONFIG_PATH,
            "--execution_date", "{{ ds }}",
            "--source_system", all_inst_names,  # Liste des universités
            "--domain_pattern", "openalex/*",   # On lit tout le dossier openalex
            "--force_target_table", "research_publications",
            "--api_name", "OpenAlexAPI"
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

    # ---- Task 3 : Phase GOLD (Lecture Hudi Incrémental, NLP Chunking) ----
    gold_search_task = SparkSubmitOperator(
        task_id="nlp_chunking_and_gold_indexing",
        application="/app/processing/jobs/process_hudi_to_search.py",
        conn_id="spark_gold",
        name="UniversityData_GoldNLP_OpenAlex",
        application_args=[
            "--config", CONFIG_PATH,
            "--execution_date", "{{ ds }}",
            "--source_system", all_inst_names,
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
        driver_memory="4g",
        verbose=True,
        execution_timeout=timedelta(hours=2),
    )

    # Zone Analytique : Hive Metastore Sync
    sync_catalog_task = SparkSubmitOperator(
        task_id='sync_hive_catalog_for_bi',
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

    # Orchestration Linéaire
    ingestion_task >> silver_hudi_task >> gold_search_task >> sync_catalog_task
