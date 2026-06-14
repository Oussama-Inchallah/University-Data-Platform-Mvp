from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.ingestion.crawler.engine import GenericCrawler

# ==========================================
# CONFIGURATION SPÉCIFIQUE À L'UNIVERSITÉ
# ==========================================
UNIVERSITY_ID = "FSJ"
SEED_URL = "https://www.fs.ucd.ac.ma"
ALLOWED_DOMAINS = ["fs.ucd.ac.ma", "www.fs.ucd.ac.ma"]
MAX_DEPTH = 3
CRAWL_DELAY = 1.0

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
    crawl_task = PythonOperator(
        task_id="extract_and_route_to_minio",
        python_callable=run_ingestion_pipeline,
        execution_timeout=timedelta(hours=2)
    )
