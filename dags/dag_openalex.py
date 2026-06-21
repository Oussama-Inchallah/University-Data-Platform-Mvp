"""
DAG d'Ingestion OpenAlex
------------------------
Ce DAG se connecte à l'API OpenAlex pour extraire les publications scientifiques
liées aux universités, et les sauvegarde pour traitement analytique ultérieur.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from src.ingestion.openalex.fetcher import OpenAlexFetcher

# ==========================================
# CONFIGURATION CENTRALISÉE OPENALEX
# ==========================================
# Ajoutez ici les identifiants OpenAlex des universités et facultés à extraire.
# Vous pouvez trouver ces identifiants sur le site openalex.org
INSTITUTIONS_TO_CRAWL = [
    "I99297268",  # Université Hassan II de Casablanca
    # "I12345678", # Décommentez et ajoutez d'autres facultés ici
]

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
    description='Ingestion des publications scientifiques via l\'API OpenAlex',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ingestion', 'api', 'openalex'],
) as dag:

    @task
    def fetch_openalex_data(**kwargs):
        if not INSTITUTIONS_TO_CRAWL:
            raise ValueError("La liste INSTITUTIONS_TO_CRAWL est vide. Veuillez ajouter au moins un identifiant.")
            
        execution_date = kwargs.get('ds')
        fetcher = OpenAlexFetcher(execution_date=execution_date)
        
        for inst_id in INSTITUTIONS_TO_CRAWL:
            fetcher.fetch_institution_works(institution_id=inst_id)
            
    fetch_openalex_data()
