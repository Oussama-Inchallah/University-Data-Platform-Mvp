import os
import json
import logging
from datetime import datetime
import requests

logger = logging.getLogger(__name__)

class PlatformLogger:
    """
    Utilitaire pour envoyer les statuts et logs d'exécution des pipelines ETL
    vers Elasticsearch pour l'observabilité DataOps.
    """
    
    def __init__(self, es_url="http://elasticsearch:9200", index_name="platform_logs"):
        self.es_url = es_url
        self.index_name = index_name
        self._ensure_index_exists()

    def _ensure_index_exists(self):
        """Vérifie et crée l'index platform_logs s'il n'existe pas."""
        try:
            res = requests.head(f"{self.es_url}/{self.index_name}", timeout=5)
            if res.status_code == 404:
                mapping = {
                    "mappings": {
                        "properties": {
                            "timestamp": {"type": "date"},
                            "status": {"type": "keyword"},
                            "job_name": {"type": "keyword"},
                            "error_message": {"type": "text"}
                        }
                    }
                }
                requests.put(f"{self.es_url}/{self.index_name}", json=mapping, timeout=5)
                logger.info(f"Created Elasticsearch observability index: {self.index_name}")
        except Exception as e:
            logger.warning(f"Could not connect to Elasticsearch for observability initialization: {e}")

    def log_job_status(self, job_name: str, status: str, error_message: str = None):
        """
        Enregistre le statut d'une tâche d'ingestion ou d'ETL.
        status: 'SUCCESS', 'FAILED', 'RUNNING'
        """
        try:
            log_doc = {
                "timestamp": datetime.utcnow().isoformat(),
                "status": status,
                "job_name": job_name,
                "error_message": error_message
            }
            headers = {'Content-Type': 'application/json'}
            res = requests.post(
                f"{self.es_url}/{self.index_name}/_doc", 
                headers=headers, 
                data=json.dumps(log_doc),
                timeout=5
            )
            res.raise_for_status()
            logger.info(f"Successfully logged {job_name} ({status}) to platform_logs")
        except Exception as e:
            logger.warning(f"Failed to log observability metrics to Elasticsearch: {e}")

