# ==============================================================================
# © 2026 University Data Platform
# Ce fichier fait partie du système central de la Plateforme Data Universitaire.
# ==============================================================================

"""
Observabilité DataOps (Platform Logger)
---------------------------------------
Client de journalisation centralisé. Envoie les métriques de santé des pipelines
ETL (Succès, Échecs, Durée) vers l'index Elasticsearch 'platform_logs'.
Permet d'alimenter les tableaux de bord de supervision de l'usine logicielle.
"""

import os
import json
import logging
from datetime import datetime
import requests

logger = logging.getLogger(__name__)

class PlatformLogger:
    """
    Système de monitoring actif pour les pipelines de données.
    """
    
    def __init__(self, es_url="http://elasticsearch:9200", index_name="platform_logs"):
        """
        Initialise le logger de plateforme.
        Assure la création du schéma strict (mapping) de l'index de monitoring.
        """
        self.es_url = es_url
        self.index_name = index_name
        self._ensure_index_exists()

    def _ensure_index_exists(self):
        """Provisionne l'index de monitoring DataOps de manière idempotent."""
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
                logger.info(f"Index de monitoring DataOps provisionné : {self.index_name}")
        except Exception as e:
            logger.warning(f"Elasticsearch indisponible pour l'initialisation du monitoring : {e}")

    def log_job_status(self, job_name: str, status: str, error_message: str = None):
        """
        Exporte un événement de santé ETL vers le puits de logs centralisé.
        
        Args:
            job_name (str): Nom de la tâche (ex: 'bronze_to_silver').
            status (str): État métier ('SUCCESS', 'FAILED', 'RUNNING').
            error_message (str, optional): Stacktrace ou message d'erreur si FAILED.
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
            logger.info(f"Observabilité mise à jour pour le job {job_name} ({status})")
        except Exception as e:
            logger.warning(f"Échec de l'export des métriques DataOps : {e}")
