# ==============================================================================
# © 2026 University Data Platform
# Ce fichier fait partie du système central de la Plateforme Data Universitaire.
# ==============================================================================

"""
Connecteur Elasticsearch (Moteur de Recherche Documentaire)
-----------------------------------------------------------
Gère la synchronisation des données (Lakehouse -> Elasticsearch) pour
alimenter le moteur de recherche plein texte (BM25).
"""

from pyspark.sql import DataFrame
import logging

class ElasticsearchWriter:
    """
    Client d'écriture distribuée vers Elasticsearch (via elasticsearch-spark).
    
    Architecture (DataOps) :
    - Tolérance aux pannes : Les échecs réseau ES ne font pas planter le job Spark (Fall-forward).
    - Upsert mode : Écrase les anciens documents basés sur le `record_id`.
    """
    def __init__(self, es_nodes: str = "elasticsearch", es_port: str = "9200"):
        """
        Initialise la connexion au cluster Elasticsearch.
        """
        self.es_nodes = es_nodes
        self.es_port = es_port
        self.logger = logging.getLogger(__name__)

    def write_index(self, df: DataFrame, index_name: str = "university_content"):
        """
        Indexe le DataFrame Spark vers Elasticsearch.
        Effectue une projection (SELECT) pour ne conserver que les champs nécessaires à la recherche.
        
        Args:
            df (DataFrame): Le DataFrame Hudi source.
            index_name (str): Nom de l'index ES de destination.
        """
        es_options = {
            "es.nodes": self.es_nodes,
            "es.port": self.es_port,
            "es.resource": index_name,
            "es.mapping.id": "record_id",
            "es.write.operation": "upsert",
            "es.nodes.wan.only": "true"
        }
        
        df_es = df.select(
            "record_id", 
            "normalized_text", 
            "language", 
            "source_system", 
            "target_table", 
            "crawl_timestamp"
        ).withColumnRenamed("target_table", "entity_type")
        
        self.logger.info(f"Écriture de {df_es.count()} documents dans l'index ES: {index_name}")
        
        try:
            if df_es.isEmpty():
                self.logger.warning("Le DataFrame est vide, annulation de l'écriture ES.")
                return
                
            df_es.write.format("org.elasticsearch.spark.sql") \
                .options(**es_options) \
                .mode("append") \
                .save()
            self.logger.info("Indexation Elasticsearch terminée avec succès.")
        except Exception as e:
            self.logger.error(f"Échec de l'écriture vers Elasticsearch : {e}")
            self.logger.warning("Le job Spark continue malgré l'erreur ES (Soft failure).")
