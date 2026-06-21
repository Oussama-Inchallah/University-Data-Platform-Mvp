"""
Connecteur Elasticsearch (Recherche)
------------------------------------
Ce module permet d'indexer les données transformées du DataFrame Spark
vers Elasticsearch pour le moteur de recherche global (actuellement désactivé dans le MVP).
"""
from pyspark.sql import DataFrame

class ElasticsearchWriter:
    """
    Gestionnaire d'écriture vers le moteur de recherche Elasticsearch.
    """
    def __init__(self, es_nodes: str = "elasticsearch", es_port: str = "9200"):
        self.es_nodes = es_nodes
        self.es_port = es_port

    def write_index(self, df: DataFrame, index_name: str = "university_content"):
        """
        Publie le DataFrame nettoyé vers Elasticsearch.
        Ne sélectionne que les champs pertinents pour le moteur de recherche.
        """
        es_options = {
            "es.nodes": self.es_nodes,
            "es.port": self.es_port,
            "es.resource": index_name,
            "es.mapping.id": "record_id", # La clé unique pour écraser (upsert) dans ES
            "es.write.operation": "upsert",
            "es.nodes.wan.only": "true" # Crucial pour éviter des erreurs réseau dans Docker
        }
        
        # Projection : on ne garde que ce qui est utile à la recherche
        # 'target_table' (ex: faculty_profiles) devient 'entity_type' dans ES
        df_es = df.select(
            "record_id", 
            "normalized_text", 
            "language", 
            "source_system", 
            "target_table", 
            "crawl_timestamp"
        ).withColumnRenamed("target_table", "entity_type")
        
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Writing {df_es.count()} documents to Elasticsearch index: {index_name}")
        
        try:
            if df_es.isEmpty():
                logger.warning("Empty dataframe, skipping Elasticsearch write.")
                return
                
            df_es.write.format("org.elasticsearch.spark.sql") \
                .options(**es_options) \
                .mode("append") \
                .save()
            logger.info("Successfully wrote data to Elasticsearch.")
        except Exception as e:
            logger.error(f"Failed to write to Elasticsearch: {e}")
            logger.warning("Continuing Spark job despite ES failure.")
