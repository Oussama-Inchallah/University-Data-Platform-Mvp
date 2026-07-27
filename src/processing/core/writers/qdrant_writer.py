# ==============================================================================
# © 2026 University Data Platform
# Ce fichier fait partie du système central de la Plateforme Data Universitaire.
# ==============================================================================

"""
Connecteur Qdrant (Base de Données Vectorielle RAG)
---------------------------------------------------
Gère l'écriture distribuée (gRPC via Spark) des chunks vectorisés (Embeddings)
vers le cluster Qdrant pour la recherche sémantique (RAG).
"""

import os
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

class QdrantWriter:
    """
    Client d'écriture vers Qdrant.
    
    Architecture (DataOps) :
    - Provisionnement Automatique : Crée la collection via l'API REST si elle n'existe pas.
    - Écriture Haute Performance : Utilise le port gRPC (6334) via le connecteur natif Spark-Qdrant.
    """
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.qdrant_url = os.environ.get("QDRANT_URL", "http://qdrant:6333")

    def write_collection(self, df: DataFrame, collection_name: str = "university_chunks"):
        """
        Persiste les vecteurs sémantiques.
        
        Args:
            df (DataFrame): DataFrame contenant au minimum 'id' (UUID string) et 'embedding' (Array de floats).
            collection_name (str): Nom de la collection vectorielle cible.
        """
        self.logger.info(f"Écriture des chunks vectorisés vers Qdrant (collection: {collection_name})")
        
        try:
            try:
                from qdrant_client import QdrantClient
                from qdrant_client.http.models import Distance, VectorParams
                
                client = QdrantClient(url=self.qdrant_url)
                
                collections_response = client.get_collections()
                existing_collections = [c.name for c in collections_response.collections]
                
                if collection_name not in existing_collections:
                    self.logger.info(f"Création de la collection Qdrant '{collection_name}' (384 dims, Cosine)...")
                    client.create_collection(
                        collection_name=collection_name,
                        vectors_config=VectorParams(size=384, distance=Distance.COSINE)
                    )
            except Exception as e:
                self.logger.warning(f"Alerte provisionnement Qdrant (REST API) : {e}")
            
            if df.isEmpty():
                self.logger.warning("Le DataFrame est vide, annulation de l'écriture Qdrant.")
                return
                
            spark_qdrant_url = self.qdrant_url.replace("6333", "6334")
            
            (df.write
               .format("io.qdrant.spark.Qdrant")
               .option("qdrant_url", spark_qdrant_url)
               .option("collection_name", collection_name)
               .option("embedding_field", "embedding")
               .option("id_field", "id")
               .option("schema", df.schema.json())
               .option("vector_size", "384")
               .option("distance", "Cosine")
               .mode("append")
               .save())
               
            self.logger.info(f"Insertion vectorielle Qdrant réussie ('{collection_name}')")
            
        except Exception as e:
            self.logger.error(f"Échec de l'écriture vers Qdrant : {e}")
            self.logger.warning("Le job Spark continue malgré l'erreur Qdrant (Soft failure).")
