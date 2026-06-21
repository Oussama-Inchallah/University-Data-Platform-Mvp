"""
Qdrant Writer (Base de Données Vectorielle)
-------------------------------------------
Gère l'écriture des segments (chunks) vectorisés vers Qdrant via le connecteur natif Spark.
Utile pour la recherche sémantique (RAG).
"""
import os
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

class QdrantWriter:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Qdrant URL (par défaut le conteneur docker local)
        self.qdrant_url = os.environ.get("QDRANT_URL", "http://qdrant:6333")

    def write_collection(self, df: DataFrame, collection_name: str = "university_chunks"):
        """
        Écrit un DataFrame contenant une colonne 'embedding' vers Qdrant.
        """
        self.logger.info(f"Writing vectorized chunks to Qdrant collection: {collection_name}")
        
        try:
            # Qdrant nécessite un champ 'id' valide (entier ou UUID).
            # On génère un UUID de manière déterministe ou aléatoire.
            # Le Spark Qdrant connector gère automatiquement la conversion si on spécifie le bon champ.
            
            # S'assurer que la collection existe (le connecteur Spark ne la crée pas automatiquement)
            try:
                from qdrant_client import QdrantClient
                from qdrant_client.http.models import Distance, VectorParams
                
                client = QdrantClient(url=self.qdrant_url)
                if not client.collection_exists(collection_name):
                    self.logger.info(f"Création de la collection Qdrant '{collection_name}' (768 dimensions)...")
                    client.create_collection(
                        collection_name=collection_name,
                        vectors_config=VectorParams(size=768, distance=Distance.COSINE)
                    )
            except Exception as e:
                self.logger.warning(f"Impossible de vérifier ou créer la collection Qdrant avec qdrant_client: {e}")
            
            # S'assurer que le DataFrame n'est pas vide
            if df.isEmpty():
                self.logger.warning("Empty dataframe, skipping Qdrant write.")
                return
                
            # Écriture via le connecteur natif Spark-Qdrant
            (df.write
               .format("io.qdrant.spark.Qdrant")
               .option("qdrant_url", self.qdrant_url)
               .option("collection_name", collection_name)
               .option("embedding_field", "embedding")
               .option("schema", df.schema.json())
               # Configuration optionnelle pour la création automatique de la collection
               .option("vector_name", "") # Vector anonyme
               .option("vector_size", "768") # Taille pour text-embedding-004 (Gemini API)
               .option("distance", "Cosine")
               .mode("append")
               .save())
               
            self.logger.info(f"Successfully wrote data to Qdrant collection '{collection_name}'")
            
        except Exception as e:
            self.logger.error(f"Failed to write to Qdrant: {e}")
            # On ne fait pas crasher tout le job Spark si Qdrant échoue pendant le MVP
            self.logger.warning("Continuing Spark job despite Qdrant failure.")
