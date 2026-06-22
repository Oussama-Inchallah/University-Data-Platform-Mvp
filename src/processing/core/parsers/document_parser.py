"""
Parseur de Documents (PDF, Word) - Metadata Driven
--------------------------------------------------
Ce module utilise une UDF Spark pour extraire le texte brut et les métadonnées
depuis MinIO via Tika (tika-python).
Conçu pour éviter les erreurs OOM : Spark ne gère que les chemins (bucket/path),
et l'UDF streame le binaire directement depuis MinIO vers Tika.
"""
import logging
from typing import Dict
import os
import urllib3

# Suppress insecure request warnings for MinIO
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

def extract_document_from_minio(bucket: str, path: str) -> Dict[str, str]:
    """
    Télécharge un document depuis MinIO et extrait son texte via Tika.
    
    Args:
        bucket: Le nom du bucket MinIO (ex: 'raw-documents')
        path: Le chemin S3/MinIO du fichier
        
    Returns:
        Dict contenant 'text' (texte normalisé) et 'title'.
    """
    if not bucket or not path:
        return {"text": "", "title": ""}
        
    try:
        from minio import Minio
        
        # Configuration MinIO
        endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
        access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
        
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        
        # Streamer le fichier depuis MinIO
        response = client.get_object(bucket, path)
        binary_data = response.read()
        response.close()
        response.release_conn()
        
        # FORCE Tika to act as a client pointing to the dedicated Docker container
        os.environ['TIKA_CLIENT_ONLY'] = 'True'
        os.environ['TIKA_SERVER_ENDPOINT'] = 'http://tika:9998'
        
        from tika import parser
        parsed = parser.from_buffer(binary_data)
        
        # Libération immédiate de la mémoire binaire
        del binary_data
        
        content = parsed.get("content", "")
        metadata = parsed.get("metadata", {})
        
        if content is None:
            content = ""
            
        # Nettoyage très basique du texte, et limitation pour éviter les pics de RAM sur des PDF géants
        text = " ".join(content.split())
        
        # Limite à ~200,000 caractères (approx 50-100 pages de texte brut) pour éviter les OOM lors de l'enregistrement Hudi
        if len(text) > 200000:
            text = text[:200000]
            
        title = metadata.get("title", "")
        if isinstance(title, list):
            title = title[0] if title else ""
        if not title:
            title = metadata.get("dc:title", "")
            if isinstance(title, list):
                title = title[0] if title else ""
        
        return {"text": text, "title": str(title)}
        
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction de {bucket}/{path}: {str(e)}")
        return {"text": "", "title": ""}

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf

# Définition du schéma de retour de la UDF
document_schema = StructType([
    StructField("text", StringType(), True),
    StructField("title", StringType(), True)
])

# Création de la UDF PySpark
parse_document_udf = udf(extract_document_from_minio, document_schema)
