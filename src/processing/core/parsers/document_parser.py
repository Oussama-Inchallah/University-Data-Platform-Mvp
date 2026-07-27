# ==============================================================================
# © 2026 University Data Platform
# Ce fichier fait partie du système central de la Plateforme Data Universitaire.
# ==============================================================================

"""
Parseur de Documents (Tika / OCR)
---------------------------------
Module de traitement pour les fichiers binaires (PDF, Word, PPT).
Utilise Apache Tika via des requêtes REST pour éviter l'instanciation de JVMs
lourdes dans les workers Spark.
"""

import logging
import os
import urllib3
from typing import Dict
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

def extract_document_from_minio(bucket: str, path: str) -> Dict[str, str]:
    """
    Extrait le texte d'un document binaire via le serveur Tika.
    
    Architecture (DataOps) :
    - Le fichier est streamé directement vers Tika pour éviter les Out-Of-Memory (OOM).
    - OCR lourd volontairement désactivé pour respecter les SLAs de traitement.
    
    Args:
        bucket (str): Bucket source.
        path (str): Chemin du document.
        
    Returns:
        Dict[str, str]: Titre extrait des métadonnées et texte brut normalisé.
    """
    if not bucket or not path:
        return {"text": "", "title": ""}
        
    try:
        global minio_client
        if 'minio_client' not in globals():
            from minio import Minio
            endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
            access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
            secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
            minio_client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
            
        response = minio_client.get_object(bucket, path)
        binary_data = response.read()
        response.close()
        response.release_conn()
        
        os.environ['TIKA_CLIENT_ONLY'] = 'True'
        os.environ['TIKA_SERVER_ENDPOINT'] = 'http://tika:9998'
        from tika import parser
        
        headers = {"X-Tika-PDFOcrStrategy": "no_ocr"}
        requestOptions = {'timeout': 30}
        parsed = parser.from_buffer(binary_data, headers=headers, requestOptions=requestOptions)
        
        content = parsed.get("content", "")
        if content is None: content = ""
        text = " ".join(content.split())
        
        metadata = parsed.get("metadata", {})
        
        if len(text) < 100:
            logger.info(f"Omission de l'OCR pour performance sur : {bucket}/{path}")
                
        del binary_data
        
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
        logger.error(f"Erreur d'extraction de document ({bucket}/{path}) : {str(e)}")
        return {"text": "", "title": ""}

document_schema = StructType([
    StructField("text", StringType(), True),
    StructField("title", StringType(), True)
])

parse_document_udf = udf(extract_document_from_minio, document_schema)
