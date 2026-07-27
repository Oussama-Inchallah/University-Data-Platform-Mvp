# ==============================================================================
# © 2026 University Data Platform
# Ce fichier fait partie du système central de la Plateforme Data Universitaire.
# ==============================================================================

"""
Parseur HTML Sécurisé (Zone Bronze -> Silver)
---------------------------------------------
Module en charge de l'ingestion distribuée des pages web brutes.
Utilise BeautifulSoup dans un contexte PySpark (UDF) pour nettoyer les scories HTML,
JavaScript et CSS, et extraire le contenu textuel qualifié pour le NLP.
"""

from bs4 import BeautifulSoup
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType
from typing import Dict
import os
import urllib3
import logging
import re

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

def extract_html_from_minio(bucket: str, path: str) -> Dict[str, str]:
    """
    Télécharge et parse dynamiquement une page HTML stockée sur MinIO (S3).
    
    Architecture (DataOps) :
    - Exécuté directement par les workers Spark (Lazy initialization du client S3).
    - Inclut une heuristique de nettoyage agressif (Regex) pour éviter
      la pollution sémantique (résidus de JS/CSS, iframes).
      
    Args:
        bucket (str): Nom du bucket S3/MinIO.
        path (str): Chemin absolu de l'objet dans le bucket.
        
    Returns:
        Dict[str, str]: Dictionnaire contenant 'title' et 'text' normalisés.
    """
    if not bucket or not path:
        return {"title": "", "text": ""}
        
    try:
        global minio_client
        if 'minio_client' not in globals():
            from minio import Minio
            endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
            access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
            secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
            minio_client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
            
        response = minio_client.get_object(bucket, path)
        html_content = response.read().decode('utf-8', errors='ignore')
        response.close()
        response.release_conn()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        title_tag = soup.find('title')
        title = title_tag.get_text(strip=True) if title_tag else ""
        if not title:
            h1_tag = soup.find('h1')
            title = h1_tag.get_text(strip=True) if h1_tag else ""
        
        for element in soup(["script", "style", "noscript", "nav", "footer", "header", "iframe", "object", "embed", "svg", "form", "button"]):
            element.decompose()
            
        text = soup.get_text(separator=' ')
        
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\{.*?\}', ' ', text)
        text = re.sub(r'[A-Za-z0-9_]+\.getAttribute\([^)]+\)', ' ', text)
        text = re.sub(r'function\s*\([^)]*\)\s*\{', ' ', text)
        text = re.sub(r'Math\.random\(\).*?;', ' ', text)
        text = re.sub(r'https?://[^\s]+\.(webp|png|jpg|jpeg|gif|css|js)', ' ', text)
        
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return {"title": title, "text": text}
    except Exception as e:
        logger.error(f"Erreur d'extraction HTML ({bucket}/{path}) : {str(e)}")
        return {"title": "", "text": ""}

parse_html_schema = StructType([
    StructField("title", StringType(), True),
    StructField("text", StringType(), True)
])

parse_html_udf = udf(extract_html_from_minio, parse_html_schema)
