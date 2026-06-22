"""
Parseur HTML (Web) - Metadata Driven
------------------------------------
Ce module utilise BeautifulSoup via une UDF Spark pour extraire le texte propre
et le titre d'une page HTML téléchargée à la volée depuis MinIO.
"""
from bs4 import BeautifulSoup
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType
from typing import Dict
import os
import urllib3
import logging

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

def extract_html_from_minio(bucket: str, path: str) -> Dict[str, str]:
    """
    Télécharge une page HTML depuis MinIO et extrait son texte utile et son titre.
    """
    if not bucket or not path:
        return {"title": "", "text": ""}
        
    try:
        from minio import Minio
        
        endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
        access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
        
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        
        response = client.get_object(bucket, path)
        html_content = response.read().decode('utf-8', errors='ignore')
        response.close()
        response.release_conn()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extraction du titre
        title_tag = soup.find('title')
        title = title_tag.get_text(strip=True) if title_tag else ""
        if not title:
            h1_tag = soup.find('h1')
            title = h1_tag.get_text(strip=True) if h1_tag else ""
        
        # Suppression des éléments non textuels ou parasites
        for element in soup(["script", "style", "noscript", "nav", "footer", "header"]):
            element.decompose()
            
        text = soup.get_text(separator=' ')
        
        # Nettoyage des espaces multiples et sauts de ligne
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return {"title": title, "text": text}
    except Exception as e:
        logger.error(f"Erreur extraction HTML {bucket}/{path}: {str(e)}")
        return {"title": "", "text": ""}

# Définition du schéma de retour de l'UDF
parse_html_schema = StructType([
    StructField("title", StringType(), True),
    StructField("text", StringType(), True)
])

# UDF (User Defined Function) Spark prête à l'emploi
parse_html_udf = udf(extract_html_from_minio, parse_html_schema)
