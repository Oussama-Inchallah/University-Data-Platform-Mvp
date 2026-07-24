"""
Parseur JSON - Metadata Driven (Mode Batch)
-------------------------------------------
Extrait les textes contenus dans les réponses JSON des API téléchargées.
Capable de détecter les pages d'API (comme OpenAlex) et d'éclater les résultats
en un tableau d'objets pour éviter le problème des "Small Files".
"""
import logging
from typing import List, Dict
import os
import json
import urllib3
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import udf

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

def extract_json_from_minio(bucket: str, path: str) -> List[Dict[str, str]]:
    if not bucket or not path:
        return []
        
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
        content = response.read().decode('utf-8', errors='ignore')
        response.close()
        response.release_conn()
        
        data = json.loads(content)
        
        def extract_strings(obj):
            strings = []
            if isinstance(obj, dict):
                for v in obj.values():
                    strings.extend(extract_strings(v))
            elif isinstance(obj, list):
                for item in obj:
                    strings.extend(extract_strings(item))
            elif isinstance(obj, str):
                s = obj.strip()
                if not s:
                    return strings
                    
                # Tentative de désérialisation si c'est un JSON stringifié (ex: "[{...}]")
                if (s.startswith('{') and s.endswith('}')) or (s.startswith('[') and s.endswith(']')):
                    try:
                        inner_obj = json.loads(s)
                        strings.extend(extract_strings(inner_obj))
                        return strings
                    except:
                        pass
                        
                # Nettoyage des balises HTML si présentes (ex: "<h1 style=...>Texte</h1>")
                if '<' in s and '>' in s:
                    try:
                        from bs4 import BeautifulSoup
                        soup = BeautifulSoup(s, 'html.parser')
                        s = soup.get_text(separator=' ', strip=True)
                    except:
                        pass
                        
                if s:
                    strings.append(s)
            return strings
            
        def process_item(item_data, default_url=""):
            all_texts = extract_strings(item_data)
            text = " ".join(all_texts)
            text = " ".join(text.split())
            
            if len(text) > 200000:
                text = text[:200000]
                
            title = ""
            url = default_url
            if isinstance(item_data, dict):
                # Ajout de 'titre' (très courant dans les API francophones/marocaines)
                title = item_data.get("title", item_data.get("titre", item_data.get("name", item_data.get("display_name", ""))))
                url = item_data.get("id", item_data.get("url", default_url))
                
            return {"url": str(url), "title": str(title), "text": text}

        # Détection du format OpenAlex ou paginé (tableau 'results')
        if isinstance(data, dict) and "results" in data and isinstance(data["results"], list):
            results = []
            for item in data["results"]:
                results.append(process_item(item))
            return results if results else [process_item(data)]
        else:
            # Traitement d'un JSON normal ou d'un tableau direct
            if isinstance(data, list):
                return [process_item(item) for item in data]
            else:
                return [process_item(data)]
                
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction de {bucket}/{path}: {str(e)}")
        return []

# Le schéma renvoie maintenant un ARRAY de structures
json_schema = ArrayType(StructType([
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True)
]))

parse_json_udf = udf(extract_json_from_minio, json_schema)
