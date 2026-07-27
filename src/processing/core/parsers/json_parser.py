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
        # Initialisation lazy du client MinIO
        global minio_client
        if 'minio_client' not in globals():
            from minio import Minio
            endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
            access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
            secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
            minio_client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
            
        # Streamer le fichier JSON depuis MinIO
        response = minio_client.get_object(bucket, path)
        content = response.read().decode('utf-8', errors='ignore')
        response.close()
        response.release_conn()
        
        data = json.loads(content)
        
        def reconstruct_openalex_abstract(inverted_index: dict) -> str:
            if not inverted_index:
                return ""
            try:
                max_pos = max([pos for positions in inverted_index.values() for pos in positions])
                words = [""] * (max_pos + 1)
                for word, positions in inverted_index.items():
                    for pos in positions:
                        words[pos] = word
                return " ".join(words).strip()
            except Exception:
                return ""

        def extract_strings(obj):
            # Fallback générique qui évite de prendre les URLs, les IDs techniques, etc.
            strings = []
            if isinstance(obj, dict):
                for k, v in obj.items():
                    # Ignorer les clés bruyantes
                    if str(k).lower() in ['id', 'url', 'doi', 'orcid', 'ror', 'issn', 'wikidata', 'uri']:
                        continue
                    strings.extend(extract_strings(v))
            elif isinstance(obj, list):
                for item in obj:
                    strings.extend(extract_strings(item))
            elif isinstance(obj, str):
                s = obj.strip()
                # Ignorer les chaînes qui sont juste des URLs
                if s.startswith('http://') or s.startswith('https://') or s.startswith('doi:'):
                    return strings
                if len(s) > 2 and not s.isdigit():  # Ignorer les strings trop courtes ou purement numériques
                    strings.append(s)
            return strings
            
        def process_item(item_data, default_url=""):
            title = ""
            url = default_url
            text = ""
            
            if isinstance(item_data, dict):
                title = item_data.get("title", item_data.get("titre", item_data.get("name", item_data.get("display_name", ""))))
                url = item_data.get("id", item_data.get("url", default_url))
                
                # --- Traitement Spécifique : Schema OpenAlex ---
                if "abstract_inverted_index" in item_data or (isinstance(url, str) and "openalex.org/W" in url):
                    abstract = reconstruct_openalex_abstract(item_data.get("abstract_inverted_index"))
                    
                    authors = []
                    authors_list = item_data.get("authorships") or []
                    for authorship in authors_list:
                        author_name = authorship.get("author", {}).get("display_name", "")
                        if author_name: authors.append(author_name)
                    
                    concepts = []
                    concepts_list = item_data.get("concepts") or []
                    for concept in concepts_list:
                        concept_name = concept.get("display_name", "")
                        if concept_name: concepts.append(concept_name)
                        
                    parts = []
                    if title: parts.append(f"Title: {title}")
                    if authors: parts.append(f"Authors: {', '.join(authors)}")
                    if concepts: parts.append(f"Keywords: {', '.join(concepts)}")
                    if abstract: parts.append(f"Abstract: {abstract}")
                    
                    text = "\n".join(parts)
                # --- Fin Traitement OpenAlex ---
                else:
                    # Fallback JSON générique (épuré)
                    all_texts = extract_strings(item_data)
                    text = " ".join(all_texts)
                    
            text = " ".join(text.split()) # Normalize whitespaces
            if len(text) > 200000:
                text = text[:200000]
                
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
