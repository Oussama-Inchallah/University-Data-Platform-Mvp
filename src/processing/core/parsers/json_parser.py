# ==============================================================================
# © 2026 University Data Platform
# Ce fichier fait partie du système central de la Plateforme Data Universitaire.
# ==============================================================================

"""
Parseur JSON Avancé (APIs & OpenAlex)
-------------------------------------
Parseur générique pour le traitement distribué des réponses API stockées en format JSON.
Intègre une heuristique pour désérialiser les index inversés complexes (ex: Schema OpenAlex)
afin d'extraire les résumés (abstracts) et métadonnées scientifiques.
"""

import logging
import os
import json
import urllib3
from typing import List, Dict
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import udf

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

def extract_json_from_minio(bucket: str, path: str) -> List[Dict[str, str]]:
    """
    Lit et aplatit un fichier JSON (Array ou Object) depuis MinIO.
    
    Architecture (DataOps) :
    - Gestion robuste de la pagination implicite (détection de listes de résultats).
    - Traitement spécial du schéma OpenAlex pour le Text Mining.
    
    Args:
        bucket (str): Bucket source.
        path (str): Chemin d'accès.
        
    Returns:
        List[Dict[str, str]]: Liste d'objets standardisés (url, title, text).
    """
    if not bucket or not path:
        return []
        
    try:
        global minio_client
        if 'minio_client' not in globals():
            from minio import Minio
            endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
            access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
            secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
            minio_client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
            
        response = minio_client.get_object(bucket, path)
        content = response.read().decode('utf-8', errors='ignore')
        response.close()
        response.release_conn()
        
        data = json.loads(content)
        
        def reconstruct_openalex_abstract(inverted_index: dict) -> str:
            """Reconstruit le texte à partir d'un index inversé (OpenAlex)."""
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
            """Extraction récursive sécurisée pour le nettoyage sémantique."""
            strings = []
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if str(k).lower() in ['id', 'url', 'doi', 'orcid', 'ror', 'issn', 'wikidata', 'uri']:
                        continue
                    strings.extend(extract_strings(v))
            elif isinstance(obj, list):
                for item in obj:
                    strings.extend(extract_strings(item))
            elif isinstance(obj, str):
                s = obj.strip()
                if s.startswith('http://') or s.startswith('https://') or s.startswith('doi:'):
                    return strings
                if len(s) > 2 and not s.isdigit():
                    strings.append(s)
            return strings
            
        def process_item(item_data, default_url=""):
            title = ""
            url = default_url
            text = ""
            
            if isinstance(item_data, dict):
                title = item_data.get("title", item_data.get("titre", item_data.get("name", item_data.get("display_name", ""))))
                url = item_data.get("id", item_data.get("url", default_url))
                
                if "abstract_inverted_index" in item_data or (isinstance(url, str) and "openalex.org/W" in url):
                    abstract = reconstruct_openalex_abstract(item_data.get("abstract_inverted_index"))
                    
                    authors = []
                    for authorship in (item_data.get("authorships") or []):
                        author_name = authorship.get("author", {}).get("display_name", "")
                        if author_name: authors.append(author_name)
                    
                    concepts = []
                    for concept in (item_data.get("concepts") or []):
                        concept_name = concept.get("display_name", "")
                        if concept_name: concepts.append(concept_name)
                        
                    parts = []
                    if title: parts.append(f"Title: {title}")
                    if authors: parts.append(f"Authors: {', '.join(authors)}")
                    if concepts: parts.append(f"Keywords: {', '.join(concepts)}")
                    if abstract: parts.append(f"Abstract: {abstract}")
                    
                    text = "\n".join(parts)
                else:
                    all_texts = extract_strings(item_data)
                    text = " ".join(all_texts)
                    
            text = " ".join(text.split())
            if len(text) > 200000:
                text = text[:200000]
                
            return {"url": str(url), "title": str(title), "text": text}

        if isinstance(data, dict) and "results" in data and isinstance(data["results"], list):
            results = [process_item(item) for item in data["results"]]
            return results if results else [process_item(data)]
        else:
            if isinstance(data, list):
                return [process_item(item) for item in data]
            else:
                return [process_item(data)]
                
    except Exception as e:
        logger.error(f"Erreur extraction JSON ({bucket}/{path}) : {str(e)}")
        return []

json_schema = ArrayType(StructType([
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True)
]))

parse_json_udf = udf(extract_json_from_minio, json_schema)
