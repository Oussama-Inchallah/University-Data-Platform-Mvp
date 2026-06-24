from fastapi import FastAPI, Query, HTTPException
from elasticsearch import Elasticsearch
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer
import logging

# Configuration des logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SearchAPI")

app = FastAPI(
    title="University Data Platform - Search & RAG API",
    description="Endpoint de recherche basique (Keyword et Sémantique) pour le MVP.",
    version="1.0.0"
)

# Chargement global des clients et du modèle
try:
    logger.info("Connexion à Elasticsearch...")
    es_client = Elasticsearch("http://elasticsearch:9200")
    
    logger.info("Connexion à Qdrant...")
    qdrant_client = QdrantClient(url="http://qdrant:6333")
    
    logger.info("Chargement du modèle de vectorisation (Zero-Latency)...")
    # Ce modèle a été mis en cache lors du docker build
    model = SentenceTransformer("intfloat/multilingual-e5-small")
    
    logger.info("API prête à recevoir des requêtes.")
except Exception as e:
    logger.error(f"Erreur lors de l'initialisation de l'API : {e}")

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Search & RAG API is running"}

@app.get("/search/keyword")
def search_keyword(q: str = Query(..., description="Le mot-clé à rechercher")):
    """
    Recherche lexicale (Mots-clés purs) via Elasticsearch.
    Recherche dans le titre et le contenu normalisé.
    """
    try:
        body = {
            "query": {
                "multi_match": {
                    "query": q,
                    "fields": ["normalized_text"],
                    "fuzziness": "AUTO"
                }
            },
            "size": 10
        }
        
        response = es_client.search(index="university_content", body=body)
        
        results = []
        for hit in response["hits"]["hits"]:
            src = hit["_source"]
            results.append({
                "score": hit["_score"],
                "type": src.get("entity_type", "Document"),
                "snippet": src.get("normalized_text", "")[:300] + "...",
                "faculty": src.get("source_system", "Unknown"),
                "language": src.get("language", "Unknown")
            })
            
        return {"query": q, "total_hits": response["hits"]["total"]["value"], "results": results}
        
    except Exception as e:
        logger.error(f"Erreur Elasticsearch : {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/search/semantic")
def search_semantic(q: str = Query(..., description="La question en langage naturel")):
    """
    Recherche Sémantique (Vectorielle) via Qdrant.
    Génère l'embedding de la question puis trouve les chunks les plus proches.
    """
    try:
        # e5-small requiert le préfixe "query: " pour les requêtes
        formatted_query = f"query: {q}"
        
        # Encodage de la question
        query_vector = model.encode(formatted_query).tolist()
        
        # Recherche dans Qdrant (on demande plus de résultats pour pouvoir dédupliquer)
        search_result = qdrant_client.search(
            collection_name="university_chunks",
            query_vector=query_vector,
            limit=15
        )
        
        results = []
        seen_texts = set()
        
        for hit in search_result:
            payload = hit.payload or {}
            text = payload.get("chunk_text", "").strip()
            
            # Élimination des doublons exacts
            if text in seen_texts:
                continue
            seen_texts.add(text)
            
            results.append({
                "score": hit.score,
                "text": text,
                "url": payload.get("source_url", ""),
                "faculty": payload.get("source_system", "Unknown"),
                "source_table": payload.get("target_table", "")
            })
            
            # On s'arrête dès qu'on a 5 résultats uniques
            if len(results) >= 5:
                break
            
        return {"query": q, "results": results}
        
    except Exception as e:
        logger.error(f"Erreur Qdrant/Embedding : {e}")
        raise HTTPException(status_code=500, detail=str(e))
