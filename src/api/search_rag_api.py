import socket

# ==============================================================================
# WORKAROUND DOCKER IPv6 "Network is unreachable" [Errno 101]
# Empêche l'API Gemini (google.generativeai) de bloquer indéfiniment 
# si Docker tente de résoudre googleapis.com en IPv6.
# ==============================================================================
old_getaddrinfo = socket.getaddrinfo
def new_getaddrinfo(*args, **kwargs):
    responses = old_getaddrinfo(*args, **kwargs)
    return [response for response in responses if response[0] == socket.AF_INET]
socket.getaddrinfo = new_getaddrinfo
# ==============================================================================

from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from typing import Optional, List
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

@app.get("/faculties")
def get_faculties():
    """
    Récupère la liste dynamique de toutes les facultés existantes dans la base.
    (Gardé pour rétrocompatibilité)
    """
    try:
        body = {
            "size": 0,
            "aggs": {
                "faculties": {
                    "terms": {
                        "field": "source_system.keyword",
                        "size": 100
                    }
                }
            }
        }
        response = es_client.search(index="university_content", body=body)
        buckets = response.get("aggregations", {}).get("faculties", {}).get("buckets", [])
        faculties = [b["key"] for b in buckets]
        return {"faculties": faculties}
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des facultés : {e}")
        return {"faculties": []}

@app.get("/metadata_filters")
def get_metadata_filters(
    faculty: str = Query(None),
    doc_type: str = Query(None),
    language: str = Query(None)
):
    """
    Récupère les listes dynamiques pour tous les filtres : facultés, types de doc, langues.
    Filtre dynamiquement en fonction des sélections précédentes (filtres en cascade).
    """
    try:
        must_conditions = []
        
        if faculty and faculty != "Toutes":
            must_conditions.append({"term": {"source_system.keyword": faculty}})
        if doc_type and doc_type != "Toutes":
            must_conditions.append({"term": {"entity_type.keyword": doc_type}})
        if language and language != "Toutes":
            must_conditions.append({"term": {"language.keyword": language}})
            
        body = {
            "size": 0,
            "aggs": {
                "faculties": {"terms": {"field": "source_system.keyword", "size": 100}},
                "doc_types": {"terms": {"field": "entity_type.keyword", "size": 100}},
                "languages": {"terms": {"field": "language.keyword", "size": 50}}
            }
        }
        
        if must_conditions:
            body["query"] = {"bool": {"must": must_conditions}}

        # On ignore les erreurs si certains champs n'existent pas encore dans l'index
        response = es_client.search(index="university_content", body=body, ignore_unavailable=True)
        aggs = response.get("aggregations", {})
        
        return {
            "faculties": [b["key"] for b in aggs.get("faculties", {}).get("buckets", [])],
            "doc_types": [b["key"] for b in aggs.get("doc_types", {}).get("buckets", []) if b["key"] != "unclassified_content"],
            "languages": [b["key"] for b in aggs.get("languages", {}).get("buckets", [])]
        }
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des filtres métadonnées : {e}")
        return {"faculties": [], "doc_types": [], "languages": []}

@app.get("/search/keyword")
def search_keyword(
    q: str = Query(..., description="Le mot-clé à rechercher"),
    faculty: str = Query(None, description="Filtrer par faculté")
):
    """
    Recherche lexicale (Mots-clés purs) via Elasticsearch.
    Recherche améliorée : multi-champs pondérés, recherche de phrase exacte, mise en évidence et facettes.
    """
    try:
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": q,
                                "fields": ["title^3", "url^2", "normalized_text", "raw_content"],
                                "fuzziness": "AUTO",
                                "type": "best_fields"
                            }
                        }
                    ],
                    "should": [
                        {
                            "match_phrase": {
                                "normalized_text": {
                                    "query": q,
                                    "slop": 2,
                                    "boost": 2.0
                                }
                            }
                        }
                    ]
                }
            },
            "highlight": {
                "fields": {
                    "normalized_text": {
                        "fragment_size": 150,
                        "number_of_fragments": 2,
                        "pre_tags": ["<mark>"],
                        "post_tags": ["</mark>"]
                    }
                }
            },
            "aggs": {
                "faculties": {
                    "terms": {
                        "field": "source_system.keyword"
                    }
                }
            },
            "size": 10
        }
        
        if faculty and faculty != "Toutes":
            body["query"]["bool"]["filter"] = [
                {"term": {"source_system.keyword": faculty}}
            ]
        
        response = es_client.search(index="university_content", body=body)
        
        results = []
        for hit in response["hits"]["hits"]:
            src = hit["_source"]
            
            # Récupération du highlight s'il existe, sinon on prend le début du texte
            highlights = hit.get("highlight", {})
            snippet = " ... ".join(highlights.get("normalized_text", []))
            if not snippet:
                snippet = src.get("normalized_text", "")[:200] + "..."

            results.append({
                "score": hit["_score"],
                "type": src.get("entity_type", "Document"),
                "snippet": snippet,
                "title": src.get("title", "Sans titre"),
                "faculty": src.get("source_system", "Unknown"),
                "language": src.get("language", "Unknown")
            })
            
        facets = response.get("aggregations", {}).get("faculties", {}).get("buckets", [])
            
        return {
            "query": q, 
            "total_hits": response["hits"]["total"]["value"], 
            "results": results,
            "facets": facets
        }
        
    except Exception as e:
        logger.error(f"Erreur Elasticsearch : {e}")
        raise HTTPException(status_code=500, detail=str(e))

import os
import google.generativeai as genai

# Configuration de Gemini
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
if GEMINI_API_KEY:
    # Force REST transport to avoid gRPC IPv6 hanging issues in Docker
    genai.configure(api_key=GEMINI_API_KEY, transport="rest")
    gemini_model = genai.GenerativeModel('gemini-3.1-flash-lite')
else:
    gemini_model = None

@app.get("/search/semantic")
def search_semantic(q: str = Query(..., description="La question en langage naturel")):
    """
    Recherche Sémantique (Vectorielle) via Qdrant avec Génération RAG (Gemini).
    Génère l'embedding de la question, trouve les chunks les plus proches, puis génère une réponse.
    """
    try:
        # e5-small requiert le préfixe "query: " pour les requêtes
        formatted_query = f"query: {q}"
        
        # Encodage de la question
        query_vector = model.encode(formatted_query).tolist()
        
        # Recherche dans Qdrant
        search_result = qdrant_client.search(
            collection_name="university_chunks",
            query_vector=query_vector,
            limit=15
        )
        
        results = []
        seen_texts = set()
        
        for hit in search_result:
            # --- Seuil intelligent (Option B) ---
            # On ignore les chunks dont la pertinence sémantique est trop faible
            if hit.score < 0.82:
                continue
                
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
            
            # Limite haute de sécurité pour les résultats Qdrant
            if len(results) >= 7:
                break
                
        # --- RECHERCHE HYBRIDE (Mots-clés via Elasticsearch) ---
        try:
            import re
            quoted_terms = re.findall(r'"([^"]+)"', q)
            
            must_clauses = [
                {
                    "multi_match": {
                        "query": q,
                        "fields": ["normalized_text^2", "raw_content"]
                    }
                }
            ]
            
            # Forcer la présence des mots entre guillemets
            for term in quoted_terms:
                must_clauses.append({
                    "match_phrase": {
                        "normalized_text": term
                    }
                })
                
            es_body = {
                "query": {
                    "bool": {
                        "must": must_clauses
                    }
                },
                "size": 5,
                "highlight": {
                    "fields": {
                        "normalized_text": {
                            "fragment_size": 300,
                            "number_of_fragments": 10
                        }
                    },
                    "pre_tags": [""],
                    "post_tags": [""]
                }
            }
            es_response = es_client.search(index="university_content", body=es_body)
            es_hits = es_response.get("hits", {}).get("hits", [])
            for hit in es_hits:
                src = hit.get("_source", {})
                
                # Si on a des extraits pertinents (Highlighting), on les utilise pour ne pas envoyer tout le gros document
                highlights = hit.get("highlight", {}).get("normalized_text", [])
                if highlights:
                    text = " ... ".join(highlights)
                else:
                    text = src.get("normalized_text", "").strip()[:2000]
                    
                if not text or text in seen_texts:
                    continue
                seen_texts.add(text)
                
                results.append({
                    "score": 1.0 + (hit.get("_score", 0) / 50.0), # Boost pour garantir le top classement
                    "text": text,
                    "url": src.get("url", ""),
                    "faculty": src.get("source_system", "Unknown"),
                    "source_table": "Elasticsearch"
                })
        except Exception as e:
            logger.error(f"Erreur lors de la recherche hybride Elasticsearch : {e}")
            
        # Trier par score (ES > 1.0 passera devant Qdrant qui est < 1.0)
        # et limiter à 10 pour le LLM
        results = sorted(results, key=lambda x: x['score'], reverse=True)[:10]
                
        # --- GENERATION RAG ---
        llm_answer = None
        if gemini_model and results:
            context_text = "\n\n---\n\n".join([f"Source ({r['faculty']}): {r['text']}" for r in results])
            prompt = f"""Tu es l'assistant IA officiel de l'université.
Utilise UNIQUEMENT le contexte ci-dessous pour répondre à la question de l'étudiant. 
Si la réponse ne se trouve pas dans le contexte, dis poliment que tu ne sais pas, sans inventer d'information.
Fais une réponse claire, structurée et professionnelle.

Contexte extrait de la base de connaissances :
{context_text}

Question de l'étudiant : {q}
Réponse :"""
            
            try:
                response = gemini_model.generate_content(prompt)
                llm_answer = response.text
            except Exception as e:
                logger.error(f"Erreur lors de la génération LLM : {e}")
                llm_answer = "Erreur lors de la génération de la réponse par l'IA."
        elif not gemini_model:
            llm_answer = "Génération RAG désactivée car la clé GEMINI_API_KEY n'est pas configurée."
            
        return {"query": q, "results": results, "llm_answer": llm_answer}
        
    except Exception as e:
        logger.error(f"Erreur Qdrant/Embedding : {e}")
        raise HTTPException(status_code=500, detail=str(e))

from qdrant_client.http import models

@app.get("/search/qdrant_pure")
def search_qdrant_pure(
    q: str = Query(..., description="La question en langage naturel"),
    doc_type: str = Query(None, description="Filtrer par type de document (ex: documents_registry)")
):
    """
    Recherche Sémantique (Vectorielle) pure via Qdrant, sans génération IA ni hybridation ES.
    Idéal pour retrouver des documents similaires (règlements, syllabi, etc.).
    """
    try:
        formatted_query = f"query: {q}"
        query_vector = model.encode(formatted_query).tolist()
        
        query_filter = None
        if doc_type and doc_type != "Tous":
            query_filter = models.Filter(
                must=[
                    models.FieldCondition(
                        key="target_table",
                        match=models.MatchValue(value=doc_type)
                    )
                ]
            )
        
        search_result = qdrant_client.search(
            collection_name="university_chunks",
            query_vector=query_vector,
            query_filter=query_filter,
            limit=20
        )
        
        results = []
        seen_texts = set()
        
        for hit in search_result:
            if hit.score < 0.80:
                continue
                
            payload = hit.payload or {}
            text = payload.get("chunk_text", "").strip()
            
            if text in seen_texts:
                continue
            seen_texts.add(text)
            
            results.append({
                "score": hit.score,
                "text": text,
                "url": payload.get("source_url", ""),
                "faculty": payload.get("source_system", "Unknown"),
                "source_table": payload.get("target_table", "Unknown")
            })
            
            if len(results) >= 10:
                break
                
        return {"query": q, "results": results}
        
    except Exception as e:
        logger.error(f"Erreur Qdrant Pure : {e}")
        raise HTTPException(status_code=500, detail=str(e))

class AdvancedSearchPayload(BaseModel):
    query: str
    doc_id: Optional[str] = None
    type: Optional[str] = None
    faculty: Optional[str] = None
    language: Optional[str] = None
    source: Optional[str] = None

@app.post("/search/qdrant_advanced")
def search_qdrant_advanced(payload: AdvancedSearchPayload):
    """
    Recherche Sémantique (Vectorielle) avec filtres de métadonnées (Hybride).
    Construit dynamiquement les conditions exactes de Qdrant.
    """
    try:
        formatted_query = f"query: {payload.query}"
        query_vector = model.encode(formatted_query).tolist()
        
        # Construction dynamique du filtre Qdrant
        must_conditions = []
        
        # Correspondance avec le payload
        if payload.doc_id:
            must_conditions.append(models.FieldCondition(key="doc_id", match=models.MatchValue(value=payload.doc_id)))
        
        if payload.type:
            # Map type -> target_table ou autre champ selon NLP processor
            must_conditions.append(models.FieldCondition(key="target_table", match=models.MatchValue(value=payload.type)))
            
        if payload.faculty:
            # Map faculty -> source_system
            must_conditions.append(models.FieldCondition(key="source_system", match=models.MatchValue(value=payload.faculty)))
            
        if payload.language:
            must_conditions.append(models.FieldCondition(key="language", match=models.MatchValue(value=payload.language)))
            
        if payload.source:
            must_conditions.append(models.FieldCondition(key="source", match=models.MatchValue(value=payload.source)))

        query_filter = None
        if must_conditions:
            query_filter = models.Filter(must=must_conditions)
            
        search_result = qdrant_client.search(
            collection_name="university_chunks",
            query_vector=query_vector,
            query_filter=query_filter,
            limit=20
        )
        
        results = []
        seen_texts = set()
        
        for hit in search_result:
            if hit.score < 0.75: # Seuil un peu plus permissif car on a des filtres stricts
                continue
                
            chunk_payload = hit.payload or {}
            text = chunk_payload.get("chunk_text", "").strip()
            
            if text in seen_texts:
                continue
            seen_texts.add(text)
            
            results.append({
                "score": hit.score,
                "text": text,
                "doc_id": chunk_payload.get("doc_id", ""),
                "type": chunk_payload.get("target_table", ""),
                "faculty": chunk_payload.get("source_system", ""),
                "language": chunk_payload.get("language", ""),
                "source": chunk_payload.get("source", ""),
                "url": chunk_payload.get("source_url", "")
            })
            
            if len(results) >= 10:
                break
                
        return {"query": payload.query, "filters_applied": len(must_conditions), "results": results}
        
    except Exception as e:
        logger.error(f"Erreur Qdrant Advanced : {e}")
        raise HTTPException(status_code=500, detail=str(e))
