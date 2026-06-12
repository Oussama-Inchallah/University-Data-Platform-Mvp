"""
Extraction API OpenAlex (UH2C)
Branche : feature/ingest-openalex

Ce script interroge l'API REST publique d'OpenAlex pour récupérer la totalité des
publications (Works) associées à l'Université Hassan II de Casablanca (ID: I99297268).

Il utilise la pagination par curseur (Cursor Pagination) obligatoire sur OpenAlex 
pour récupérer plus de 10 000 résultats (environ 22 900 publications existantes).

Le format de retour étant un JSON natif, la donnée est directement stockée
dans le bucket `raw-json` (sans modification) avec ses métadonnées d'ingestion.
"""

import os
import io
import json
import hashlib
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from minio import Minio

# ── Configuration OpenAlex ─────────────────────────────────────────────────────

# OpenAlex ID pour "University of Hassan II Casablanca"
UH2C_OPENALEX_ID = "I99297268"
BASE_URL = "https://api.openalex.org/works"

# Paramètres du Polite Pool (Optionnel mais recommandé par OpenAlex)
CONTACT_EMAIL = "data.engineering.mvp@example.com"
PER_PAGE = 200 # Le maximum autorisé par OpenAlex

# ── Configuration MinIO ────────────────────────────────────────────────────────

env_path = os.path.join(os.path.dirname(__file__), "../../../infra/docker/.env")
load_dotenv(dotenv_path=env_path)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password")

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)

# ── Fonctions ──────────────────────────────────────────────────────────────────

def build_minio_prefix():
    """Construit le préfixe de chemin partitionné par date (Hive-style)."""
    now = datetime.now(timezone.utc)
    return (
        f"source=openalex/year={now.strftime('%Y')}"
        f"/month={now.strftime('%m')}"
        f"/day={now.strftime('%d')}"
    )

def fetch_and_store_page(cursor, page_num):
    """
    Récupère une page via le curseur depuis OpenAlex et la stocke dans MinIO.
    Retourne le prochain curseur (next_cursor) ou None s'il n'y a plus de résultats.
    """
    # Construit l'URL avec les filtres et le curseur
    url = (f"{BASE_URL}?filter=institutions.id:{UH2C_OPENALEX_ID}"
           f"&mailto={CONTACT_EMAIL}&per-page={PER_PAGE}&cursor={cursor}")
    
    print(f"  -> GET Page {page_num} (Curseur: {cursor[:15]}...)")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except Exception as e:
        print(f"    [X] Erreur de récupération pour la page {page_num} : {e}")
        return None
        
    raw_bytes = response.content
    http_status = response.status_code
    bucket_name = "raw-json"
    
    # Extraire le prochain curseur du JSON
    try:
        data = json.loads(raw_bytes)
        next_cursor = data.get("meta", {}).get("next_cursor")
    except json.JSONDecodeError:
        print(f"    [X] Erreur : La réponse n'est pas un JSON valide.")
        return None

    # S'assurer que le bucket existe
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    # Construire les chemins MinIO
    prefix = build_minio_prefix()
    file_path = f"{prefix}/university_news_page_{page_num}.json"
    meta_path = f"{prefix}/university_news_page_{page_num}_metadata.json"

    # 1. Sauvegarder le fichier brut (Payload JSON complet d'OpenAlex)
    minio_client.put_object(
        bucket_name,
        file_path,
        data=io.BytesIO(raw_bytes),
        length=len(raw_bytes),
        content_type="application/json",
    )
    print(f"    [+] DATA -> s3://{bucket_name}/{file_path}")

    # 2. Construire et sauvegarder les métadonnées (Sidecar)
    metadata = {
        "source_system": "OpenAlex API",
        "source_url": url,
        "crawl_timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "http_status": http_status,
        "content_hash": hashlib.md5(raw_bytes).hexdigest(),
        "raw_object_location": f"s3://{bucket_name}/{file_path}",
    }
    
    meta_bytes = json.dumps(metadata, ensure_ascii=False, indent=2).encode("utf-8")
    minio_client.put_object(
        bucket_name,
        meta_path,
        data=io.BytesIO(meta_bytes),
        length=len(meta_bytes),
        content_type="application/json",
    )
    print(f"    [+] META -> s3://{bucket_name}/{meta_path}")
    
    return next_cursor

# ── Script principal ───────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"  Ingestion API REST — OpenAlex pour UH2C ({UH2C_OPENALEX_ID})")
    print("  Mode : Full Extraction via Cursor Pagination")
    print("  Routage : raw-json (.json)")
    print("=" * 60)

    success_count = 0
    page_num = 1
    cursor = "*" # Curseur de départ défini par OpenAlex

    while cursor:
        next_cursor = fetch_and_store_page(cursor, page_num)
        
        if next_cursor:
            success_count += 1
            page_num += 1
            cursor = next_cursor
            # Politesse API (OpenAlex Polite Pool permet 10 req/sec)
            time.sleep(0.1)
        else:
            # Fin de la pagination ou erreur
            if next_cursor is None and page_num > 1:
                success_count += 1 # La dernière page a bien été récupérée
            break

    print("\n" + "=" * 60)
    print(f"  Terminé : {success_count} pages (de {PER_PAGE} articles max) récupérées.")
    print(f"  Fichiers créés : {success_count * 2} ({success_count} JSON + {success_count} metadata)")
    print("=" * 60)

if __name__ == "__main__":
    main()
