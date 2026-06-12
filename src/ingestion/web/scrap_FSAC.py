"""
Scraping FSAC — Faculté des Sciences Aïn Chock
Branche : feature/scrape-uh2c-fsac

Télécharge le HTML brut des pages du site FSAC et le stocke
dans MinIO (bucket raw-web-html) avec la méthode Sidecar :
  - <page>.html           → HTML brut (aucune modification)
  - <page>_metadata.json  → Métadonnées d'ingestion séparées

Le parsing HTML sera effectué par Spark lors de l'étape Processing.

Pages cibles :
  - /front/index.html                       → Accueil (annonces, chiffres clés)
  - /front/tabs.html?page=presentation      → Doyen, staff administratif
  - /front/tabs.html?page=departements      → 5 départements + enseignants
  - /front/tabs.html?page=formations        → Catalogue des formations
  - /front/tabs.html?page=recherche         → Laboratoires de recherche
"""

import os
import io
import json
import hashlib
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from minio import Minio

# ── Configuration ──────────────────────────────────────────────────────────────

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

# ── Pages cibles ───────────────────────────────────────────────────────────────

# Après analyse du code source, le site FSAC utilise JavaScript pour charger
# le contenu des onglets depuis des fichiers JSON cachés.
# Nous tapons donc directement ces endpoints pour récupérer les vraies données.

PAGES = {
    "accueil":      "https://fsac.univh2c.ma/front/index.html",
    "presentation": "https://fsac.univh2c.ma/Docs/static_pages/presentation.json",
    "departements": "https://fsac.univh2c.ma/Docs/static_pages/departements.json",
    "formations":   "https://fsac.univh2c.ma/Docs/static_pages/formations.json",
    "recherche":    "https://fsac.univh2c.ma/Docs/static_pages/recherche.json",
}


# ── Fonctions ──────────────────────────────────────────────────────────────────

def build_minio_prefix():
    """Construit le préfixe de chemin partitionné par date (Hive-style)."""
    now = datetime.now(timezone.utc)
    return (
        f"source=fsac/year={now.strftime('%Y')}"
        f"/month={now.strftime('%m')}"
        f"/day={now.strftime('%d')}"
    )

def process_page(page_name, url):
    """Traite une ressource : téléchargement brut + sauvegarde + sauvegarde metadata (Sidecar)."""
    print(f"  -> GET {url}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    raw_bytes = response.content
    http_status = response.status_code
    
    # Déterminer l'extension d'origine et le bucket cible
    if url.endswith(".json"):
        ext = ".json"
        content_type = "application/json"
        bucket_name = "raw-json"
    else:
        ext = ".html"
        content_type = "text/html"
        bucket_name = "raw-web-html"

    # S'assurer que le bucket existe
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    # Construire les chemins MinIO
    prefix = build_minio_prefix()
    file_path = f"{prefix}/{page_name}{ext}"
    meta_path = f"{prefix}/{page_name}_metadata.json"

    # 1. Sauvegarder le fichier brut (absolument aucune modification, 100% raw)
    minio_client.put_object(
        bucket_name,
        file_path,
        data=io.BytesIO(raw_bytes),
        length=len(raw_bytes),
        content_type=content_type,
    )
    print(f"    [+] DATA -> s3://{bucket_name}/{file_path}")

    # 2. Construire et sauvegarder les métadonnées (Sidecar)
    metadata = {
        "source_system": "FSAC Website",
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


# ── Script principal ───────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Ingestion Web Scraping — FSAC (Raw Zone)")
    print("  Routage : raw-web-html (.html) / raw-json (.json)")
    print("=" * 60)

    success_count = 0
    error_count = 0

    for page_name, url in PAGES.items():
        try:
            print(f"\n[*] Page : {page_name}")
            process_page(page_name, url)
            success_count += 1
        except requests.exceptions.RequestException as e:
            print(f"    [X] Erreur sur {page_name} : {e}")
            error_count += 1

    print("\n" + "=" * 60)
    print(f"  Terminé : {success_count} pages OK, {error_count} erreurs.")
    print(f"  Fichiers créés : {success_count * 2} ({success_count} HTML + {success_count} metadata)")
    print("=" * 60)


if __name__ == "__main__":
    main()
