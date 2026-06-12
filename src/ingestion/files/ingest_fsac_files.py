"""
Script d'ingestion des fichiers plats (PDF, images, etc.) de la FSAC.
Branche : feature/scrape-uh2c-fsac

Ce script télécharge les documents référencés sur le site FSAC et les stocke
directement dans MinIO (Sidecar pattern), avec routage dynamique :
  - PDFs, CSVs, DOCs  -> bucket `raw-documents`
  - Images (JPG, PNG) -> bucket `raw-images`
"""

import os
import re
import io
import json
import hashlib
from datetime import datetime, timezone
from urllib.parse import urljoin, unquote

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

BASE_URL = "https://fsac.univh2c.ma/front/"

# Les pages qui contiennent les liens vers les documents
JSON_ENDPOINTS = [
    "https://fsac.univh2c.ma/Docs/static_pages/formations.json",
    "https://fsac.univh2c.ma/Docs/static_pages/presentation.json",
    "https://fsac.univh2c.ma/Docs/static_pages/departements.json",
    "https://fsac.univh2c.ma/Docs/static_pages/recherche.json"
]

# Extensions de fichiers plats ciblées
TARGET_EXTENSIONS = [".pdf", ".csv", ".jpg", ".jpeg", ".png", ".doc", ".docx"]

# ── Fonctions ──────────────────────────────────────────────────────────────────

def get_bucket_and_type(ext):
    """Détermine le bucket et le content-type en fonction de l'extension."""
    ext = ext.lower()
    if ext in [".jpg", ".jpeg", ".png"]:
        return "raw-images", f"image/{ext.strip('.')}"
    elif ext in [".pdf"]:
        return "raw-documents", "application/pdf"
    elif ext in [".csv"]:
        return "raw-documents", "text/csv"
    elif ext in [".doc", ".docx"]:
        return "raw-documents", "application/msword"
    return "raw-documents", "application/octet-stream"

def build_minio_prefix():
    """Construit le préfixe de chemin partitionné par date (Hive-style)."""
    now = datetime.now(timezone.utc)
    return (
        f"source=fsac/year={now.strftime('%Y')}"
        f"/month={now.strftime('%m')}"
        f"/day={now.strftime('%d')}"
    )

def extract_links_from_json(url):
    """Récupère le JSON et extrait tous les liens vers des fichiers plats."""
    print(f"[*] Analyse de {url}...")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"    [X] Erreur lors de la récupération : {e}")
        return []

    html_content = ""
    for part in data.get("parts", []):
        html_content += part.get("html", "")

    # Recherche des attributs href="..." et src="..."
    links = re.findall(r'(?:href|src)=["\']([^"\']+)["\']', html_content)
    
    # Filtrer par extension
    valid_links = []
    for link in links:
        link_lower = link.lower()
        if any(link_lower.endswith(ext) for ext in TARGET_EXTENSIONS):
            valid_links.append(link)
            
    return list(set(valid_links))  # Dédoublonner

def process_file(file_url):
    """Télécharge un fichier et le stocke dans MinIO avec ses métadonnées."""
    absolute_url = urljoin(BASE_URL, file_url)
    file_name = os.path.basename(unquote(absolute_url))
    file_name = re.sub(r'[<>:"/\\|?*]', '_', file_name)  # Nettoyage
    
    # Récupérer l'extension
    _, ext = os.path.splitext(file_name)
    bucket_name, content_type = get_bucket_and_type(ext)

    # S'assurer que le bucket existe
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    print(f"    [+] Téléchargement : {file_name}")
    try:
        response = requests.get(absolute_url, timeout=30)
        response.raise_for_status()
        raw_bytes = response.content
        http_status = response.status_code
        
        prefix = build_minio_prefix()
        file_path = f"{prefix}/{file_name}"
        meta_path = f"{prefix}/{file_name}_metadata.json"

        # 1. Sauvegarder le fichier brut (100% raw)
        minio_client.put_object(
            bucket_name,
            file_path,
            data=io.BytesIO(raw_bytes),
            length=len(raw_bytes),
            content_type=content_type,
        )
        print(f"        -> s3://{bucket_name}/{file_path}")

        # 2. Construire et sauvegarder les métadonnées (Sidecar)
        metadata = {
            "source_system": "FSAC Website",
            "source_url": absolute_url,
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
    except Exception as e:
        print(f"    [X] Échec du téléchargement/upload ({absolute_url}) : {e}")

# ── Script principal ───────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Ingestion des fichiers plats FSAC vers MinIO (Raw Zone)")
    print("  Routage : raw-documents (.pdf, .csv) / raw-images (.jpg, .png)")
    print("=" * 60)

    all_links = []
    for endpoint in JSON_ENDPOINTS:
        links = extract_links_from_json(endpoint)
        print(f"    -> {len(links)} fichiers trouvés.")
        all_links.extend(links)

    # Dédoublonner globalement
    all_links = list(set(all_links))
    print("-" * 60)
    print(f"Total unique de fichiers à ingérer : {len(all_links)}")
    print("-" * 60)

    for i, link in enumerate(all_links, 1):
        print(f"[{i}/{len(all_links)}]", end="")
        process_file(link)
        
    print("=" * 60)
    print("  Terminé !")
    print("=" * 60)

if __name__ == "__main__":
    main()
