"""
Gestionnaire de Stockage S3 (MinIO)
-----------------------------------
Ce module gère l'enregistrement des fichiers bruts (HTML, documents, images)
et de leurs métadonnées (fichiers JSON) dans MinIO (S3) lors du crawling.
"""
import hashlib
import logging
from io import BytesIO
from minio import Minio
from minio.error import S3Error
from datetime import datetime
from urllib.parse import urlparse, quote, unquote
import mimetypes
import json

class StorageRouter:
    def __init__(self, endpoint="minio:9000", access_key="minioadmin", secret_key="minioadmin", execution_date=None):
        self.logger = logging.getLogger(__name__)
        self.execution_date = execution_date
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        self.memory_index = {}
        
    def build_memory_index(self, domain_prefix: str, buckets: list):
        """
        Construit un index en mémoire des fichiers déjà téléchargés pour ce site
        en scannant tous les chemins (dates) dans MinIO.
        """
        self.logger.info(f"Building memory index for {domain_prefix} across buckets...")
        count = 0
        for bucket in buckets:
            try:
                objects = self.client.list_objects(bucket, prefix=domain_prefix, recursive=True)
                for obj in objects:
                    if obj.object_name.endswith('_metadata.json'):
                        continue
                    file_name = obj.object_name.split('/')[-1]
                    # list_objects renvoie par ordre alphabétique, donc les dates récentes écrasent les anciennes
                    self.memory_index[file_name] = {"bucket": bucket, "object_name": obj.object_name}
                    count += 1
            except S3Error as e:
                if e.code != 'NoSuchBucket':
                    self.logger.warning(f"Error scanning bucket {bucket}: {e}")
        self.logger.info(f"Memory index built with {count} unique files.")

    def _get_bucket_by_type(self, content_type: str, url: str) -> str:
        if 'text/html' in content_type:
            return "raw-web-html"
        elif 'image/' in content_type:
            return "raw-images"
        elif 'application/json' in content_type:
            return "raw-json"
        else:
            return "raw-documents"
            
    def compute_sha256(self, content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()
        
    def _generate_file_name(self, url: str, content_type: str) -> str:
        parsed_url = urlparse(url)
        decoded_path = unquote(parsed_url.path)
        file_name = decoded_path.split('/')[-1]
        
        if not file_name:
            path_parts_full = [p for p in decoded_path.split('/') if p]
            ext = mimetypes.guess_extension(content_type.split(';')[0]) or ""
            if 'text/html' in content_type:
                suffix = ".html"
            else:
                suffix = ext if ext else ""
                
            if path_parts_full:
                file_name = f"{path_parts_full[-1]}{suffix}"
            else:
                file_name = f"index{suffix}" if suffix else "unknown_file"
            
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:10]
        
        if '.' in file_name:
            base_name, ext = file_name.rsplit('.', 1)
            file_name = f"{base_name}_{url_hash}.{ext}"
        else:
            file_name = f"{file_name}_{url_hash}"

        if 'text/html' in content_type and not file_name.endswith('.html'):
            file_name += ".html"
            
        file_bytes_len = len(file_name.encode('utf-8'))
        if file_bytes_len > 200:
            name_hash = hashlib.md5(file_name.encode('utf-8')).hexdigest()[:8]
            if '.' in file_name:
                base_name, ext = file_name.rsplit('.', 1)
                file_name = f"{base_name[:150]}_{name_hash}.{ext}"
            else:
                file_name = f"{file_name[:150]}_{name_hash}"
                
        return file_name
        
    def get_existing_metadata(self, url: str, content_type: str) -> dict:
        """
        Vérifie si le fichier existe dans l'index mémoire et récupère ses métadonnées MinIO.
        """
        file_name = self._generate_file_name(url, content_type)
        if file_name in self.memory_index:
            entry = self.memory_index[file_name]
            try:
                stat = self.client.stat_object(entry['bucket'], entry['object_name'])
                return {
                    "sha256": stat.metadata.get('x-amz-meta-sha256', ''),
                    "server_etag": stat.metadata.get('x-amz-meta-server-etag', ''),
                    "server_last_modified": stat.metadata.get('x-amz-meta-server-last-modified', ''),
                    "bucket": entry['bucket'],
                    "object_name": entry['object_name']
                }
            except S3Error:
                pass
        return {}

    def upload_to_minio(self, url: str, content: bytes, content_type: str, node_id: str, university_id: str, domain_prefix: str, depth: int = 0, parent_url: str = "", http_status: int = 200, force_bucket: str = None, server_etag: str = "", server_last_modified: str = ""):
        bucket_name = force_bucket if force_bucket else self._get_bucket_by_type(content_type, url)
        now = datetime.utcnow()
        file_name = self._generate_file_name(url, content_type)
        content_hash = self.compute_sha256(content)
        
        # Dédoublonnage via index mémoire
        if file_name in self.memory_index:
            entry = self.memory_index[file_name]
            try:
                stat = self.client.stat_object(entry['bucket'], entry['object_name'])
                existing_hash = stat.metadata.get('x-amz-meta-sha256')
                if existing_hash == content_hash:
                    self.logger.info(f"Skipping {url}: File unchanged (Hash Match).")
                    return False
            except S3Error:
                pass
        
        # Partitionnement
        if hasattr(self, 'execution_date') and self.execution_date:
            try:
                y, m, d = self.execution_date.split('-')
                date_path = f"year={y}/month={m}/day={d}"
            except ValueError:
                date_path = f"year={now.year:04d}/month={now.month:02d}/day={now.day:02d}"
        else:
            date_path = f"year={now.year:04d}/month={now.month:02d}/day={now.day:02d}"
            
        storage_path = f"{domain_prefix}/{date_path}/{file_name}"
        object_name = storage_path
        
        # Gestion des collisions si le fichier existe DANS LE MEME DOSSIER mais est différent
        try:
            stat = self.client.stat_object(bucket_name, object_name)
            existing_hash = stat.metadata.get('x-amz-meta-sha256')
            if existing_hash and existing_hash != content_hash:
                parsed_url = urlparse(url)
                decoded_path = unquote(parsed_url.path)
                path_parts = [p for p in decoded_path.split('/') if p][:-1]
                if path_parts:
                    dir_prefix = "_".join(path_parts)
                    file_name = f"{dir_prefix}_{file_name}"
                    storage_path = f"{domain_prefix}/{date_path}/{file_name}"
                    object_name = storage_path
        except S3Error:
            pass

        def safe_quote(text: str, max_length: int = 300) -> str:
            if not text: return ""
            encoded = quote(text)
            return encoded[:max_length] if len(encoded) > max_length else encoded

        s3_metadata = {
            "source_system": university_id,
            "source_url": safe_quote(url, 400),
            "crawl_timestamp": now.isoformat() + "Z",
            "http_status": str(http_status),
            "content_type": content_type,
            "sha256": content_hash,
            "server_etag": safe_quote(server_etag, 200),
            "server_last_modified": safe_quote(server_last_modified, 200),
            "depth": str(depth),
            "parent_url": safe_quote(parent_url, 400),
            "storage_bucket": bucket_name,
            "storage_path": safe_quote(storage_path, 400),
            "file_name": safe_quote(file_name, 200),
            "file_size_bytes": str(len(content))
        }
        
        json_metadata = {
            "source_system": university_id,
            "source_url": url,
            "crawl_timestamp": now.isoformat() + "Z",
            "http_status": str(http_status),
            "content_type": content_type,
            "sha256": content_hash,
            "server_etag": server_etag,
            "server_last_modified": server_last_modified,
            "depth": str(depth),
            "parent_url": parent_url,
            "storage_bucket": bucket_name,
            "storage_path": storage_path,
            "file_name": file_name,
            "file_size_bytes": len(content)
        }
        
        content_stream = BytesIO(content)
        self.client.put_object(
            bucket_name,
            object_name,
            content_stream,
            length=len(content),
            content_type=content_type,
            metadata=s3_metadata
        )
        self.logger.info(f"Uploaded {url} to {bucket_name}/{object_name}")
        
        # Mise à jour de l'index en mémoire
        self.memory_index[file_name] = {"bucket": bucket_name, "object_name": object_name}
        
        base_name = file_name.rsplit('.', 1)[0] if '.' in file_name else file_name
        metadata_file_name = f"{base_name}_metadata.json"
        metadata_path = f"{domain_prefix}/{date_path}/{metadata_file_name}"
        
        metadata_content = json.dumps(json_metadata, indent=2, ensure_ascii=False).encode('utf-8')
        metadata_stream = BytesIO(metadata_content)
        self.client.put_object(
            bucket_name,
            metadata_path,
            metadata_stream,
            length=len(metadata_content),
            content_type="application/json"
        )
        self.logger.info(f"Metadata saved: {bucket_name}/{metadata_path}")
        
        return True
