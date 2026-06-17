import hashlib
import logging
from io import BytesIO
from minio import Minio
from minio.error import S3Error
from datetime import datetime
from urllib.parse import urlparse, quote

class StorageRouter:
    def __init__(self, endpoint="minio:9000", access_key="minioadmin", secret_key="minioadmin"):
        self.logger = logging.getLogger(__name__)
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        
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
        
    def upload_to_minio(self, url: str, content: bytes, content_type: str, node_id: str, university_id: str, domain_prefix: str, depth: int = 0, parent_url: str = "", http_status: int = 200, force_bucket: str = None):
        bucket_name = force_bucket if force_bucket else self._get_bucket_by_type(content_type, url)
        
        parsed_url = urlparse(url)
        now = datetime.utcnow()
        
        from urllib.parse import unquote
        # On décode l'URL pour transformer les %D8%A5 en caractères arabes réels
        decoded_path = unquote(parsed_url.path)
        
        # Extraction stricte du nom du fichier depuis le chemin
        file_name = decoded_path.split('/')[-1]
        
        if not file_name:
            # L'URL se termine par un slash (ex: /biologie/ ou /img/)
            path_parts_full = [p for p in decoded_path.split('/') if p]
            
            import mimetypes
            # Deviner l'extension d'après le vrai Content-Type (ex: image/jpeg -> .jpg)
            ext = mimetypes.guess_extension(content_type.split(';')[0]) or ""
            
            if 'text/html' in content_type:
                suffix = ".html"
            else:
                # Si ce n'est pas du HTML, on utilise l'extension devinée (ex: .jpg, .pdf)
                # Si mimetypes ne trouve rien et que l'URL n'a pas d'extension, on ajoute juste le suffixe vide.
                suffix = ext if ext else ""
                
            if path_parts_full:
                file_name = f"{path_parts_full[-1]}{suffix}"
            else:
                file_name = f"index{suffix}" if suffix else "unknown_file"
            
        # Ajout systématique d'un petit hash (10 caractères) basé sur l'URL complète.
        # Cela garantit qu'aucune page n'écrase une autre, même si elles ont le même nom 
        # (ex: bd2c.fsac.ma/ et isi.fsac.ma/ donneront index-index_a1b2c3d4e5.html)
        import hashlib
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:10]
        
        if '.' in file_name:
            base_name, ext = file_name.rsplit('.', 1)
            file_name = f"{base_name}_{url_hash}.{ext}"
        else:
            file_name = f"{file_name}_{url_hash}"

        # Si c'est du HTML et qu'il n'y a pas d'extension, on s'assure qu'il finit par .html
        if 'text/html' in content_type and not file_name.endswith('.html'):
            file_name += ".html"
            
        # TRONCATURE SÉCURITÉ OS : Le nom de fichier ne doit pas dépasser 255 octets (limite Linux Ext4/NTFS).
        # Certains slugs CMS (ex: une phrase entière avec des tirets) dépassent cette limite.
        # S'il est trop long, on le coupe à 150 caractères et on ajoute un petit hash pour garantir l'unicité.
        file_bytes_len = len(file_name.encode('utf-8'))
        if file_bytes_len > 200:
            import hashlib
            name_hash = hashlib.md5(file_name.encode('utf-8')).hexdigest()[:8]
            if '.' in file_name:
                base_name, ext = file_name.rsplit('.', 1)
                file_name = f"{base_name[:150]}_{name_hash}.{ext}"
            else:
                file_name = f"{file_name[:150]}_{name_hash}"
        
        
        # Le storage_path correspond à la structure partitionnée (sans l'arborescence du site)
        # On utilise strictement le domain_prefix (ex: fsac.univh2c.ma) même si l'URL vient d'ailleurs
        storage_path = f"{domain_prefix}/year={now.year:04d}/month={now.month:02d}/day={now.day:02d}/{file_name}"
            
        object_name = storage_path
            
        content_hash = self.compute_sha256(content)
        
        try:
            stat = self.client.stat_object(bucket_name, object_name)
            existing_hash = stat.metadata.get('x-amz-meta-sha256')
            if existing_hash == content_hash:
                self.logger.info(f"Skipping {url}: File unchanged (Hash Match).")
                return False
            else:
                # COLLISION DE NOM DÉTECTÉE !
                # Un fichier avec ce nom existe déjà, mais le contenu est différent.
                # On ajoute le chemin des dossiers au nom de fichier pour le rendre unique (ex: MR_GL.pdf).
                path_parts = [p for p in decoded_path.split('/') if p][:-1]
                if path_parts:
                    dir_prefix = "_".join(path_parts)
                    file_name = f"{dir_prefix}_{file_name}"
                    storage_path = f"{domain_prefix}/year={now.year:04d}/month={now.month:02d}/day={now.day:02d}/{file_name}"
                    object_name = storage_path
        except S3Error as e:
            if e.code != 'NoSuchKey':
                self.logger.warning(f"Error checking MinIO stat for {object_name}: {e}")

        # Fonction utilitaire pour éviter que l'encodage URL (ex: mots arabes) ne dépasse la limite de 2KB S3
        def safe_quote(text: str, max_length: int = 300) -> str:
            encoded = quote(text)
            return encoded[:max_length] if len(encoded) > max_length else encoded

        # Contrat Strict de Métadonnées S3 (S3 exige des chaînes US-ASCII uniquement et limitées en taille)
        s3_metadata = {
            "source_system": university_id,
            "source_url": safe_quote(url, 400),
            "crawl_timestamp": now.isoformat() + "Z",
            "http_status": str(http_status),
            "content_type": content_type,
            "sha256": content_hash,
            "depth": str(depth),
            "parent_url": safe_quote(parent_url, 400),
            "storage_bucket": bucket_name,
            "storage_path": safe_quote(storage_path, 400),
            "file_name": safe_quote(file_name, 200),
            "file_size_bytes": str(len(content))
        }
        
        # Métadonnées d'origine non encodées pour le fichier JSON compagnon (UTF-8 supporté)
        json_metadata = {
            "source_system": university_id,
            "source_url": url,
            "crawl_timestamp": now.isoformat() + "Z",
            "http_status": str(http_status),
            "content_type": content_type,
            "sha256": content_hash,
            "depth": str(depth),
            "parent_url": parent_url,
            "storage_bucket": bucket_name,
            "storage_path": storage_path,
            "file_name": file_name,
            "file_size_bytes": len(content)
        }
        
        # 1. Upload du fichier principal
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
        
        # 2. Upload du fichier _metadata.json compagnon
        import json
        base_name = file_name.rsplit('.', 1)[0] if '.' in file_name else file_name
        metadata_file_name = f"{base_name}_metadata.json"
        metadata_path = f"{domain_prefix}/year={now.year:04d}/month={now.month:02d}/day={now.day:02d}/{metadata_file_name}"
        
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
