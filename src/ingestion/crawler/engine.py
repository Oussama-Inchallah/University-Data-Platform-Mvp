"""
Moteur de Crawling (Ingestion Web)
----------------------------------
Ce module contient le crawler principal basé sur BeautifulSoup/requests.
Il explore les sites web des universités (HTML, PDF, DOCX) et télécharge
les fichiers bruts pour les stocker dans la zone 'raw' de MinIO.
"""
import os
import re
import logging
import time
import json
import hashlib
import socket
from typing import List, Set, Dict, Optional, Tuple
import requests
import urllib3
from bs4 import BeautifulSoup
from collections import deque
from urllib.parse import urljoin, urlparse
from src.ingestion.crawler.storage import StorageRouter
import sys
try:
    from src.processing.core.utils.platform_logger import PlatformLogger
except ImportError:
    # Fallback si le module est appelé de l'extérieur
    pass

# ==============================================================================
# WORKAROUND DOCKER IPv6 "Network is unreachable" [Errno 101]
# Docker ne route pas l'IPv6 par défaut. Si le DNS retourne une adresse IPv6, 
# la librairie requests échoue au lieu de tester l'IPv4. 
# Ce patch force Python à n'utiliser que l'IPv4 (AF_INET) pour toutes les requêtes.
# ==============================================================================
old_getaddrinfo = socket.getaddrinfo
def new_getaddrinfo(*args, **kwargs):
    responses = old_getaddrinfo(*args, **kwargs)
    # Garder uniquement les résolutions IPv4 (socket.AF_INET = 2)
    return [response for response in responses if response[0] == socket.AF_INET]
socket.getaddrinfo = new_getaddrinfo
# ==============================================================================

from playwright.sync_api import sync_playwright

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Extensions de fichiers utiles pour le Data Lake
DOC_EXTS = {'.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.csv', '.tsv', '.odt', '.rtf', '.txt'}
MEDIA_EXTS = {'.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg', '.bmp', '.ico', '.mp4', '.mp3', '.webm', '.ogg', '.wav', '.avi'}
JUNK_EXTS = {'.css', '.js', '.woff', '.woff2', '.ttf', '.eot', '.otf', '.map'}

# Segments d'URL à ignorer (bruit, tracking, infrastructure)
EXCLUDED_URL_PATTERNS = [
    '/cdn-cgi/',           # Cloudflare internals
    '/gtag/',              # Google Tag Manager
    'googletagmanager.com',
    'google-analytics.com',
    'cloudflareinsights.com',
    'gpteng.co',           # GPT Engineer tracking
    'fonts.googleapis.com',
    'fonts.gstatic.com',
    '/maps/embed',         # Google Maps embed
    'maps.googleapis.com',
    'maps.gstatic.com',
    '/favicon',
    '/robots.txt',
    '/sitemap.xml',
]

def is_junk_url(url: str) -> bool:
    """Détermine si une URL est du bruit (tracking, fonts, analytics, etc.)"""
    # Règle anti-phrase : si l'URL contient plusieurs espaces bruts, c'est probablement une phrase copiée par erreur.
    if url.count(' ') > 2 or ' http' in url:
        return True
        
    url_lower = url.lower()
    
    # Règle anti-localhost (évite les Connection Refused à l'intérieur du conteneur Docker)
    parsed_netloc = urlparse(url_lower).netloc
    if parsed_netloc in ['localhost', '127.0.0.1'] or parsed_netloc.startswith('localhost:') or parsed_netloc.startswith('127.0.0.1:'):
        return True

    # Vérifier les patterns exclus
    for pattern in EXCLUDED_URL_PATTERNS:
        if pattern in url_lower:
            return True
    # Vérifier les extensions de fichiers indésirables
    ext = os.path.splitext(urlparse(url_lower).path)[1]
    if ext in JUNK_EXTS:
        return True
    return False

def is_useful_content_type(ct: str) -> bool:
    """Détermine si un content-type intercepté est utile pour le Data Lake."""
    if not ct:
        return False
    # Au lieu d'une whitelist restrictive, on utilise une blacklist.
    # On rejette explicitement ce qui n'est pas de la donnée brute.
    # Le HTML est traité par le flux principal (pas par l'interception).
    blacklist = ['text/html', 'text/css', 'javascript', 'font', 'x-empty']
    return not any(b in ct for b in blacklist)

def is_valid_size(content_bytes: bytes, content_type: str) -> bool:
    """Filtre les fichiers trop petits (corrompus, vides ou pixels de tracking)"""
    size = len(content_bytes)
    ct = content_type.lower()
    if "pdf" in ct or "word" in ct or "officedocument" in ct:
        return size > 5120  # > 5 KB
    elif "image" in ct:
        return size > 15360 # > 15 KB
    else:
        return size > 100   # > 100 bytes pour HTML/JSON

class DynamicFetcher:
    def __init__(self, user_agent: str = "Mozilla/5.0", request_timeout: int = 30, retry_count: int = 3):
        self.logger = logging.getLogger(__name__)
        self.user_agent = user_agent
        self.request_timeout = request_timeout
        self.retry_count = retry_count

    def fetch(self, url: str, is_allowed_domain: bool = True, on_resource=None, storage_router=None) -> tuple[bytes, str, str, str, str]:
        self.logger.info(f"Fetching: {url}")
        headers = {"User-Agent": self.user_agent}
        last_error = None
        
        # Ajustement du timeout pour les liens externes, mais on garde les retries
        # car le Fail Fast arrêtera de toute façon les domaines morts dès la 1ère tentative.
        current_timeout = self.request_timeout if is_allowed_domain else 10
        current_retries = self.retry_count
        
        for attempt in range(current_retries):
            try:
                with requests.get(url, headers=headers, stream=True, timeout=current_timeout, verify=False) as response:
                    response.raise_for_status()
                    
                    content_type = response.headers.get('Content-Type', 'text/html').lower()
                    
                    # Filtre Universel Anti-Débordement : 
                    # Si c'est un lien vers un domaine externe ET que c'est une page Web (HTML), on l'ignore.
                    # Ainsi, on récupère TOUS les documents externes (S3, Supabase) sans crawler d'autres sites.
                    if not is_allowed_domain and 'text/html' in content_type:
                        self.logger.debug(f"Ignoring external HTML page: {url}")
                        return None, None, None, None, None
                        
                    etag = response.headers.get('ETag', '').strip('"')
                    last_modified = response.headers.get('Last-Modified', '')
                    final_url = response.url
                    
                    # Logique de SKIP pour les fichiers binaires (stream=True ne télécharge pas encore le body)
                    if 'text/html' not in content_type and storage_router:
                        existing_meta = storage_router.get_existing_metadata(final_url, content_type)
                        if etag and existing_meta.get('server_etag') == etag:
                            self.logger.info(f"Skipping download for {url} (ETag match).")
                            return b"SKIPPED", content_type, final_url, etag, last_modified
                            
                    # Limite de taille (100 MB)
                    max_size = 100 * 1024 * 1024
                    content_length_str = response.headers.get('Content-Length')
                    if content_length_str and content_length_str.isdigit():
                        if int(content_length_str) > max_size:
                            self.logger.warning(f"File too large ({content_length_str} bytes), skipping: {url}")
                            return b"SKIPPED_SIZE", content_type, final_url, etag, last_modified
                            
                    content_chunks = []
                    downloaded_size = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            content_chunks.append(chunk)
                            downloaded_size += len(chunk)
                            if downloaded_size > max_size:
                                self.logger.warning(f"File exceeded max size ({max_size} bytes) during stream, skipping: {url}")
                                return b"SKIPPED_SIZE", content_type, final_url, etag, last_modified
                                
                    content = b"".join(content_chunks)
                    
                    if not is_valid_size(content, content_type):
                        self.logger.warning(f"File too small ({len(content)} bytes) for {content_type}, skipping: {url}")
                        return b"SKIPPED_SIZE", content_type, final_url, etag, last_modified
                
                # Si c'est un document (pdf, doc, img), on n'a pas besoin de Playwright
                if 'text/html' not in content_type:
                    return content, content_type, final_url, etag, last_modified
                
                if self._needs_playwright(content, content_type, url):
                    self.logger.info(f"Dynamic content detected for {url}. Using Playwright.")
                    c, ct, furl = self._fetch_playwright(url, on_resource)
                    return c, ct, furl, etag, last_modified
                
                return content, content_type, final_url, etag, last_modified
                
            except requests.exceptions.HTTPError as e:
                # Si c'est une 404 (Lien mort), ça ne sert à rien de réessayer ou de lancer Playwright
                if e.response is not None and e.response.status_code == 404:
                    self.logger.warning(f"Broken link (404 Not Found) for {url}. Skipping.")
                    return None, None, None, None, None
                last_error = e
                self.logger.warning(f"Attempt {attempt+1}/{current_retries} failed for {url} via Requests (HTTPError): {e}")
            except requests.exceptions.RequestException as e:
                error_str = str(e)
                # Fail Fast : Si le serveur n'existe plus ou que le réseau est coupé vers cette IP, 
                # on abandonne immédiatement pour économiser 3 retries et 1 fallback Playwright.
                if "NameResolutionError" in error_str or "Network is unreachable" in error_str:
                    self.logger.warning(f"Fast failing on deep network error for {url}: {error_str}")
                    return None, None, None, None, None
                    
                last_error = e
                self.logger.warning(f"Attempt {attempt+1}/{current_retries} failed for {url} via Requests: {e}")
                
        # Si on a épuisé les retries en Requests, on tente une dernière fois via Playwright comme fallback
        # MAIS uniquement pour les domaines internes ! Pour les liens externes (S3, autres sites), on abandonne.
        if not is_allowed_domain:
            self.logger.warning(f"All requests retries failed for external URL {url}. Skipping Playwright fallback.")
            return None, None, None, None, None
            
        self.logger.warning(f"All requests retries failed for {url}. Falling back to Playwright.")
        try:
            c, ct, furl = self._fetch_playwright(url, on_resource)
            return c, ct, furl, "", ""
        except Exception as e:
            self.logger.error(f"Playwright fallback also failed for {url}: {e}")
            raise last_error or e

    def _needs_playwright(self, content: bytes, content_type: str, url: str = "") -> bool:
        if 'text/html' not in content_type:
            return False
            
        # Règle 1 : S'il y a un fragment dans l'URL (ex: annonce.html#1184), 
        # c'est obligatoirement une route SPA ou un ancrage dynamique.
        if '#' in url:
            return True
            
        try:
            text = content.decode('utf-8', errors='ignore')
            
            # Règle 2 : Détection de requêtes asynchrones pour le rendu de la page
            import re
            if re.search(r'\$\.ajax|fetch\(|\$\(document\)\.ready|XMLHttpRequest', text):
                return True
                
            soup = BeautifulSoup(text, 'html.parser')
            if soup.find('noscript', string=lambda t: t and 'JavaScript' in t):
                return True
                
            body = soup.find('body')
            # Règle 3 : Framework JS pur (body absent ou presque vide)
            if not body: return True
            if len(body.get_text(strip=True)) < 50 and soup.find_all('script'):
                return True
                
            # Règle 4 : Conteneurs classiques de rendu (React/Vue/Angular)
            if soup.find(id=re.compile(r'^(root|app|__next|nuxt-app)$')): return True
        except Exception:
            pass
            
        return False
        
    def _fetch_playwright(self, url: str, on_resource=None) -> tuple[bytes, str, str]:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(ignore_https_errors=True, user_agent=self.user_agent)
            page = context.new_page()
            
            # Définition du handler d'interception (filtre intelligent)
            def handle_response(response):
                if not on_resource: return
                try:
                    # Ignorer les redirections et les erreurs
                    if response.status < 200 or response.status >= 400: return
                    if 300 <= response.status < 400: return
                    
                    # Ignorer les URLs de bruit
                    if is_junk_url(response.url): return
                    
                    ct = response.headers.get('content-type', '').lower()
                    
                    # Ne capturer QUE les contenus utiles (JSON API, documents, images, médias)
                    if is_useful_content_type(ct):
                        body = response.body()
                        if is_valid_size(body, ct):
                            on_resource(response.url, body, ct)
                except Exception:
                    pass
            
            page.on('response', handle_response)
            
            try:
                # 1. On garantit d'abord que le document HTML de base est complètement chargé (document.readyState === "complete")
                response = page.goto(url, wait_until='load', timeout=15000)
                
                # 2. networkidle est crucial pour les SPAs (React) afin que les fetch() API aient le temps de s'exécuter.
                # On accorde 8 secondes supplémentaires au réseau pour se calmer.
                page.wait_for_load_state('networkidle', timeout=8000)
            except Exception as e:
                # Si networkidle timeout (ex: websocket ouvert ou polling infini),
                # on continue quand même. Le DOM de base est déjà garanti d'être chargé.
                self.logger.info(f"SPA background connections still active for {url}. Proceeding with loaded DOM.")
                response = None
                
            # --- NOUVEAU : Auto-Scroll (Limité) ---
            try:
                max_scrolls = 50
                for _ in range(max_scrolls):
                    previous_height = page.evaluate("document.body.scrollHeight")
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(500) # Wait 500ms for lazy load
                    new_height = page.evaluate("document.body.scrollHeight")
                    if new_height == previous_height:
                        break # Reached the bottom
            except Exception as e:
                self.logger.warning(f"Error during auto-scroll on {url}: {e}")
            # --------------------------------------
            
            # Attendre un court instant supplémentaire pour laisser le rendu React se stabiliser si besoin
            page.wait_for_timeout(2000)
            html = page.content()
            final_url = page.url
            browser.close()
            return html.encode('utf-8'), 'text/html', final_url

class GenericCrawler:
    def __init__(self, seed_url: str, allowed_domains: list, max_depth: int, university_id: str,
                 crawl_delay: float = 1.0, request_timeout: int = 30, 
                 user_agent: str = "Mozilla/5.0", retry_count: int = 3, execution_date: str = None):
        self.seed_url = seed_url
        self.allowed_domains = allowed_domains
        self.max_depth = max_depth
        self.crawl_delay = crawl_delay
        self.university_id = university_id
        
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
        
        self.fetcher = DynamicFetcher(user_agent=user_agent, request_timeout=request_timeout, retry_count=retry_count)
        self.storage_router = StorageRouter(execution_date=execution_date)
        
    def _is_allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        return any(domain in parsed.netloc for domain in self.allowed_domains)

    def extract_links(self, html_content: bytes, base_url: str) -> list:
        soup = BeautifulSoup(html_content, 'html.parser')
        links = []
        
        def process_url(tag_url, is_resource=False):
            if not tag_url or tag_url.startswith(('data:', 'javascript:', 'mailto:', 'tel:')): 
                return
                
            # Nettoyer l'URL : on encode les espaces isolés (ex: "document final.pdf" -> "document%20final.pdf")
            # pour éviter que 'requests' ne plante (InvalidURL / Failed to parse).
            tag_url = tag_url.strip().replace(' ', '%20')
                
            # Les applications SPA modernes (React, Angular, Vue) utilisent souvent des fragments (#) 
            # pour la navigation (ex: annonce.html#1181). Il ne faut surtout pas supprimer le '#'.
            full_url = urljoin(base_url, tag_url).rstrip('?')
            if not full_url.startswith(('http://', 'https://')): 
                return
            
            # Filtrer le bruit dès la phase de découverte
            if is_junk_url(full_url):
                return
            
            is_internal = self._is_allowed(full_url)
            
            # Universal Crawler Logic: On ajoute TOUT à la file d'attente.
            # Le filtrage intelligent (ignorer le HTML externe) est géré dans fetcher.fetch()
            # grâce à un `HEAD`/`stream=True` rapide pour vérifier le Content-Type.
            links.append((full_url, is_resource))

        # 1. Liens standards (<a href>)
        for a_tag in soup.find_all('a', href=True):
            process_url(a_tag['href'], is_resource=False)
            
        # 2. Balises de ressources utiles (PAS script/link qui sont du CSS/JS)
        resource_tags = [
            ('img', 'src'), ('iframe', 'src'), ('embed', 'src'), ('source', 'src'),
            ('video', 'src'), ('audio', 'src'), ('object', 'data')
        ]
        for tag, attr in resource_tags:
            for el in soup.find_all(tag, **{attr: True}):
                process_url(el[attr], is_resource=True)
                
        # 3. Attributs data-* et onclick qui ressemblent à des URLs utiles
        # Regex pour trouver une URL absolue (http...) ou un chemin relatif vers un fichier
        url_pattern = re.compile(r'(https?://[^\s"\'\\,\[\]{}<>]+|/[a-zA-Z0-9_\-\./]+\.(?:pdf|doc|docx|xls|xlsx|ppt|pptx|png|jpg|jpeg|json))', re.IGNORECASE)
        for el in soup.find_all(True):
            for attr, value in el.attrs.items():
                if isinstance(value, str) and ('url' in attr or 'href' in attr or 'link' in attr or attr == 'onclick' or attr.startswith('data-')):
                    matches = url_pattern.findall(value)
                    for match in matches:
                        if not is_junk_url(match):
                            process_url(match, is_resource=True)

        # Déduplication tout en gardant l'ordre
        seen = set()
        deduped = []
        for url, is_res in links:
            if url not in seen:
                seen.add(url)
                deduped.append((url, is_res))
        return deduped

    def _extract_urls_from_json(self, content: bytes, parent_url: str, depth: int, node_id: str, queue: deque, visited: set):
        try:
            content_str = content.decode('utf-8', errors='ignore')
            urls_in_json = re.findall(r'(https?://[^\s"\'\\,\[\]{}<>]+|/[a-zA-Z0-9_\-\./]+\.(?:pdf|doc|docx|xls|xlsx|ppt|pptx|png|jpg|jpeg|json))', content_str, re.IGNORECASE)
            for idx, raw_url in enumerate(urls_in_json):
                new_url = urljoin(parent_url, raw_url)
                if new_url in visited:
                    continue
                # On met en file d'attente TOUTES les URLs du JSON.
                # fetcher.fetch() se chargera de rejeter les pages HTML externes.
                new_node_id = f"{node_id}_json_{idx}"
                queue.append((new_url, depth + 1, new_node_id, parent_url))
        except Exception as e:
            self.logger.warning(f"Error parsing JSON for URLs from {parent_url}: {e}")

    def start_crawl(self):
        self.logger.info(f"Starting crawl for {self.university_id} at {self.seed_url}")
        
        queue = deque([(self.seed_url, 0, "link_0", "")])
        visited = set()
        
        stats = {
            "pages_crawled": 0,
            "intercepted_resources": 0,
            "errors": 0,
            "ignored": 0,
            "downloaded_by_bucket": {},
            "skipped_by_hash": 0,
            "logs": [],
            "urls_crawled": [],
            "urls_intercepted": [],
            "urls_ignored": [],
            "urls_skipped": [],
            "urls_errors": []
        }
        
        import time
        import json
        
        domain_prefix = urlparse(self.seed_url).netloc
        
        # Construire l'index mémoire des fichiers existants dans MinIO
        self.storage_router.build_memory_index(domain_prefix, ["raw-web-html", "raw-documents", "raw-images", "raw-json"])
        
        while queue:
            url, depth, node_id, parent_url = queue.popleft()
            
            if url in visited or depth > self.max_depth:
                stats["ignored"] += 1
                stats["urls_ignored"].append(url)
                continue
            
            # Filtrer le bruit AVANT de consommer un slot
            if is_junk_url(url):
                self.logger.debug(f"Skipping junk URL: {url}")
                stats["ignored"] += 1
                stats["urls_ignored"].append(url)
                continue
                
            visited.add(url)
            
            def on_intercepted_resource(res_url, res_content, res_content_type):
                if res_url in visited: return
                visited.add(res_url)
                try:
                    res_uploaded = self.storage_router.upload_to_minio(
                        url=res_url,
                        content=res_content,
                        content_type=res_content_type,
                        node_id=f"intercepted_{len(visited)}",
                        university_id=self.university_id,
                        domain_prefix=domain_prefix,
                        depth=depth,
                        parent_url=url,
                        http_status=200
                    )
                    stats["intercepted_resources"] += 1
                    stats["urls_intercepted"].append(res_url)
                    stats["logs"].append({"url": res_url, "status": "success (intercepted)", "type": res_content_type})
                    res_bucket = self.storage_router._get_bucket_by_type(res_content_type, res_url)
                    if not res_uploaded:
                        stats["skipped_by_hash"] += 1
                        stats["urls_skipped"].append(res_url)
                    else:
                        if res_bucket not in stats["downloaded_by_bucket"]:
                            stats["downloaded_by_bucket"][res_bucket] = 0
                        stats["downloaded_by_bucket"][res_bucket] += 1
                        
                    # Extraction des PDFs cachés dans le JSON
                    if depth < self.max_depth and ('json' in res_content_type.lower()):
                        self._extract_urls_from_json(res_content, res_url, depth, f"intercept_{len(visited)}", queue, visited)

                except Exception as e:
                    self.logger.error(f"Error saving intercepted resource {res_url}: {e}")
                    stats["errors"] += 1
                    stats["urls_errors"].append(res_url)
                    stats["logs"].append({"url": res_url, "status": "error (intercepted)", "error": str(e)})

            try:
                if stats["pages_crawled"] > 0:
                    time.sleep(self.crawl_delay)
                    
                is_internal = self._is_allowed(url)
                res = self.fetcher.fetch(url, is_allowed_domain=is_internal, on_resource=on_intercepted_resource, storage_router=self.storage_router)
                if res[0] is None:
                    stats["ignored"] += 1
                    stats["urls_ignored"].append(url)
                    continue
                content, content_type, final_url, etag, last_modified = res
                
                if content == b"SKIPPED":
                    uploaded = False
                    content = b"" # Reset for extract_links safety although we won't extract links from binaries
                else:
                    uploaded = self.storage_router.upload_to_minio(
                        url=final_url, 
                        content=content, 
                        content_type=content_type, 
                        node_id=node_id, 
                        university_id=self.university_id,
                        domain_prefix=domain_prefix,
                        depth=depth,
                        parent_url=parent_url,
                        http_status=200,
                        server_etag=etag,
                        server_last_modified=last_modified
                    )
                
                stats["pages_crawled"] += 1
                stats["urls_crawled"].append(url)
                stats["logs"].append({"url": url, "status": "success", "type": content_type})
                
                # Mise à jour des stats par bucket
                bucket_name = self.storage_router._get_bucket_by_type(content_type, url)
                if not uploaded:
                    stats["skipped_by_hash"] += 1
                    stats["urls_skipped"].append(url)
                else:
                    if bucket_name not in stats["downloaded_by_bucket"]:
                        stats["downloaded_by_bucket"][bucket_name] = 0
                    stats["downloaded_by_bucket"][bucket_name] += 1
                
                if depth < self.max_depth:
                    if 'text/html' in content_type and self._is_allowed(final_url):
                        links = self.extract_links(content, final_url)
                        for idx, (new_url, is_res) in enumerate(links, 1):
                            if new_url not in visited:
                                new_node_id = f"{node_id}_{idx}"
                                queue.append((new_url, depth + 1, new_node_id, url))
                    elif 'json' in content_type.lower():
                        self._extract_urls_from_json(content, final_url, depth, node_id, queue, visited)
                            
                            
            except Exception as e:
                self.logger.error(f"Error processing {url}: {e}")
                stats["errors"] += 1
                stats["urls_errors"].append(url)
                stats["logs"].append({"url": url, "status": "error", "error": str(e)})
                if depth == 0:
                    self.logger.critical("Seed URL failed. Aborting crawler.")
                    break
        
        self.logger.info("==================================================")
        self.logger.info("                  URL DETAILS                     ")
        self.logger.info("==================================================")
        
        self.logger.info(f"--- PAGES CRAWLED ({len(stats['urls_crawled'])}) ---")
        for u in stats["urls_crawled"]:
            self.logger.info(f"  [CRAWLED] {u}")
            
        self.logger.info(f"--- INTERCEPTED RESOURCES ({len(stats['urls_intercepted'])}) ---")
        for u in stats["urls_intercepted"]:
            self.logger.info(f"  [INTERCEPTED] {u}")
            
        self.logger.info(f"--- IGNORED / FILTERED ({len(stats['urls_ignored'])}) ---")
        for u in stats["urls_ignored"]:
            self.logger.info(f"  [IGNORED] {u}")
            
        self.logger.info(f"--- SKIPPED (HASH MATCH) ({len(stats['urls_skipped'])}) ---")
        for u in stats["urls_skipped"]:
            self.logger.info(f"  [SKIPPED] {u}")
            
        self.logger.info(f"--- ERRORS ENCOUNTERED ({len(stats['urls_errors'])}) ---")
        for u in stats["urls_errors"]:
            self.logger.info(f"  [ERROR] {u}")
            
        self.logger.info("==================================================")

        # Affichage des statistiques dans les logs Airflow sous forme de table
        total = stats['pages_crawled'] + stats['intercepted_resources']
        
        table_border = "+" + "-"*35 + "+" + "-"*12 + "+"
        self.logger.info("")
        self.logger.info(table_border)
        self.logger.info(f"| {'CRAWL STATISTICS':<33} | {'COUNT':<10} |")
        self.logger.info(table_border)
        self.logger.info(f"| {'Pages crawled':<33} | {stats['pages_crawled']:<10} |")
        self.logger.info(f"| {'Intercepted resources':<33} | {stats['intercepted_resources']:<10} |")
        self.logger.info(f"| {'Total objects stored':<33} | {total:<10} |")
        self.logger.info(f"| {'Ignored/Filtered':<33} | {stats['ignored']:<10} |")
        self.logger.info(f"| {'Skipped (Hash Match)':<33} | {stats['skipped_by_hash']:<10} |")
        self.logger.info(f"| {'Errors encountered':<33} | {stats['errors']:<10} |")
        self.logger.info(table_border)
        self.logger.info("Downloads by destination bucket:")
        for bucket, count in stats["downloaded_by_bucket"].items():
            self.logger.info(f"  -> {bucket}: {count} objects")
        self.logger.info("==================================================")
        
        # Enregistrement des logs en fin d'exécution
        import datetime
        run_timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        log_content = json.dumps(stats, indent=2).encode('utf-8')
        log_url = f"{self.seed_url}/crawl_stats_{run_timestamp}.json"
        self.storage_router.upload_to_minio(
            url=log_url,
            content=log_content,
            content_type="application/json",
            node_id="crawl_stats",
            university_id=self.university_id,
            domain_prefix=domain_prefix,
            depth=0,
            parent_url="",
            http_status=200,
            force_bucket="raw-logs"
        )
        self.logger.info("Stats uploaded to raw-logs.")
        
        # --- Platform Observability Logging ---
        job_name = f"scraper_{self.university_id.lower()}"
        status = "SUCCESS" if stats["errors"] == 0 else "FAILED"
        error_msg = f"{stats['errors']} errors encountered" if stats["errors"] > 0 else None
        
        try:
            pl = PlatformLogger()
            pl.log_job_status(job_name, status, error_msg)
        except Exception as e:
            self.logger.warning(f"Could not push to PlatformLogger: {e}")

