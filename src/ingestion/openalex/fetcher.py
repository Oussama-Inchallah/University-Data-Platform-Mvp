# ==============================================================================
# © 2026 University Data Platform
# Ce fichier fait partie du système central de la Plateforme Data Universitaire.
# ==============================================================================

"""
Extracteur de données OpenAlex (API)
------------------------------------
Ce module interroge l'API publique OpenAlex pour récupérer les publications
scientifiques et les travaux de recherche associés à une université.
"""
import logging
import time
import requests
from urllib.parse import urlencode
from src.ingestion.crawler.storage import StorageRouter
from src.processing.core.utils.platform_logger import PlatformLogger

class OpenAlexFetcher:
    """
    Ingestion component for OpenAlex API.
    Handles cursor pagination and delegates robust storage to StorageRouter.
    """
    BASE_URL = "https://api.openalex.org/works"

    def __init__(self, per_page: int = 200, max_retries: int = 3, execution_date: str = None):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
        
        self.per_page = per_page
        self.max_retries = max_retries
        self.storage = StorageRouter(execution_date=execution_date)
        self.platform_logger = PlatformLogger()

    def fetch_institution_works(self, institution_id: str, institution_name: str):
        """
        Fetches all works for a given institution using cursor pagination.
        Saves each API page as a separate JSON file in MinIO using the institution's name.
        Saves each API page as a separate JSON file in MinIO.
        """
        cursor = "*"
        page_num = 1
        total_works_saved = 0
        
        self.logger.info(f"Starting OpenAlex ingestion for institution: {institution_id}")
        
        # Build memory index to prevent redownloading existing files in future DAG runs
        self.storage.build_memory_index(f"openalex/{institution_name}", ["raw-json"])
        
        while cursor:
            params = {
                "filter": f"institutions.id:{institution_id}",
                "per-page": self.per_page,
                "cursor": cursor
            }
                
            query_string = urlencode(params)
            url = f"{self.BASE_URL}?{query_string}"
            
            self.logger.info(f"Fetching page {page_num}...")
            
            # Request with retry logic
            response_json, raw_bytes, status_code = self._make_request(url)
            
            if not response_json:
                error_msg = f"Failed to fetch page {page_num} for {institution_name} after {self.max_retries} retries. Network Unreachable."
                self.logger.error(error_msg)
                self.platform_logger.log_job_status(f"ingest_openalex_{institution_name}", "FAILED", error_msg)
                raise ConnectionError(error_msg)
                
            # Check for results
            results = response_json.get("results", [])
            count = len(results)
            
            if count == 0:
                self.logger.info("No more results found. Pagination complete.")
                break
                
            total_works_saved += count
            
            # Save the raw JSON chunk via StorageRouter
            # Virtual URL so StorageRouter names the file professionally: works_page_0001.json
            virtual_url = f"https://api.openalex.org/institutions/{institution_id}/works_page_{page_num:04d}.json"
            
            self.storage.upload_to_minio(
                url=virtual_url,
                content=raw_bytes,
                content_type="application/json",
                node_id="openalex-api",
                university_id=institution_name,
                domain_prefix=f"openalex/{institution_name}",
                depth=0,
                parent_url=f"https://openalex.org/{institution_id}",
                http_status=status_code
            )
            
            # Get next cursor
            next_cursor = response_json.get("meta", {}).get("next_cursor")
            
            if next_cursor == cursor or not next_cursor:
                # End of pagination
                break
                
            cursor = next_cursor
            page_num += 1
            
            # Be polite to the API
            time.sleep(1.0)
            
        self.logger.info(f"Finished. Total works saved: {total_works_saved} across {page_num} pages.")
        self.platform_logger.log_job_status(f"ingest_openalex_{institution_name}", "SUCCESS")

    def _make_request(self, url: str):
        headers = {"User-Agent": "University-Data-Platform/1.0"}
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, headers=headers, timeout=30)
                
                # Handle Rate Limiting gracefully
                if response.status_code == 429:
                    wait_time = 2 ** attempt
                    self.logger.warning(f"Rate limited (429). Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                    
                response.raise_for_status()
                return response.json(), response.content, response.status_code
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Attempt {attempt+1}/{self.max_retries} failed: {e}")
                time.sleep(2 ** attempt)
                
        return None, None, 500
