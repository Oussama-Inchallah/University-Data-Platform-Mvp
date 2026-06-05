# University Data Platform MVP

Projet MVP de Data Engineering permettant l'ingestion, le traitement et l'analyse de données universitaires.

## Installation rapide

1. **Cloner le projet :**
  
  ```bash
   git clone https://github.com/Oussama-Inchallah/University-Data-Platform-Mvp.git
   cd "University-Data-Platform-Mvp"
   ```
   
2. **Installer l'environnement Python :**

   ```bash
   python -m venv udp-env
   source udp-env/bin/activate  # Sur Windows: .\udp-env\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Démarrer l'infrastructure (MinIO) :**

   ```bash
   # Accéder au dossier
   cd infra/docker

   # Créer le fichier .env
   cp .env.example .env
   
   # Lancer Docker
   docker compose up -d

   # Revenir à la racine
   cd ../..

   ```

## Project Architecture & Directory Structure

Here is an overview of the project's folder structure, including the role of each directory and what it contains:

```text
University-Data-Platform-Mvp/
├── infra/                 # Infrastructure and platform configuration
│   ├── docker/            # Docker Compose files to spin up MinIO, Spark, Hive, Elasticsearch, etc.
│   ├── elastic_mappings/  # Elasticsearch index mappings and configurations
│   ├── hive_ddl/          # Hive Metastore table definitions (DDL) for Hudi tables
│   └── metabase_dashboards/# Exported or predefined Metabase dashboard configurations
├── screenshots/           # Images showcasing the platform's features, architecture, and UI
│   ├── architecture/      # Architecture diagrams and system design overviews
│   ├── dashboards/        # Screenshots of Metabase and other BI or monitoring dashboards
│   ├── ingestion/         # Screenshots showing data crawling, API fetching, or MinIO raw zone
│   └── processing/        # Screenshots of Spark UI, ETL logs, or Hudi curated zone
├── scripts/               # Utility scripts for executing pipelines (e.g., PowerShell run scripts)
├── src/                   # Main source code for the data platform
│   ├── common/            # Shared utilities, logging, configuration, and helpers
│   ├── ingestion/         # Ingestion layer to collect data from various sources
│   │   ├── api/           # API connectors (e.g., OpenAlex, Crossref)
│   │   ├── files/         # File parsers (PDF, CSV, JSON, documents)
│   │   └── web/           # Web scrapers (Requests, BeautifulSoup, Playwright)
│   └── processing/        # Processing and transformation layer (Apache Spark/Hudi)
│       ├── jobs/          # Spark ETL jobs (cleaning, deduplication, enrichment)
│       └── hudi_utils.py  # Utility functions for interacting with Apache Hudi
├── tests/                 # Unit and integration tests
│   ├── test_ingestion/    # Tests for ingestion scripts and scrapers
│   └── test_processing/   # Tests for processing jobs and Hudi utilities
├── requirements.txt       # Python dependencies for the project
└── udp-env/               # Local Python virtual environment (ignored in version control)
```
