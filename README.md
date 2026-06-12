# University Data Platform MVP

Projet MVP de Data Engineering permettant l'ingestion, le traitement et l'analyse de données universitaires.

## Installation rapide

1. **Cloner le projet :**
  
   ```bash
   git clone https://github.com/Oussama-Inchallah/University-Data-Platform-Mvp.git
   cd University-Data-Platform-Mvp
   ```

2. **Démarrer l'infrastructure (Airflow, MinIO, Postgres, Redis) :**

   Nous utilisons des profils Docker Compose pour gérer les différentes couches de la plateforme. Pour lancer l'infrastructure d'ingestion complète :

   ```bash
   cd infra/docker
   # Lancer l'infrastructure avec la couche d'ingestion
   docker-compose --profile ingestion up -d --build
   ```

   Airflow sera disponible sur `localhost:8080` et MinIO sur `localhost:9001`.

## Architecture du Projet & Structure des Dossiers

Voici un aperçu de la structure du projet après la consolidation de la plateforme de données et de l'orchestrateur :

```text
University-Data-Platform-Mvp/
├── infra/                  # Infrastructure as Code
│   ├── docker/
│   │   ├── docker-compose.yml   # Fichier central avec Profils Docker (ingestion, processing, etc.)
│   │   ├── Dockerfile           # Build custom d'Airflow avec Playwright
│   │   ├── requirements.txt     # Dépendances Python pour l'image Airflow
│   │   └── .env                 # Variables d'environnement
│   ├── elastic_mappings/        # Index Elasticsearch
│   ├── hive_ddl/                # Schémas Hive Metastore pour Hudi
│   └── metabase_dashboards/     # Configurations de tableaux de bord Metabase
│
├── dags/                   # Orchestration Centrale (Airflow)
│   ├── ingestion/          # DAGs de crawling et de collecte d'API
│   ├── processing/         # Futurs DAGs pour déclencher Hudi/Spark
│   └── serving/            # Futurs DAGs pour l'exportation vers Elastic/Metabase
│
├── plugins/                # Code Custom Airflow
│   └── crawler/            # Le moteur Python Crawler Universel
│
├── src/                    # Logique Métier Indépendante (Agnostique d'Airflow)
│   ├── processing/         # Scripts PySpark (ex: hudi_utils.py)
│   ├── common/             # Utilitaires partagés
│   └── serving/            # Scripts de chargement de données finales
│
├── tests/                  # Tests unitaires et d'intégration
│   ├── test_ingestion/
│   └── test_processing/
│
├── docs/                   # Documentation globale
│   ├── openAlexAPI/
│   └── screenshots/        # Captures d'écran (architecture, ingestion, processing)
│
├── scripts/                # Scripts utilitaires Bash/PowerShell
```
