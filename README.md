# University Data Platform MVP

Projet MVP de Data Engineering permettant l'ingestion, le traitement et l'analyse de données universitaires.

## Installation rapide

1. **Cloner le projet :**
  
   ```bash
   git clone https://github.com/Oussama-Inchallah/University-Data-Platform-Mvp.git
   cd University-Data-Platform-Mvp
   ```

2. **Configurer l'environnement :**

   Il faut créer un fichier `.env` contenant les variables secrètes à partir de l'exemple :

   ```bash
   cd infra/docker
   cp .env.example .env
   # Pensez à éditer le fichier .env si vous souhaitez changer les mots de passe par défaut
   ```

3. **Démarrer l'infrastructure (Airflow, MinIO, Postgres, Redis) :**

   ```bash
   # Lancer linfrastructure globale
   docker compose up -d --build
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
│   ├── dag_fsac.py         # Pipeline End-to-End pour FSAC
│   ├── dag_estc.py         # Pipeline End-to-End pour ESTC
│   └── ...                 # Un DAG par source
│
├── src/                    # Logique Métier Indépendante (Agnostique d'Airflow)
│   ├── ingestion/          # Logique d'extraction (Crawler, APIs)
│   │   ├── crawler/        # Le moteur Python Crawler Universel
│   │   └── openalex/       # Connecteur API OpenAlex
│   ├── processing/         # Scripts PySpark (Couche Silver)
│   ├── common/             # Utilitaires partagés
│   └── serving/            # Scripts de chargement de données finales
│
├── tests/                  # Tests unitaires et d'intégration
│   ├── test_ingestion/
│   └── test_processing/
│
├── docs/                   # Documentation globale
│   └── screenshots/        # Captures d'écran (architecture, ingestion, processing)
│
├── scripts/                # Scripts utilitaires Bash/PowerShell
```
