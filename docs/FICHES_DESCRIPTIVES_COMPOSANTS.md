# 📋 Fiches Descriptives par Composant

---

## Fiche 1 : Ingestion des Données (Zone Bronze)

| Champ | Détail |
|-------|--------|
| **Composant** | Crawler Universel + Fetcher OpenAlex |
| **Objectif** | Collecter les données brutes depuis 12 sites universitaires et l'API OpenAlex |
| **Technologies** | Python, Playwright, Requests, MinIO (S3), Apache Airflow |
| **Fichiers principaux** | `src/ingestion/crawler/engine.py`, `src/ingestion/crawler/storage.py`, `src/ingestion/openalex/fetcher.py` |

### Description fonctionnelle
Le module d'ingestion est un **crawler web universel** capable de traiter :
- **Sites WordPress classiques** (SSR) : navigation par liens `<a href>`, téléchargement de fichiers attachés.
- **SPAs React/Supabase** (FSBM) : interception des requêtes API en arrière-plan via Playwright, attente du `networkidle`.
- **Sites avec chargement Ajax** (FSAC) : détection automatique de jQuery/fetch et activation de Playwright.
- **API REST externes** (OpenAlex) : pagination par curseur avec reprise en cas d'échec.

### Entrées / Sorties

| Entrée | Sortie |
|--------|--------|
| URLs des sites universitaires (configurées dans les DAGs) | Fichiers bruts dans MinIO (HTML, PDF, Word, JSON, images) |
| Paramètres OpenAlex (institution ID) | Publications JSON dans MinIO |

### Mécanismes de robustesse
- **ETags** : vérification avant re-téléchargement (idempotence).
- **Hash MD5** dans les noms de fichiers : anti-écrasement (Data Loss).
- **Fail Fast** : abandon immédiat sur liens morts (DNS/Socket errors).
- **Safe Quote** : gestion des URLs en arabe (encodage + troncature).
- **Anti-Localhost** : filtrage des liens `localhost` laissés par les webmasters.

### Problèmes majeurs résolus
- Incompatibilité SPA React (21 bugs corrigés, documentés dans `TROUBLESHOOTING_CRAWLER.md`)
- Certificats SSL invalides sur les sites universitaires marocains
- Écrasement de fichiers entre sous-domaines

---

## Fiche 2 : Traitement Silver (Bronze → Hudi)

| Champ | Détail |
|-------|--------|
| **Composant** | Pipeline Spark Silver |
| **Objectif** | Parser, nettoyer et structurer les données brutes en tables Apache Hudi |
| **Technologies** | Apache Spark 3.x, Apache Tika, Apache Hudi, MinIO (S3A) |
| **Fichier principal** | `src/processing/jobs/process_raw_to_hudi.py` |

### Description fonctionnelle
Le job Spark Silver :
1. **Lit** les fichiers bruts depuis MinIO (via le protocole S3A en streaming).
2. **Parse** chaque fichier avec Apache Tika (HTML, PDF, Word → texte brut).
3. **Route** chaque document vers une table cible via le fichier de configuration `universal_routing.yaml`.
4. **Écrit** dans des tables Apache Hudi avec `UPSERT` (clé primaire = `url`).

### Entrées / Sorties

| Entrée | Sortie |
|--------|--------|
| Fichiers bruts MinIO (`raw-university-data/<faculty>/`) | Tables Hudi Silver dans MinIO (`hudi-warehouse/`) |

### Caractéristiques techniques
- **Pandas UDF** pour le parsing distribué (Tika sur tous les cœurs).
- **BLOOM Index** Hudi pour une déduplication ultra-rapide.
- **Streaming fichier-par-fichier** : architecture Zero-OOM (pas de `collect()`).
- **Date logique Airflow** (`{{ ds }}`) pour éviter les décalages autour de minuit.

---

## Fiche 3 : Traitement Gold (Hudi → ES + Qdrant)

| Champ | Détail |
|-------|--------|
| **Composant** | Pipeline Spark Gold (NLP + Indexation) |
| **Objectif** | Enrichir les données Silver (NLP), chunker, vectoriser et indexer |
| **Technologies** | Spark, `sentence-transformers`, Elasticsearch, Qdrant |
| **Fichier principal** | `src/processing/jobs/process_hudi_to_search.py` |

### Description fonctionnelle
Le job Spark Gold :
1. **Lit** les tables Hudi Silver.
2. **Détecte la langue** de chaque document (FR/EN/AR).
3. **Normalise** le texte (suppression HTML, accents, ponctuation).
4. **Découpe** en chunks de taille optimale pour le RAG.
5. **Calcule les embeddings** avec `multilingual-e5-small` (Pandas UDF vectorisée).
6. **Indexe** dans Elasticsearch (texte complet + métadonnées) et Qdrant (vecteurs + payload).

### Entrées / Sorties

| Entrée | Sortie |
|--------|--------|
| Tables Hudi Silver | Index `university_content` dans Elasticsearch |
| | Collection `university_chunks` dans Qdrant |

### Caractéristiques techniques
- **Modèle multilingue** : `intfloat/multilingual-e5-small` (384 dimensions, FR/EN/AR).
- **Pandas UDF vectorisée** via Apache Arrow pour le calcul distribué des embeddings.
- **Modèle broadcasté** : chargé une seule fois et partagé entre les exécuteurs Spark.

---

## Fiche 4 : Recherche Full-Text (Elasticsearch)

| Champ | Détail |
|-------|--------|
| **Composant** | Index Elasticsearch |
| **Objectif** | Recherche par mots-clés avec scoring, highlighting et facettes |
| **Technologies** | Elasticsearch 8.11, FastAPI |
| **Endpoint API** | `GET /search/keyword` |

### Description fonctionnelle
Le moteur Elasticsearch offre :
- **Recherche multi-champs pondérée** : `title^3`, `url^2`, `normalized_text`, `raw_content`.
- **Fuzzy matching** : tolérance aux fautes de frappe (`AUTO`).
- **Phrase boosting** : bonus pour les correspondances exactes (`match_phrase` avec `slop=2`).
- **Highlighting** : mise en évidence des termes trouvés avec balises `<mark>`.
- **Facettes** : agrégation par faculté (`source_system.keyword`).

### Exemple de requête
```
GET /search/keyword?q=calendrier examens automne&faculty=FMPC
```

### Résultat type
- Total : 8 825 documents trouvés
- Top 10 affichés avec score de pertinence
- Facettes : UH2C (3595) | FSK (2390) | FSJ (1269) | FMPC (831)

---

## Fiche 5 : Recherche Sémantique (Qdrant)

| Champ | Détail |
|-------|--------|
| **Composant** | Base vectorielle Qdrant |
| **Objectif** | Recherche par proximité de sens (comprendre l'intention de l'utilisateur) |
| **Technologies** | Qdrant, `multilingual-e5-small`, FastAPI |
| **Endpoints API** | `GET /search/qdrant_pure`, `POST /search/qdrant_advanced` |

### Description fonctionnelle
- L'utilisateur pose une question en langage naturel.
- La question est vectorisée avec le même modèle `e5-small` utilisé à l'indexation.
- Qdrant effectue une recherche de **similarité cosinus** dans l'espace vectoriel.
- Des **filtres de métadonnées** (type, faculté, langue) peuvent être appliqués en cascade.

### Filtres dynamiques disponibles
| Filtre | Champ Qdrant | Exemple |
|--------|-------------|---------|
| Type de document | `target_table` | `course_catalog`, `students_external_profiles` |
| Faculté | `source_system` | `FSAC`, `FMPC`, `FSK` |
| Langue | `language` | `fr`, `en`, `ar` |

### Seuil de pertinence
- Score vectoriel minimum : **0.75** (résultats en dessous ignorés).
- Déduplication par texte (`seen_texts`) pour éviter les doublons.

---

## Fiche 6 : Assistant IA — RAG Hybride (Gemini)

| Champ | Détail |
|-------|--------|
| **Composant** | Pipeline RAG (Retrieval-Augmented Generation) |
| **Objectif** | Générer des réponses en langage naturel à partir des données universitaires |
| **Technologies** | Google Gemini (`gemini-3.1-flash-lite`), Qdrant, Elasticsearch, FastAPI |
| **Endpoint API** | `GET /search/semantic` |

### Description fonctionnelle
Le pipeline RAG hybride fonctionne en **4 étapes** :

| Étape | Action | Moteur |
|-------|--------|--------|
| 1. Retrieval sémantique | Recherche des 30 chunks les plus proches | Qdrant |
| 2. Retrieval lexical | Recherche des 30 meilleurs résultats par mots-clés | Elasticsearch |
| 3. Fusion RRF | Fusion des deux listes avec Reciprocal Rank Fusion (k=5) | Python |
| 4. Génération | Envoi des 10 meilleurs résultats comme contexte à Gemini | Google Gemini API |

### Prompt System
```
Tu es l'assistant IA officiel de l'université.
Utilise UNIQUEMENT le contexte ci-dessous pour répondre à la question de l'étudiant.
Si la réponse ne se trouve pas dans le contexte, dis poliment que tu ne sais pas.
```

### Gestion des erreurs
- **Safety Block** : détection explicite des filtres de sécurité Google.
- **IPv6 Docker** : monkey-patch pour forcer IPv4.
- **Transport REST** : contournement des problèmes gRPC en environnement conteneurisé.

---

## Fiche 7 : Business Intelligence (Metabase)

| Champ | Détail |
|-------|--------|
| **Composant** | Couche analytique BI |
| **Objectif** | Dashboards interactifs pour visualiser les KPIs universitaires |
| **Technologies** | Metabase, Spark Thrift Server (JDBC), Hive Metastore, PostgreSQL |

### Description fonctionnelle
- Les tables Hudi sont **synchronisées** dans le catalogue Hive Metastore (Task 4 du DAG).
- Le **Spark Thrift Server** expose une interface JDBC compatible SQL standard.
- **Metabase** se connecte via JDBC et permet de créer des dashboards sans code.

### KPIs disponibles
| KPI | Description |
|-----|-------------|
| Volume par faculté | Nombre de documents indexés par source |
| Répartition par type | Cours, actualités, règlements, publications |
| Cartographie thématique | Text Mining des publications OpenAlex |
| Taux de contenu non classifié | Jauge de qualité du routeur YAML |

---

## Fiche 8 : Observabilité & DataOps

| Champ | Détail |
|-------|--------|
| **Composant** | Système de monitoring intégré |
| **Objectif** | Surveiller la santé des pipelines et alerter en cas d'échec |
| **Technologies** | Elasticsearch (index `platform_logs`), Streamlit, SMTP (Gmail) |

### Description fonctionnelle
Chaque job (scraper, Spark Silver, Spark Gold) enregistre automatiquement son statut d'exécution dans l'index Elasticsearch `platform_logs` :

| Champ logué | Exemple |
|-------------|---------|
| `job_name` | `scraper_fsac`, `spark_silver_fstm` |
| `status` | `SUCCESS`, `FAILED` |
| `timestamp` | `2026-07-27T14:30:00Z` |
| `error_message` | *(traceback complet en cas d'échec)* |
| `documents_processed` | `1 542` |

### Interface Streamlit (Onglet Observabilité)
- **Logs Scraper** : dernières exécutions des crawlers.
- **Logs ETL** : dernières exécutions Spark.
- **Erreurs d'ingestion** : filtre sur `status=FAILED`.
- **Dashboard DataOps** : métriques globales (total exécutions, taux de fiabilité, détail par pipeline).

### Alertes automatiques
- En cas d'échec d'une tâche Airflow, un **email** est envoyé automatiquement via SMTP (Gmail) avec le traceback complet.

---

## Fiche 9 : Infrastructure & Déploiement

| Champ | Détail |
|-------|--------|
| **Composant** | Infrastructure conteneurisée |
| **Objectif** | Déploiement reproductible en une seule commande |
| **Technologies** | Docker Compose, 14+ services |

### Services Docker

| Service | Image / Base | Port exposé |
|---------|-------------|-------------|
| Airflow Webserver | Custom (Spark + NLP) | 8080 |
| Airflow Scheduler | Custom | — |
| Airflow Worker (Celery) | Custom | — |
| Redis | `redis:latest` | 6379 |
| PostgreSQL (Airflow) | `postgres:13` | 5432 |
| PostgreSQL (Hive) | `postgres:13` | 5433 |
| MinIO | `minio/minio` | 9000 / 9001 |
| Elasticsearch | `elasticsearch:8.11` | 9200 |
| Qdrant | `qdrant/qdrant` | 6333 |
| Hive Metastore | Custom | 9083 |
| Spark Thrift Server | Custom | 10000 |
| Metabase | `metabase/metabase` | 3000 |
| FastAPI (Search API) | Custom | 8000 |
| Streamlit (Frontend) | Custom | 8501 |
| Apache Tika | Custom (+ OCR) | 9998 |

### Commande de déploiement
```bash
cd infra/docker
docker compose up -d --build
```

### Prérequis matériels
- **RAM Docker** : minimum 10 Go (recommandé : 12 Go)
- **Espace disque** : ~15 Go pour les images
- **OS** : Windows (Docker Desktop/WSL2), Linux, macOS

---

> 📖 Ces fiches sont complémentaires à la [Fiche Descriptive Globale](FICHE_DESCRIPTIVE_GLOBALE.md) et au [README](../README.md) du projet.
