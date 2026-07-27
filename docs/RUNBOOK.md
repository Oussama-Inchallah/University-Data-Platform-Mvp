# 📖 RUNBOOK — University Data Platform MVP

> © 2026 University Data Platform — Guide opérationnel complet.
>
> Ce document permet à **toute personne extérieure** d'exécuter, surveiller et dépanner la plateforme sans connaissance préalable du code source.

---

## 🚀 1. Exécuter la Plateforme

### 1.1 Premier démarrage (Installation complète)

```bash
# 1. Cloner le dépôt
git clone https://github.com/Oussama-Inchallah/University-Data-Platform-Mvp.git
cd University-Data-Platform-Mvp

# 2. Configurer les variables d'environnement
cp infra/docker/.env.example infra/docker/.env
# Éditez infra/docker/.env pour renseigner GEMINI_API_KEY (optionnel)

# 3. Lancer toute la stack
cd infra/docker
docker compose up -d --build
```

> ⏱️ **Durée du premier build** : 15-20 minutes (téléchargement des images Docker, modèles NLP, dépendances Spark/Hudi).

### 1.2 Vérifier le statut des services

```bash
docker compose ps
```

Tous les services doivent afficher `Up` ou `healthy`. Attendez ~3 minutes après le `up` pour que l'initialisation d'Airflow et de Hive Metastore se termine.

### 1.3 Lancer un pipeline d'ingestion

1. Ouvrez Airflow : **http://localhost:8080** (login : `airflow` / `airflow`)
2. Dans la liste des DAGs, activez `ingest_raw_fsac` (clic sur le toggle)
3. Cliquez sur le bouton **▶ Trigger DAG**
4. Suivez l'exécution en temps réel dans la vue **Graph**

Le pipeline exécute 4 tâches séquentielles :

| Étape | Task ID | Durée estimée | Ce qui se passe |
|---|---|---|---|
| 1 - Bronze | `scrape_and_ingest_to_minio` | 5-15 min | Crawl du site web, stockage brut dans MinIO |
| 2 - Silver | `parse_and_normalize_to_hudi` | 3-10 min | Parsing (Tika) + nettoyage + écriture Hudi & ES |
| 3 - Gold | `embed_and_index_to_search` | 2-5 min | Chunking NLP + embedding + indexation Qdrant |
| 4 - Analytics | `sync_hudi_to_hive_metastore` | ~30 sec | Synchronisation du catalogue Hive pour Metabase |

### 1.4 Arrêter / Redémarrer la plateforme

```bash
# Arrêter (conserve les données)
docker compose stop

# Redémarrer
docker compose start

# Relancer complètement (si des fichiers ont changé)
docker compose up -d --build
```

---

## 🧭 2. Surveiller la Plateforme

### 2.1 Interfaces de monitoring

| Outil | URL | Ce qu'il montre |
|---|---|---|
| **Airflow** | http://localhost:8080 | État des DAGs, historique d'exécution, logs de chaque tâche |
| **MinIO Console** | http://localhost:9001 | Contenu du Data Lake (buckets raw-web-html, raw-documents, etc.) |
| **Streamlit — Observabilité** | http://localhost:8501 | Onglet "📈 Observabilité" : logs scraper, ETL, erreurs, métriques |
| **Elasticsearch** | http://localhost:9200/_cat/indices?v | Liste des index et nombre de documents |
| **Qdrant Dashboard** | http://localhost:6333/dashboard | Collections vectorielles et nombre de points |
| **Spark UI** | http://localhost:4041 | Jobs Spark en cours (Thrift Server) |

### 2.2 Vérifier le nombre de documents indexés

```bash
# Nombre de documents dans Elasticsearch
curl -s http://localhost:9200/university_content/_count | python -m json.tool

# Nombre de vecteurs dans Qdrant
curl -s http://localhost:6333/collections/university_chunks | python -m json.tool
```

### 2.3 Consulter les logs d'un service Docker

```bash
# Logs de l'API
docker compose logs -f search-api

# Logs du worker Airflow (qui exécute les DAGs)
docker compose logs -f airflow-worker

# Logs d'Elasticsearch
docker compose logs -f elasticsearch
```

### 2.4 Onglet Observabilité Streamlit

L'onglet **"📈 Observabilité de la Plateforme"** dans l'interface Streamlit (http://localhost:8501) offre :

- **Logs Scraper** : historique des crawls (succès/échecs)
- **Logs ETL** : historique des jobs Spark
- **Erreurs d'ingestion** : filtre sur les échecs uniquement
- **Tableaux de bord DataOps** : métriques globales (taux de fiabilité, détail par pipeline)

---

## 🛟 3. Récupérer (Dépannage)

### 3.1 Un DAG a échoué dans Airflow

**Symptôme** : une tâche est en rouge dans Airflow.

**Action** :
1. Cliquez sur la tâche en rouge dans la vue Graph
2. Cliquez sur **Logs** pour voir le message d'erreur
3. Si c'est une erreur réseau (timeout, site indisponible) :
   - Cliquez sur **Clear** pour relancer **uniquement cette tâche**
   - Le pipeline reprend à partir de la tâche échouée
4. Si l'erreur est un bug dans le code :
   - Corrigez le fichier source dans `src/`
   - Les volumes Docker montent le code source → la correction est immédiate
   - Relancez la tâche avec **Clear**

> 💡 **Le système est idempotent** : relancer une tâche ne crée jamais de doublons grâce aux Upserts Hudi et aux ETags du crawler.

### 3.2 OutOfMemory (Exit Code 137)

**Symptôme** : un conteneur redémarre en boucle ou s'arrête avec le code 137.

**Action** :
1. Vérifiez la RAM allouée à Docker Desktop : **Settings → Resources → Memory**
2. Augmentez à **12 Go minimum** (16 Go recommandé si vous lancez plusieurs DAGs en parallèle)
3. Si le problème persiste, relancez :
```bash
docker compose restart airflow-worker
```

### 3.3 Elasticsearch ne répond pas

**Symptôme** : `curl http://localhost:9200` ne retourne rien.

**Action** :
```bash
# Vérifier le statut
docker compose ps elasticsearch

# Si le conteneur est arrêté, le relancer
docker compose restart elasticsearch

# Attendre 30 secondes, puis vérifier
curl http://localhost:9200/_cluster/health?pretty
```

### 3.4 MinIO est vide après un DAG réussi

**Symptôme** : le DAG est vert mais MinIO n'a pas de nouveaux fichiers.

**Cause probable** : le site web cible est en maintenance ou a changé sa structure HTML.

**Action** :
1. Consultez les **Logs Scraper** dans l'onglet Observabilité Streamlit
2. Vérifiez que le site est accessible : `curl -I https://fsac.univh2c.ma`
3. Si le site fonctionne mais le crawler ne trouve rien, inspectez les paramètres du DAG (SEED_URL, ALLOWED_DOMAINS)

### 3.5 Réinitialisation complète (Hard Reset)

Si vous souhaitez **tout supprimer** (données, index, volumes) pour repartir de zéro :

```bash
cd infra/docker

# Arrêter et supprimer tous les volumes
docker compose down -v

# Relancer depuis zéro
docker compose up -d --build
```

> ⚠️ **Attention** : le paramètre `-v` supprime **toutes les données** (MinIO, Elasticsearch, Qdrant, PostgreSQL, Hive Metastore, Metabase).

### 3.6 DAG "fantôme" dans Airflow

**Symptôme** : un DAG supprimé du dossier `dags/` apparaît toujours dans l'interface Airflow (sans aucune exécution récente).

**Action** :
1. Dans l'interface Airflow, cliquez sur l'icône **Corbeille** (🗑️) à droite du DAG fantôme
2. Ou via le terminal Docker :
```bash
docker compose exec airflow-webserver airflow dags delete -y nom_du_dag
```

---

## 📋 4. Checklist de Vérification Post-Déploiement

Après le lancement de la plateforme, vérifiez que tout est opérationnel :

- [ ] **Airflow** : http://localhost:8080 → 12 DAGs visibles
- [ ] **MinIO** : http://localhost:9001 → buckets `raw-web-html`, `raw-documents`, `raw-json` créés
- [ ] **Elasticsearch** : `curl http://localhost:9200` → retourne un JSON avec `cluster_name`
- [ ] **Qdrant** : http://localhost:6333/dashboard → interface accessible
- [ ] **Streamlit** : http://localhost:8501 → 4 onglets fonctionnels
- [ ] **FastAPI** : http://localhost:8000/docs → Swagger accessible
- [ ] **Metabase** : http://localhost:3000 → setup wizard ou dashboard

---

## 🔄 5. Procédure de Réexécution (Idempotence)

La plateforme est conçue pour que chaque exécution soit **sûre et répétable** :

| Composant | Mécanisme d'idempotence |
|---|---|
| **Crawler** | Vérifie les ETags existants sur MinIO avant de télécharger → ignore les fichiers déjà présents |
| **Spark Silver** | Écrit en mode `UPSERT` dans Hudi avec clé primaire `url` → met à jour au lieu de dupliquer |
| **Spark Gold** | Lit les données incrémentales Hudi → ne retraite que les documents modifiés |
| **Elasticsearch** | Indexation avec `_id` basé sur `doc_id` → écrase les documents existants |
| **Qdrant** | Points identifiés par `doc_id` → mise à jour des vecteurs existants |

**Résultat** : vous pouvez lancer le même DAG 10 fois consécutives sans aucun effet secondaire.
