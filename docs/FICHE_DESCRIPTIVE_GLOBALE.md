# 📋 Fiche Descriptive Globale du Projet

---

## Informations Générales

| Champ | Détail |
|-------|--------|
| **Titre du projet** | University Data Platform MVP |
| **Type** | Plateforme de données universitaire (Data Lakehouse) |
| **Cadre** | Challenge Best-of-9 MVP — Master BD2C |
| **Université** | Université Hassan II de Casablanca |
| **Année universitaire** | 2025 – 2026 |
| **Dépôt GitHub** | [Oussama-Inchallah/University-Data-Platform-Mvp](https://github.com/Oussama-Inchallah/University-Data-Platform-Mvp) |

---

## Équipe

- **ABBA Badr-Eddine**
- **INCHALLAH Oussama**
- **BAROUDI Yassine**
- **AGOUZOUL Mohamed Reda**
- **BOUKHATEM Mohammed**

---

## Contexte & Problématique

Les universités marocaines disposent de **dizaines de sites web** (facultés, écoles, portails) contenant des informations dispersées : catalogues de cours, actualités, règlements, calendriers d'examens, publications scientifiques. Ces données sont :

- **Fragmentées** : chaque faculté a son propre site, son propre format.
- **Non structurées** : pages HTML, PDF, Word, JSON d'APIs internes.
- **Inaccessibles** : aucun moteur de recherche unifié, aucune vue consolidée.

**Objectif** : Construire une plateforme de données **end-to-end** capable de collecter, transformer, stocker, indexer et exposer ces données via une interface intelligente.

---

## Solution Proposée

Un **Data Lakehouse universitaire** suivant l'architecture **Medallion** (Bronze → Silver → Gold), orchestré par Apache Airflow et conteneurisé avec Docker Compose.

### Fonctionnalités principales

| # | Fonctionnalité | Description |
|---|----------------|-------------|
| 1 | **Ingestion automatisée** | Crawler universel (Playwright + Requests) capable de scraper des sites WordPress, React/SPA, et des APIs REST. Gestion des certificats SSL invalides, des CAPTCHAs, et des fichiers en arabe. |
| 2 | **Traitement Big Data** | Apache Spark transforme les données brutes (HTML, PDF, Word) en tables structurées Apache Hudi avec parsing Tika distribué. |
| 3 | **Recherche par mots-clés** | Index Elasticsearch avec recherche multi-champs pondérée, fuzzy matching, highlighting et facettes par faculté. |
| 4 | **Recherche sémantique** | Embeddings multilingues (`multilingual-e5-small`) stockés dans Qdrant pour une recherche par proximité de sens (FR/EN/AR). |
| 5 | **Assistant IA (RAG)** | Génération de réponses en langage naturel via Google Gemini, alimenté par une fusion hybride RRF (Reciprocal Rank Fusion) des résultats ES + Qdrant. |
| 6 | **Business Intelligence** | Dashboards Metabase connectés via Spark Thrift Server (JDBC) aux tables Hudi exposées dans Hive Metastore. |
| 7 | **Observabilité (DataOps)** | Monitoring des pipelines en temps réel : logs, taux de fiabilité, alertes email automatiques. |

---

## Stack Technologique

| Couche | Technologies |
|--------|-------------|
| **Orchestration** | Apache Airflow (CeleryExecutor) |
| **Stockage brut** | MinIO (S3-compatible) |
| **Traitement** | Apache Spark 3.x, Apache Tika |
| **Format de table** | Apache Hudi (UPSERT, BLOOM Index) |
| **Catalogue SQL** | Hive Metastore (PostgreSQL) |
| **Recherche Full-Text** | Elasticsearch 8.11 |
| **Recherche Vectorielle** | Qdrant + `multilingual-e5-small` |
| **LLM / RAG** | Google Gemini (API REST) |
| **API** | FastAPI (Python) |
| **Frontend** | Streamlit |
| **BI** | Metabase |
| **Infrastructure** | Docker Compose (14+ services) |
| **Alerting** | SMTP (Gmail) |

---

## Architecture

```
Sites Universitaires ──► Crawler (Playwright/Requests) ──► MinIO (Raw Zone)
                                                                │
OpenAlex API ──────────► Fetcher (Cursor Pagination) ──────────►│
                                                                │
                                                                ▼
                                                    Spark Silver (Tika + Nettoyage)
                                                                │
                                                                ▼
                                                            Apache Hudi
                                                          (Tables Silver)
                                                                │
                                                ┌───────────────┴───────────────┐
                                                ▼                               ▼
                                            Spark Gold                    Hive Metastore
                                                │                         (Catalogue SQL)
                                        ┌───────┴───────┐                       │
                                        ▼               ▼                       ▼
                                Elasticsearch  Chunking + Embedding        Spark Thrift
                              (Index Full-Text)         │                  (JDBC/SQL)
                                                        ▼                       │
                                                      Qdrant                    ▼
                                                  (Vecteurs e5)              Metabase
                                                                             (KPI/BI)
```

---

## Données Traitées

| Source | Type | Volume approximatif |
|--------|------|---------------------|
| 12 sites de facultés UH2C | Pages HTML, PDF, Word, images | ~12 000+ documents |
| API OpenAlex | Publications scientifiques (JSON) | ~2 000+ publications |
| APIs internes (Supabase, CMS) | Données structurées JSON | Variable par faculté |

---

## Résultats & Livrables

| Livrable | État |
|----------|------|
| Code source complet (GitHub) | ✅ Livré |
| 12 pipelines Airflow fonctionnels | ✅ Opérationnels |
| Interface Streamlit (4 onglets) | ✅ Déployée |
| API FastAPI documentée (Swagger) | ✅ Accessible |
| Dashboard Metabase BI | ✅ Configuré |
| Documentation technique (README, Runbook, Known Issues) | ✅ Complète |

---

## Améliorations Futures

| Priorité | Amélioration |
|----------|-------------|
| 🔴 Haute | Migration vers **Kubernetes (K8s)** pour le scaling horizontal |
| 🔴 Haute | Pipeline **CI/CD** (GitHub Actions) |
| 🟡 Moyenne | **Fine-tuning** du modèle NLP sur corpus marocain |
| 🟡 Moyenne | **Authentification** (Keycloak / OAuth2) |
| 🟡 Moyenne | **Streaming temps réel** (Apache Kafka) |

---

> 📖 Documentation complète : [README.md](../README.md) | [Runbook](RUNBOOK.md) | [Problèmes Connus](KNOWN_ISSUES.md)
