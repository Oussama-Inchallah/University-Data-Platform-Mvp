# 🐛 Problèmes Connus & Solutions Appliquées

> © 2026 University Data Platform — Document de référence technique pour le jury.

Ce document recense les **principaux défis techniques** rencontrés au cours du développement de la plateforme, classés par composant. Pour chaque problème, nous décrivons le symptôme, la cause racine et la solution architecturale implémentée.

---

## 1. Ingestion des Données (Crawler Web & APIs)

### 1.1 Conflit de dépendances Playwright / Airflow
- **Symptôme :** `pip` entre en boucle infinie de résolution (*backtracking*) lors du `docker build`.
- **Cause :** `playwright>=1.40` exige `greenlet==3.0.3`, incompatible avec `greenlet==3.0.2` imposé par Airflow 2.8.
- **Solution :** Fixation de `playwright==1.18.1` dans le `requirements.txt`, version compatible avec les contraintes d'Airflow.

### 1.2 Incompatibilité avec les SPAs React (ex : FSBM / Supabase)
- **Symptôme :** Le crawler ne récupère que 10 pages au lieu de 500+ sur les sites React.
- **Cause :** Playwright n'attendait que le `domcontentloaded` (squelette HTML vide) ; les requêtes API Supabase étaient rejetées par un filtre Content-Type trop restrictif (whitelist).
- **Solution :** Passage à `networkidle` + remplacement de la whitelist par une **blacklist**. Toutes les requêtes API (Supabase, GraphQL, Ajax) sont désormais interceptées.

### 1.3 Crash d'upload MinIO — Encodage Arabe & MetadataTooLarge
- **Symptôme :** `MetadataTooLarge` lors du stockage de pages en arabe.
- **Cause :** L'URL-encoding des caractères arabes multiplie par 9 la taille des métadonnées S3 (limite : 2 Ko).
- **Solution :** Troncature des headers S3 à 400 caractères via `safe_quote()`. Les métadonnées complètes (UTF-8) sont sauvegardées dans un fichier JSON compagnon `_metadata.json`.

### 1.4 Écrasement de fichiers API (Perte de données)
- **Symptôme :** Un seul fichier `professors.json` au lieu de centaines sur MinIO.
- **Cause :** Les requêtes API paginées (`?id=1`, `?id=2`) produisaient le même nom de fichier de base.
- **Solution :** Hash MD5 court des paramètres de requête ajouté au nom du fichier (ex : `professors_abcd1234.json`).

### 1.5 Fichiers nommés `index.html` écrasés entre sous-domaines (Data Loss)
- **Symptôme :** Collision silencieuse entre `bd2c.fsac-scholar.me/` et `isi.fsac-scholar.me/` dans le même bucket.
- **Solution :** Injection d'un **hash MD5 de 10 caractères** calculé sur l'URL complète dans le nom de fichier final.

### 1.6 Certificats SSL invalides (universités marocaines)
- **Symptôme :** `SSL: CERTIFICATE_VERIFY_FAILED` sur l'ESTC et d'autres facultés.
- **Solution :** Désactivation de la vérification SSL stricte (`verify=False`) avec suppression des warnings `urllib3`.

### 1.7 Liens morts et domaines éteints (NameResolutionError)
- **Symptôme :** Le DAG s'éternise (~1 min/lien mort) avec 3 tentatives par lien.
- **Solution :** Stratégie **Fail Fast** : interruption immédiate au premier échec DNS/Socket, sans retry ni fallback Playwright.

### 1.8 Nom de fichier trop long (> 255 octets)
- **Symptôme :** `[Errno 36] File name too long` sur l'ESTC (slugs WordPress de 250+ caractères).
- **Solution :** Troncature dynamique à 150 caractères + suffixe hash MD5 pour éviter les collisions.

---

## 2. Traitement & Datalake (Apache Spark & Hudi)

### 2.1 Lenteur du parsing PDF/Word (mono-thread)
- **Symptôme :** Le traitement de milliers de documents prend des heures en séquentiel.
- **Solution :** Création d'une **Pandas UDF** PySpark pour distribuer le parsing Tika sur tous les cœurs du Worker. Le JAR Tika est pré-téléchargé dans le `Dockerfile`.

### 2.2 Décalage de date autour de minuit (Tâche 1 → Tâche 2)
- **Symptôme :** La Tâche 2 (Silver) ne trouve aucune donnée car elle cherche au jour J+1 alors que les fichiers ont été stockés au jour J.
- **Cause :** Utilisation de `datetime.now()` au lieu de la date logique du DAG Airflow.
- **Solution :** Paramètre `--date` passé dynamiquement depuis Airflow (`{{ ds }}`) vers le script PySpark.

### 2.3 Conflit Hudi / Spark (versions)
- **Symptôme :** Erreurs de sérialisation lors de l'écriture Hudi.
- **Solution :** Alignement strict des versions Hudi JAR avec la version Spark du cluster (vérification via `spark.jars.packages`).

---

## 3. NLP, Embeddings & Indexation (Qdrant / Elasticsearch)

### 3.1 Boucle infinie PIP avec PyTorch (backtracking)
- **Symptôme :** Le `docker build` tourne pendant 40+ minutes puis échoue avec `ResolutionImpossible`.
- **Cause :** `sentence-transformers` exige PyTorch récent → exige `numpy > 1.24.4`, mais Airflow impose `numpy==1.24.4`.
- **Solution architecturale :** Séparation en deux fichiers : `requirements.txt` (Airflow) et `requirements-nlp.txt` (ML), avec un second `pip install` **sans** `--constraint` dans le Dockerfile.

### 3.2 Incompatibilité du client Elasticsearch
- **Symptôme :** Conflit entre le Provider Elasticsearch d'Airflow et le client Python natif.
- **Solution :** Fixation de la version `elasticsearch==8.11.1`, compatible avec Airflow 2.8.

### 3.3 Lenteur de l'indexation vectorielle
- **Symptôme :** Le calcul d'embeddings sur des DataFrames Pandas classiques est trop lent.
- **Solution :** **Pandas UDF vectorisée** via Apache Arrow. Le modèle NLP est broadcasté aux exécuteurs Spark pour un calcul distribué batch par batch.

### 3.4 Balises HTML brutes dans les résultats Qdrant
- **Symptôme :** Le texte affiché dans l'onglet Recherche Sémantique contient des `<div class="...">`, `<ul>`, etc.
- **Cause :** Certaines facultés stockent du contenu CMS (WordPress) avec du HTML non nettoyé.
- **Solution :** Nettoyage dynamique via `re.sub(r'<[^>]+>', ' ', text)` dans le frontend Streamlit avant affichage.

---

## 4. API de Recherche & Assistant IA (RAG / Gemini)

### 4.1 Erreur de génération RAG silencieuse
- **Symptôme :** Message générique "Erreur lors de la génération de la réponse par l'IA" sans détail.
- **Cause :** Le filtre de sécurité de Google Gemini bloque certaines requêtes (noms propres, données personnelles), mais le code Python ne distinguait pas le type d'erreur.
- **Solution :** Vérification explicite de `response.candidates` et `response.prompt_feedback` pour afficher la cause réelle (Safety Block, Quota, Réseau).

### 4.2 Lien "Voir la source" provoque "Page not found" dans Streamlit
- **Symptôme :** Cliquer sur "Voir la source" redirige vers `/13652` au lieu d'un site externe.
- **Cause :** L'URL stockée dans Qdrant est un ID interne (ex : `13652`) et non une URL web complète. Streamlit interprète le lien Markdown comme une route locale.
- **Solution :** Vérification de l'URL : si elle commence par `http`, lien cliquable ; sinon, affichage en texte statique ("Référence interne : 13652").

### 4.3 IPv6 bloquant dans Docker (API Gemini)
- **Symptôme :** L'API Gemini ne répond jamais (timeout infini).
- **Cause :** Docker résout `googleapis.com` en IPv6 mais le réseau Docker ne supporte pas IPv6.
- **Solution :** Monkey-patch de `socket.getaddrinfo` pour filtrer les réponses IPv6 et forcer IPv4. Transport Gemini forcé en `REST` (au lieu de gRPC).

---

## 5. Infrastructure Docker & Déploiement

### 5.1 Permissions Docker (faux avertissement)
- **Symptôme :** `[Errno 1] Operation not permitted: Changing permission to 509` au démarrage de chaque tâche.
- **Cause :** L'utilisateur `airflow` n'a pas les droits `chmod` sur les volumes montés via WSL.
- **Impact :** Aucun — l'écriture des logs fonctionne normalement. Message ignoré en production.

### 5.2 Fichier de stats écrasé entre exécutions
- **Symptôme :** `crawl_stats.json` réécrit à chaque run du DAG.
- **Solution :** Nom de fichier horodaté (ex : `crawl_stats_20260611_154530.json`) pour une traçabilité complète.

---

## 6. Problèmes Ouverts (Non Bloquants)

| # | Description | Priorité | Remarque |
|---|-------------|----------|----------|
| 1 | Certains sites changent leur structure HTML entre deux runs → le crawler peut rater des liens. | Basse | Fonctionne au re-run suivant |
| 2 | Le modèle `multilingual-e5-small` a une précision limitée sur l'arabe dialectal (darija). | Moyenne | Améliorable avec un modèle fine-tuné |
| 3 | **Blocage par CAPTCHA** : Le crawler est parfois bloqué par des systèmes anti-bots (Cloudflare, reCAPTCHA) sur certains portails universitaires. | Haute | Nécessite l'intégration de proxies ou de solveurs de CAPTCHA |
| 4 | **Signal Page / Pages de Connexion** : Impossibilité d'extraire les données se trouvant derrière des pages nécessitant une authentification (Sign-in / Signal Page). | Haute | Implémenter une gestion des cookies de session ou des identifiants |

---

> 📖 Pour plus de détails sur chaque problème du crawler, consultez : [TROUBLESHOOTING_CRAWLER.md](../TROUBLESHOOTING_CRAWLER.md)
> 📖 Pour l'historique complet par phase : [PROBLEMS_AND_SOLUTIONS.md](../PROBLEMS_AND_SOLUTIONS.md)
