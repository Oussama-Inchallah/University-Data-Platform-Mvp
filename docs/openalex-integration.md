# Intégration OpenAlex

Cette documentation explique le fonctionnement du pipeline d'ingestion des données depuis l'API OpenAlex vers le Data Lake (MinIO).

## 🚀 Fonctionnement du Pipeline

Le pipeline OpenAlex est un DAG Airflow (`ingestion_openalex_api`) qui se charge d'extraire toutes les publications scientifiques (`works`) associées à une liste d'institutions marocaines.

Contrairement au Web Crawler qui navigue sur des pages HTML, ce pipeline interagit avec une **API REST structurée**.

### 1. Configuration Centralisée
La configuration est stockée directement et explicitement au début du fichier DAG (`dags/ingestion/dag_openalex.py`). 
Vous y trouverez une liste Python nommée `INSTITUTIONS_TO_CRAWL`.
- Pour ajouter une nouvelle université à extraire, il suffit d'ajouter son identifiant OpenAlex à cette liste (ex: `"I204337017"` pour l'Université Hassan II).
- Le DAG lira cette liste et exécutera l'extraction pour chaque identifiant.
- *Note : L'option "Polite Pool" (utilisation d'email) n'est pas activée par défaut selon les besoins actuels de l'architecture.*

### 2. Pagination par Curseur (Cursor Pagination)
L'API OpenAlex retourne un maximum de 200 résultats par requête.
Pour récupérer les dizaines de milliers de publications d'une université, notre `OpenAlexFetcher` utilise la pagination par curseur :
1. La première requête utilise `cursor=*`.
2. La réponse JSON inclut un champ `meta.next_cursor`.
3. Le fetcher utilise ce nouveau curseur pour demander la page suivante.
4. La boucle s'arrête lorsque `next_cursor` est nul ou identique au précédent.

### 3. Philosophie de Stockage "Dumb Ingestion"
Conformément aux standards de notre Data Lakehouse :
- Nous ne modifions pas la structure du JSON retourné par OpenAlex.
- **Une page = Un fichier.** Chaque réponse de l'API (contenant 200 publications) est sauvegardée sous forme d'un fichier JSON indépendant (ex: `works_page_0001.json`).
- Cela résout le problème des "Small Files" dans le Data Lake (un fichier de 200 résultats pèse environ 1 à 2 Mo, ce qui est idéal pour Spark).
- Les fichiers atterrissent dans le bucket `raw-json`, accompagnés de leur fichier `_metadata.json` garantissant la traçabilité.

---

## 🛠️ Exécution et Dépannage

### Lancement
Le DAG est configuré avec `schedule_interval=None` (Déclenchement manuel).
Pour le lancer :
1. Allez sur l'UI Airflow (`localhost:8080`).
2. Cliquez sur le bouton "Play" (Trigger DAG) sur `ingestion_openalex_api`.

### Reprise sur erreur (Robustesse)
Le script Python `fetcher.py` intègre un mécanisme de **Retry exponentiel** natif.
Si l'API OpenAlex renvoie une erreur `429 Too Many Requests` (limite de taux atteinte) ou un Timeout, le script attendra quelques secondes (2s, puis 4s, puis 8s) avant de réessayer la page en cours.
Si le DAG plante complètement (ex: coupure internet), la relance du DAG reprendra depuis la page 1 (comportement normal en couche Bronze pour garantir l'exhaustivité, la déduplication se fera en couche Silver avec Apache Hudi).
