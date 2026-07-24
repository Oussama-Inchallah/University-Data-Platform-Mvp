"""
Processeur NLP (Traitement du Langage Naturel)
----------------------------------------------
Fournit des fonctions Spark (Pandas UDFs) pour :
1. Découper les textes longs en segments (Chunking) via Langchain.
2. Vectoriser ces segments (Embeddings) via SentenceTransformers.

L'utilisation des Pandas UDFs permet d'exécuter ces modèles lourds en batch 
(Vectorisation) via Apache Arrow, optimisant ainsi la RAM et le temps CPU.
"""
import pandas as pd
from typing import Iterator
from pyspark.sql.functions import pandas_udf, col, explode
from pyspark.sql.types import ArrayType, StringType, FloatType

@pandas_udf(ArrayType(StringType()))
def chunk_text_udf(texts: pd.Series) -> pd.Series:
    """
    Pandas UDF pour découper une série de textes en chunks.
    Utilise RecursiveCharacterTextSplitter pour respecter les paragraphes et phrases.
    """
    try:
        from langchain_text_splitters import RecursiveCharacterTextSplitter
    except ImportError:
        # Fallback au cas où langchain n'est pas installé sur les workers
        return pd.Series([[] for _ in texts])
        
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=500, 
        chunk_overlap=50,
        separators=["\n\n", "\n", ".", " ", ""]
    )
    
    result = []
    for text in texts:
        if not text or not isinstance(text, str):
            result.append([])
        else:
            chunks = splitter.split_text(text)
            result.append(chunks)
            
    return pd.Series(result)

@pandas_udf(ArrayType(FloatType()))
def generate_embeddings_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """
    Pandas UDF (Iterator) pour générer les vecteurs d'embedding via un modèle local HuggingFace.
    L'utilisation d'un Iterator permet de charger le modèle (470 Mo) en RAM UNE SEULE FOIS 
    par worker (cœur CPU), au lieu de le recharger pour chaque batch.
    
    Modèle : intfloat/multilingual-e5-small (Multilingue, 384 dimensions, léger et très performant)
    """
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        # En cas d'erreur d'import (si la lib n'est pas installée sur le worker)
        for series in iterator:
            yield pd.Series([[0.0] * 384 for _ in series])
        return
        
    # Initialisation du modèle (exécuté 1 seule fois par worker)
    # Le modèle a DÉJÀ été téléchargé lors du `docker build` (voir Dockerfile ligne 73-75).
    # On force HuggingFace à fonctionner en mode HORS-LIGNE strict pour empêcher toute requête réseau 
    # de vérification de version. Cela garantit 0 ms de latence réseau et empêche les erreurs de connexion.
    import os
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["HF_HOME"] = "/opt/airflow/.cache/huggingface"
    
    import time
    model = None
    for attempt in range(5):
        try:
            # En mode offline, cela lit instantanément depuis le disque
            model = SentenceTransformer("intfloat/multilingual-e5-small")
            break
        except Exception as e:
            if attempt == 4:
                raise Exception(f"Impossible de charger le modèle NLP depuis le cache local: {e}")
            time.sleep(1)
    for series in iterator:
        # Remplacer les valeurs nulles par des chaînes vides
        texts_list = [str(t) if t else "empty" for t in series]
        
        # Le modèle e5-small recommande d'ajouter le préfixe "passage: " pour les documents
        texts_with_prefix = [f"passage: {t}" for t in texts_list]
        
        try:
            # Encodage par batch (optimisé localement sur CPU, batch_size=16 est meilleur pour le cache L3)
            embeddings_np = model.encode(texts_with_prefix, batch_size=16, show_progress_bar=False)
            
            # Conversion numpy array -> list of floats
            embeddings_list = [emb.tolist() for emb in embeddings_np]
        except Exception as e:
            print(f"Erreur d'encodage local (SentenceTransformers): {e}")
            embeddings_list = [[0.0] * 384 for _ in texts_list]
            
        yield pd.Series(embeddings_list)

def process_nlp_pipeline(df):
    """
    Applique le pipeline NLP sur un DataFrame Spark :
    1. Découpage du texte en chunks.
    2. Explode (1 ligne = 1 chunk).
    3. Génération des embeddings pour chaque chunk.
    
    Retourne le DataFrame original (intact) ET un DataFrame dérivé pour les chunks.
    """
    # 1. On chunk le texte (ajoute une colonne 'chunks' qui est un Array de Strings)
    df_with_chunks = df.withColumn("chunks", chunk_text_udf(col("normalized_text")))
    
    # 2. On explose l'array pour que chaque chunk devienne une ligne distincte
    # On garde les métadonnées utiles pour RAG (url, id, etc.)
    df_exploded = df_with_chunks.select(
        col("record_id"),
        col("source_system"),
        col("source_url"),
        col("crawl_timestamp"),
        col("target_table"),
        explode(col("chunks")).alias("chunk_text")
    )
    
    # Génération d'un sous-ID déterministe (UUID valide) pour chaque chunk (MD5 formaté en 8-4-4-4-12)
    from pyspark.sql.functions import md5, concat_ws, expr
    df_exploded = df_exploded.withColumn(
        "chunk_md5", md5(concat_ws("_", col("record_id"), col("chunk_text")))
    ).withColumn(
        "id", 
        concat_ws("-", 
            expr("substring(chunk_md5, 1, 8)"),
            expr("substring(chunk_md5, 9, 4)"),
            expr("substring(chunk_md5, 13, 4)"),
            expr("substring(chunk_md5, 17, 4)"),
            expr("substring(chunk_md5, 21, 12)")
        )
    ).drop("chunk_md5", "chunk_id")    
    # 3. Vectorisation des chunks en parallèle
    # Utilisation dynamique de tous les cœurs alloués par Spark (local[*]) au lieu d'une limite fixe de 2.
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    cores = spark.sparkContext.defaultParallelism if spark else 4
    
    df_vectorized = df_exploded.repartition(cores).withColumn("embedding", generate_embeddings_udf(col("chunk_text")))
    
    # On retire la colonne temporaire 'chunks' du DF principal pour ne pas l'alourdir
    df_main = df_with_chunks.drop("chunks")
    
    return df_main, df_vectorized
