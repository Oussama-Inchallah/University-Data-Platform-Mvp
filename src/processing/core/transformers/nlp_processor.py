# ==============================================================================
# © 2026 University Data Platform
# Ce fichier fait partie du système central de la Plateforme Data Universitaire.
# ==============================================================================

"""
Moteur NLP et Vectorisation Sémantique (Zone Gold)
--------------------------------------------------
Module critique exploitant les capacités d'Apache Arrow et des Pandas UDFs
pour exécuter des modèles de Deep Learning (Transformers) distribués.
Gère le "Chunking" intelligent des textes et la génération des "Embeddings"
pour l'alimentation du moteur de recherche vectoriel (Qdrant).
"""

import pandas as pd
from typing import Iterator
from pyspark.sql.functions import pandas_udf, col, explode
from pyspark.sql.types import ArrayType, StringType, FloatType

@pandas_udf(ArrayType(StringType()))
def chunk_text_udf(texts: pd.Series) -> pd.Series:
    """
    Pandas UDF : Segmentation sémantique des documents longs.
    
    Architecture (DataOps) :
    - Utilise RecursiveCharacterTextSplitter pour couper proprement sur les paragraphes.
    - Taille optimisée (chunk_size=1000) pour correspondre au cache contextuel du modèle e5-small.
    
    Args:
        texts (pd.Series): Série Pandas contenant les textes bruts des documents.
        
    Returns:
        pd.Series: Série d'arrays de chaînes de caractères (les chunks).
    """
    try:
        from langchain_text_splitters import RecursiveCharacterTextSplitter
    except ImportError:
        return pd.Series([[] for _ in texts])
        
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000, 
        chunk_overlap=100,
        separators=["\n\n", "\n", ".", " ", ""]
    )
    
    result = []
    for text in texts:
        if not text or not isinstance(text, str):
            result.append([])
        else:
            result.append(splitter.split_text(text))
            
    return pd.Series(result)

@pandas_udf(ArrayType(FloatType()))
def generate_embeddings_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """
    Pandas UDF (Iterator) : Vectorisation Deep Learning.
    
    Architecture (DataOps / MLOps) :
    - Format Iterator : Charge le poids du modèle (500Mo) une seule fois en RAM par worker.
    - Modèle offline strict : Désactive les appels réseau vers HuggingFace Hub pour une latence de 0ms.
    - Threading CPU PyTorch : Délègue les calculs matriciels au backend C++ (4 threads) au lieu
      de multiplier les JVMs Spark, évitant ainsi le Thrashing CPU.
      
    Args:
        iterator (Iterator[pd.Series]): Flux de lots (batches) de textes.
        
    Yields:
        Iterator[pd.Series]: Flux de lots contenant les vecteurs d'embeddings (384 dimensions).
    """
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        for series in iterator:
            yield pd.Series([[0.0] * 384 for _ in series])
        return
        
    import os
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["HF_HOME"] = "/opt/airflow/.cache/huggingface"
    
    os.environ["OMP_NUM_THREADS"] = "4"
    import torch
    torch.set_num_threads(4)
    
    import time
    model = None
    for attempt in range(5):
        try:
            model = SentenceTransformer("intfloat/multilingual-e5-small")
            break
        except Exception as e:
            if attempt == 4:
                raise Exception(f"Erreur fatale: Chargement du modèle MLOps échoué: {e}")
            time.sleep(1)
            
    for series in iterator:
        texts_list = [str(t) if t else "empty" for t in series]
        texts_with_prefix = [f"passage: {t}" for t in texts_list]
        
        try:
            embeddings_np = model.encode(texts_with_prefix, batch_size=16, show_progress_bar=False)
            embeddings_list = [emb.tolist() for emb in embeddings_np]
        except Exception as e:
            print(f"Erreur d'encodage vectoriel : {e}")
            embeddings_list = [[0.0] * 384 for _ in texts_list]
            
        yield pd.Series(embeddings_list)

def process_nlp_pipeline(df):
    """
    Orchestre le workflow complet NLP sur le DataFrame.
    
    Étapes :
    1. Chunking : Segmentation sémantique (ArrayType).
    2. Explode : Aplatissement relationnel (1 ligne = 1 chunk) préservant les métadonnées.
    3. Traçabilité : Génération d'un UUID unique par chunk via hachage MD5 déterministe.
    4. Vectorisation : Application de l'UDF d'embedding sur une topologie mono-partition
       pour libérer la puissance multi-thread du moteur PyTorch sous-jacent.
       
    Args:
        df: DataFrame PySpark (couche Silver).
        
    Returns:
        tuple: (DataFrame parent Hudi, DataFrame dérivé Qdrant vectorisé).
    """
    df_with_chunks = df.withColumn("chunks", chunk_text_udf(col("normalized_text")))
    
    df_exploded = df_with_chunks.withColumn("chunk_text", explode(col("chunks"))).drop("chunks")
    
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
    
    df_vectorized = df_exploded.repartition(1).withColumn("embedding", generate_embeddings_udf(col("chunk_text")))
    
    df_main = df_with_chunks.drop("chunks")
    
    return df_main, df_vectorized
