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
def generate_embeddings_udf(texts: pd.Series) -> pd.Series:
    """
    Pandas UDF pour générer les vecteurs d'embedding via l'API Google Gemini.
    Traite les données par batch pour optimiser les requêtes réseau.
    """
    import os
    import time
    try:
        from google import genai
    except ImportError:
        return pd.Series([[] for _ in texts])
        
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("ATTENTION: GEMINI_API_KEY non trouvée. Retour de vecteurs vides.")
        return pd.Series([[] for _ in texts])
        
    client = genai.Client(api_key=api_key)
    
    # Remplacer les valeurs nulles par des chaînes vides
    texts_list = [str(t) if t else "empty" for t in texts]
    
    embeddings = []
    # Gemini API limite la taille des batchs à 100 pour les embeddings
    # Et a un rate limit de 15 RPM en free tier
    batch_size = 100
    for i in range(0, len(texts_list), batch_size):
        batch = texts_list[i:i+batch_size]
        try:
            response = client.models.embed_content(
                model="gemini-embedding-2",
                contents=batch
            )
            # Extraire les valeurs pour chaque embedding du batch
            batch_embeddings = [emb.values for emb in response.embeddings]
            
            # Pad si l'API a retourné moins d'éléments que prévu
            while len(batch_embeddings) < len(batch):
                batch_embeddings.append([0.0] * 3072)
                
            embeddings.extend(batch_embeddings[:len(batch)])
            time.sleep(4) # Délai pour éviter le rate-limit (15 RPM = 4 sec)
        except Exception as e:
            print(f"Erreur API Gemini: {e}")
            # En cas d'erreur, remplir avec des vecteurs vides pour ce batch
            # Le modèle gemini-embedding-2 a 3072 dimensions
            embeddings.extend([[0.0] * 3072 for _ in batch])
            
    return pd.Series(embeddings)

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
    
    # Génération d'un sous-ID unique pour chaque chunk (hash du record_id + texte du chunk)
    from pyspark.sql.functions import sha2, concat_ws
    df_exploded = df_exploded.withColumn("chunk_id", sha2(concat_ws("_", col("record_id"), col("chunk_text")), 256))
    
    # 3. Vectorisation des chunks
    df_vectorized = df_exploded.withColumn("embedding", generate_embeddings_udf(col("chunk_text")))
    
    # On retire la colonne temporaire 'chunks' du DF principal pour ne pas l'alourdir
    df_main = df_with_chunks.drop("chunks")
    
    return df_main, df_vectorized
