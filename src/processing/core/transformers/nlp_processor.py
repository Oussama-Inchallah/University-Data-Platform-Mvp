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
    import random
    
    # L'API Gemini en Free Tier compte CHAQUE texte dans le batch comme 1 requête !
    # La limite est de 100 requêtes par minute (RPM).
    # Avec repartition(1), un seul worker s'exécute.
    # On peut envoyer 90 textes d'un coup, puis attendre 60s pour réinitialiser le quota.
    batch_size = 90
    for i in range(0, len(texts_list), batch_size):
        batch = texts_list[i:i+batch_size]
        
        from google.genai import types
        batch_contents = [
            types.Content(parts=[types.Part.from_text(text=t)]) 
            for t in batch
        ]
        
        max_retries = 7
        base_delay = 30
        success = False
        
        for attempt in range(max_retries):
            try:
                response = client.models.embed_content(
                    model="gemini-embedding-2",
                    contents=batch_contents,
                    config=types.EmbedContentConfig(output_dimensionality=768)
                )
                
                batch_embeddings = [emb.values for emb in response.embeddings]
                
                while len(batch_embeddings) < len(batch):
                    batch_embeddings.append([0.0] * 768)
                    
                embeddings.extend(batch_embeddings[:len(batch)])
                success = True
                
                # Délai obligatoire pour le Free Tier (90 requêtes consommées = 60s d'attente max)
                time.sleep(60)
                break
                
            except Exception as e:
                error_msg = str(e)
                if "429" in error_msg or "RESOURCE_EXHAUSTED" in error_msg:
                    sleep_time = 60 + random.uniform(0, 10)
                    print(f"Rate limit Gemini (429) atteint ! Batch de {len(batch)} textes. Tentative {attempt+1}/{max_retries}. Pause forcée de {sleep_time:.1f}s...")
                    time.sleep(sleep_time)
                else:
                    print(f"Erreur fatale API Gemini: {e}")
                    break
                    
        if not success:
            print(f"Echec final pour le batch après {max_retries} tentatives. Remplissage avec zéros.")
            embeddings.extend([[0.0] * 768 for _ in batch])
            
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
    # Repartition(1) force Spark à n'utiliser qu'un seul worker pour éviter le DDoS de l'API
    df_vectorized = df_exploded.repartition(1).withColumn("embedding", generate_embeddings_udf(col("chunk_text")))
    
    # On retire la colonne temporaire 'chunks' du DF principal pour ne pas l'alourdir
    df_main = df_with_chunks.drop("chunks")
    
    return df_main, df_vectorized
