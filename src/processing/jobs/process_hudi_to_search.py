"""
Job Spark - Zone Gold : Hudi -> Search (ES/Qdrant)
--------------------------------------------------
Lit les données de la zone curée (Hudi), applique le traitement NLP (Chunking + Embeddings),
et indexe les résultats dans Elasticsearch (Full-Text) et Qdrant (Recherche Vectorielle).
"""
import sys
import argparse
import logging
import time
import psutil
import os
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

try:
    from processing.core.config_loader import ConfigLoader
    from processing.core.writers.es_writer import ElasticsearchWriter
    from processing.core.writers.qdrant_writer import QdrantWriter
    from processing.core.transformers.nlp_processor import process_nlp_pipeline
except ImportError:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
    from processing.core.config_loader import ConfigLoader
    from processing.core.writers.es_writer import ElasticsearchWriter
    from processing.core.writers.qdrant_writer import QdrantWriter
    from processing.core.transformers.nlp_processor import process_nlp_pipeline

import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.executorEnv.PYTHONPATH", "/app") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "500") \
        .getOrCreate()

def main():
    start_time = time.time()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--source_system", required=True)
    args = parser.parse_args()

    logger.info(f"Starting GOLD job: {args.source_system} for date {args.execution_date}")

    loader = ConfigLoader(args.config)
    source_system = args.source_system
    
    spark = get_spark_session(f"UniversityData_Gold_{source_system}")
    
    # 1. Lecture de toutes les tables Hudi potentielles
    target_tables = list(set([e["target_table"] for e in loader.entities]))
    df_union = None
    
    for table in target_tables:
        table_path = f"s3a://curated-zone/{table}"
        try:
            df_table = spark.read.format("hudi").load(table_path)
            
            # Filtrage incrémental: on ne prend que les documents de ce source_system 
            # dont le business_timestamp (ou crawl_timestamp) correspond à l'execution_date
            if args.execution_date.lower() != "all":
                df_filtered = df_table.filter(
                    (F.col("source_system") == source_system) & 
                    (F.to_date(F.col("crawl_timestamp")) >= F.to_date(F.lit(args.execution_date)))
                )
            else:
                df_filtered = df_table.filter(F.col("source_system") == source_system)
                
            if df_union is None:
                df_union = df_filtered
            else:
                df_union = df_union.select(*df_filtered.columns).unionByName(df_filtered, allowMissingColumns=True)
        except AnalysisException:
            logger.warning(f"Hudi table {table} does not exist yet. Skipping.")
        except Exception as e:
            logger.warning(f"Error reading Hudi table {table}: {e}")
            
    if df_union is None or df_union.rdd.isEmpty():
        logger.info("No new data found in Hudi for this execution. Exiting.")
        return
        
    df_enriched = df_union
    df_enriched.cache()
    total_docs = df_enriched.count()
    
    # 2. Ecriture Elasticsearch (Full-Text)
    es_writer = ElasticsearchWriter()
    logger.info("Writing documents to Elasticsearch index: university_content")
    es_writer.write_index(df_enriched, "university_content")
    
    # 3. NLP Pipeline (Chunking & Local Embeddings e5-small)
    logger.info("Starting NLP Pipeline (Chunking & Vectorization)...")
    try:
        df_enriched, df_vectorized = process_nlp_pipeline(df_enriched)
        df_vectorized.cache()
    except Exception as e:
        logger.error(f"Failed NLP processing: {e}")
        df_vectorized = None

    total_chunks = 0
    zero_chunks = 0
    success_chunks = 0
    
    # 4. Ecriture Qdrant (Semantic Search)
    if df_vectorized is not None:
        logger.info("Writing vectorized chunks to Qdrant")
        total_chunks = df_vectorized.count()
        
        # Un vecteur rempli de 0 a son premier et dernier élément à 0.0
        from pyspark.sql.functions import expr
        zero_chunks = df_vectorized.filter(expr("embedding[0] = 0.0 AND embedding[383] = 0.0")).count()
        success_chunks = total_chunks - zero_chunks
        
        qdrant_writer = QdrantWriter()
        qdrant_writer.write_collection(df_vectorized, "university_chunks")
        df_vectorized.unpersist()
        
    df_enriched.unpersist()

    # --- METRICS DASHBOARD ---
    end_time = time.time()
    duration_sec = end_time - start_time
    throughput_docs = total_docs / duration_sec if duration_sec > 0 else 0
    throughput_chunks = total_chunks / duration_sec if duration_sec > 0 else 0
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    ram_mb = mem_info.rss / (1024 * 1024)
    cpu_percent = psutil.cpu_percent()
    success_rate = (success_chunks / total_chunks * 100) if total_chunks > 0 else 0

    dashboard = f"""
==================================================
        🌟 UNIVERSITY DATA - GOLD METRICS          
==================================================
[INFO] Source Traitée   : {source_system}
[INFO] Date Ingestion   : {args.execution_date}
[INFO] Durée Totale     : {duration_sec:.2f} secondes
[INFO] RAM Driver       : {ram_mb:.2f} MB
[INFO] CPU Process      : {cpu_percent}%
--------------------------------------------------
[ELASTICSEARCH]
  - Documents indexés   : {total_docs}
  - Débit (Docs)        : {throughput_docs:.2f} docs/sec
--------------------------------------------------
[QDRANT & NLP]
  - Total Chunks générés: {total_chunks}
  - Vecteurs Valides    : {success_chunks} ({success_rate:.1f}%)
  - Vecteurs Échoués    : {zero_chunks} (Remplis de zéros)
  - Débit (Vecteurs)    : {throughput_chunks:.2f} vecteurs/sec
==================================================
"""
    logger.info(dashboard)

if __name__ == "__main__":
    main()
