"""
Script principal de traitement Spark (ETL)
------------------------------------------
Ce script est exécuté par Airflow (SparkSubmitOperator).
Son rôle est de :
1. Lire les données brutes (HTML, PDF, Word) depuis la zone 'raw' de MinIO.
2. Parser le contenu (extraire le texte via Tika ou BeautifulSoup).
3. Router dynamiquement les données vers la bonne table cible (Hudi) en se basant sur rules.yaml.
4. Nettoyer et normaliser (hachage, détection de langue).
5. Écrire le résultat dans la zone 'curated' de MinIO (format Hudi).
"""
import sys
import argparse
import logging
from pyspark.sql import SparkSession

# Imports des modules core (assumant que src/ est dans le PYTHONPATH lors du spark-submit)
try:
    from processing.core.config_loader import ConfigLoader
    from processing.core.router import DynamicRouter
    from processing.core.parsers.html_parser import parse_html_udf
    from processing.core.parsers.document_parser import parse_document_udf
    from processing.core.transformers.data_cleaner import DataCleaner, detect_language_udf
    from processing.core.writers.hudi_writer import HudiWriter
    from processing.core.writers.es_writer import ElasticsearchWriter
    from processing.core.writers.qdrant_writer import QdrantWriter
    from processing.core.transformers.nlp_processor import process_nlp_pipeline
except ImportError:
    # Fallback si exécuté directement sans PYTHONPATH
    import os
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
    from processing.core.config_loader import ConfigLoader
    from processing.core.router import DynamicRouter
    from processing.core.parsers.html_parser import parse_html_udf
    from processing.core.parsers.document_parser import parse_document_udf
    from processing.core.transformers.data_cleaner import DataCleaner, detect_language_udf
    from processing.core.writers.hudi_writer import HudiWriter
    from processing.core.writers.es_writer import ElasticsearchWriter
    from processing.core.writers.qdrant_writer import QdrantWriter
    from processing.core.transformers.nlp_processor import process_nlp_pipeline

import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str) -> SparkSession:
    """Initialise la session Spark avec les configurations Hudi/S3 de base."""
    # Les options S3 et Hudi exactes seront ajoutées lors de la finalisation des Writers
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.executorEnv.PYTHONPATH", "/app") \
        .getOrCreate()

def main():
    parser = argparse.ArgumentParser(description="Universal Spark Data Processor")
    parser.add_argument("--config", required=True, help="Path to the university YAML config file")
    parser.add_argument("--execution_date", required=True, help="Airflow execution date (YYYY-MM-DD) for incremental load")
    parser.add_argument("--source_system", required=True, help="Source system identifier (e.g., FSAC, FSTM)")
    parser.add_argument("--domain_pattern", required=False, help="Domain pattern for MinIO paths (e.g. *fs.ucd.ac.ma*)")
    args = parser.parse_args()

    logger.info(f"Starting processing job with config={args.config}, date={args.execution_date}, source_system={args.source_system}")

    # 1. Chargement de la configuration YAML
    loader = ConfigLoader(args.config)
    source_system = args.source_system
    
    # 2. Initialisation Spark
    spark = get_spark_session(f"UniversityData_Process_{source_system}")
    
    # 3. Lecture depuis MinIO (Incremental load basé sur execution_date)
    try:
        execution_date = args.execution_date
        if args.domain_pattern:
            domain_pattern = args.domain_pattern
        else:
            domain_pattern = f"*{source_system.lower()}*"
        
        # --- Traitement HTML ---
        # On lit tous les fichiers qui ne sont pas des .json dans le dossier du jour
        if execution_date.lower() == "all":
            base_path_html = f"s3a://raw-web-html/{domain_pattern}/year=*/month=*/day=*/*"
            base_path_json_html = f"s3a://raw-web-html/{domain_pattern}/year=*/month=*/day=*/*_metadata.json"
        else:
            year, month, day = execution_date.split("-")
            base_path_html = f"s3a://raw-web-html/{domain_pattern}/year={year}/month={month}/day={day}/*"
            base_path_json_html = f"s3a://raw-web-html/{domain_pattern}/year={year}/month={month}/day={day}/*_metadata.json"
        
        logger.info(f"Reading HTML files from {base_path_html}")
        df_html_normalized = None
        try:
            rdd_html = spark.sparkContext.wholeTextFiles(base_path_html)
            if not rdd_html.isEmpty():
                df_html = rdd_html.toDF(["raw_path", "content"])
                df_meta_html = spark.read.option("multiLine", "true").json(base_path_json_html)
                df_meta_html = df_meta_html.withColumn("raw_path_meta", F.concat(F.lit("s3a://"), F.col("storage_bucket"), F.lit("/"), F.col("storage_path")))
                df_raw_html = df_html.join(df_meta_html, df_html.raw_path == df_meta_html.raw_path_meta, how="inner")
                
                df_html_normalized = df_raw_html \
                    .withColumn("parsed_html", parse_html_udf(F.col("content"))) \
                    .withColumn("title", F.col("parsed_html.title")) \
                    .withColumn("normalized_text", F.col("parsed_html.text")) \
                    .drop("parsed_html", "content")
            else:
                logger.warning("No HTML files found for this date.")
        except Exception as e:
            logger.warning(f"Could not read HTML files (might not exist yet): {e}")

        # --- Traitement Documents (PDF, DOCX, etc.) ---
        # On lit tous les fichiers qui ne sont pas des .html ni des .json dans raw-documents
        if args.execution_date.lower() == "all":
            base_path_docs = f"s3a://raw-documents/{domain_pattern}/year=*/month=*/day=*/*"
            base_path_json_docs = f"s3a://raw-documents/{domain_pattern}/year=*/month=*/day=*/*_metadata.json"
        else:
            year, month, day = args.execution_date.split("-")
            base_path_docs = f"s3a://raw-documents/{domain_pattern}/year={year}/month={month}/day={day}/*"
            base_path_json_docs = f"s3a://raw-documents/{domain_pattern}/year={year}/month={month}/day={day}/*_metadata.json"
        
        logger.info(f"Reading Document files from {base_path_docs}")
        df_docs_normalized = None
        try:
            rdd_docs = spark.sparkContext.binaryFiles(base_path_docs)
            # binaryFiles renvoie (path, bytearray), nous voulons l'appliquer à Tika
            if not rdd_docs.isEmpty():
                df_docs = rdd_docs.toDF(["raw_path", "content_binary"])
                # Filtrer les json qui auraient pu être attrapés par le glob "*"
                df_docs = df_docs.filter(~F.col("raw_path").endswith(".json"))
                
                df_meta_docs = spark.read.option("multiLine", "true").json(base_path_json_docs)
                df_meta_docs = df_meta_docs.withColumn("raw_path_meta", F.concat(F.lit("s3a://"), F.col("storage_bucket"), F.lit("/"), F.col("storage_path")))
                df_raw_docs = df_docs.join(df_meta_docs, df_docs.raw_path == df_meta_docs.raw_path_meta, how="inner")
                
                df_docs_normalized = df_raw_docs \
                    .withColumn("parsed_doc", parse_document_udf(F.col("content_binary"))) \
                    .withColumn("title_tika", F.col("parsed_doc.title")) \
                    .withColumn("normalized_text", F.col("parsed_doc.text")) \
                    .drop("parsed_doc", "content_binary")
                
                # Si le titre Tika est vide, on garde le title d'origine (du json) s'il existe, sinon on prend celui de Tika
                if "title" in df_docs_normalized.columns:
                    df_docs_normalized = df_docs_normalized.withColumn("title", 
                        F.when(F.col("title_tika") != "", F.col("title_tika")).otherwise(F.col("title"))
                    ).drop("title_tika")
                else:
                    df_docs_normalized = df_docs_normalized.withColumnRenamed("title_tika", "title")
            else:
                logger.info("No Document files found for this date.")
        except Exception as e:
            logger.warning(f"Could not read documents from raw-documents (might not exist yet): {e}")

        # --- Union des DataFrames ---
        df_normalized_union = None
        if df_html_normalized is not None and df_docs_normalized is not None:
            # On s'assure d'avoir les mêmes colonnes avant l'union
            df_normalized_union = df_html_normalized.select(*df_docs_normalized.columns).unionByName(df_docs_normalized, allowMissingColumns=True)
        elif df_html_normalized is not None:
            df_normalized_union = df_html_normalized
        elif df_docs_normalized is not None:
            df_normalized_union = df_docs_normalized
        
        if df_normalized_union is None:
            logger.error("No data (HTML or Docs) to process.")
            return

        df_raw = df_normalized_union
        
    except Exception as e:
        logger.error(f"Error reading from MinIO: {e}")
        return
    
    # 4. Routage dynamique
    df_routed = DynamicRouter.route_dataframe(df_raw, loader.entities)
    routed_count = df_routed.count()
    logger.info(f"===> DEBUG: Rows after routing = {routed_count}")
    
    # 5. Normalisation Commune & Hachage
    df_enriched = df_routed \
        .withColumn("source_system", F.lit(source_system)) \
        .withColumn("source_url", F.col("source_url")) \
        .withColumn("crawl_timestamp", F.col("crawl_timestamp").cast("timestamp")) \
        .withColumn("ingestion_timestamp", F.current_timestamp()) \
        .withColumn("is_deleted", F.lit(False)) \
        .withColumn("language", detect_language_udf(F.col("normalized_text"))) \
        .withColumn("record_id", DataCleaner.generate_record_id("source_system", "source_url")) \
        .withColumn("content_hash", DataCleaner.generate_content_hash("normalized_text")) \
        .withColumn("business_timestamp", F.col("crawl_timestamp")) \
        .drop("raw_path", "raw_path_meta") # OPTIMISATION : Suppression du chemin brut
        
    # 6. Écriture Hudi & ES (Ventilation Dynamique)
    # On cache le DataFrame enrichi en mémoire pour éviter de le recalculer pour chaque table
    df_enriched.cache()
    
    # --- PHASE 3: NLP & Indexation ---
    logger.info("Starting NLP Pipeline (Chunking & Embeddings)...")
    try:
        df_enriched, df_vectorized = process_nlp_pipeline(df_enriched)
        df_vectorized.cache()
    except Exception as e:
        logger.error(f"Failed NLP processing: {e}")
        df_vectorized = None
    
    # On récupère la liste unique des tables cibles trouvées dans ce batch
    tables_in_batch = [row["target_table"] for row in df_enriched.select("target_table").distinct().collect()]
    logger.info(f"Entities found in this batch: {tables_in_batch}")
    
    hudi_writer = HudiWriter(base_path="s3a://curated-zone")
    es_writer = ElasticsearchWriter()
    qdrant_writer = QdrantWriter()
    
    for table_name in tables_in_batch:
        logger.info(f"Writing data to Hudi table: {table_name}")
        # On filtre le DataFrame pour ne garder que l'entité en cours
        df_entity = df_enriched.filter(F.col("target_table") == table_name)
        
        # Upsert dans la table Hudi spécifique (ex: faculty_profiles)
        hudi_writer.upsert(df_entity, table_name)
        
    logger.info("Writing all entities to global Elasticsearch index: university_content")
    es_writer.write_index(df_enriched, "university_content")
    
    if df_vectorized is not None:
        logger.info("Writing vectorized chunks to Qdrant")
        qdrant_writer.write_collection(df_vectorized, "university_chunks")
        df_vectorized.unpersist()
    
    df_enriched.unpersist()

    # === STATISTIQUES GLOBALES ===
    try:
        def format_table(title, rows, col1_name="CATEGORY", col2_name="COUNT"):
            lines = []
            lines.append(f"+{'-'*35}+{'-'*12}+")
            lines.append(f"| {title:<33} | {col2_name:<10} |")
            lines.append(f"+{'-'*35}+{'-'*12}+")
            for row in rows:
                k = str(row[0]) if row[0] is not None else "NULL"
                if k == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
                    k = "docx"
                elif k == "application/pdf":
                    k = "pdf"
                elif k == "application/msword":
                    k = "doc"
                
                v = str(row[1])
                lines.append(f"| {k:<33} | {v:<10} |")
            lines.append(f"+{'-'*35}+{'-'*12}+")
            return "\n" + "\n".join(lines)

        content_rows = df_raw.groupBy("content_type").count().collect()
        status_rows = df_raw.groupBy("http_status").count().collect()
        hudi_rows = df_enriched.groupBy("target_table").count().collect()
        total_records = df_enriched.count()

        stat_block = []
        stat_block.append("==================================================")
        stat_block.append("              JOB STATISTICS SUMMARY              ")
        stat_block.append("==================================================")
        stat_block.append(format_table("CRAWLER DISCOVERY (Formats)", content_rows))
        stat_block.append(format_table("HTTP STATUS", status_rows))
        stat_block.append(format_table("CURATED ZONE (Hudi Tables)", hudi_rows))
        stat_block.append(f"\nTotal processed and stored records: {total_records}")
        stat_block.append("==================================================")
        
        for line in stat_block:
            logger.info(line)
            
    except Exception as e:
        logger.warning(f"Could not print statistics: {e}")
    logger.info("Processing job finished successfully.")

if __name__ == "__main__":
    main()
