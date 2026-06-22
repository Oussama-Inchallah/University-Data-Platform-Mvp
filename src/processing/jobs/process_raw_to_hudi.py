"""
Job Spark - Zone Silver : Raw -> Hudi
-------------------------------------
Architecture ZERO-OOM (Metadata-Driven).
Lit uniquement les fichiers JSON de métadonnées. L'extraction Tika et BeautifulSoup
est effectuée en streamant le contenu directement depuis MinIO dans les workers Python,
évitant totalement de charger des fichiers binaires dans la mémoire de Spark.
"""
import sys
import argparse
import logging
import time
import psutil
import os
from pyspark.sql import SparkSession

try:
    from processing.core.config_loader import ConfigLoader
    from processing.core.router import DynamicRouter
    from processing.core.parsers.html_parser import parse_html_udf
    from processing.core.parsers.document_parser import parse_document_udf
    from processing.core.transformers.data_cleaner import DataCleaner, detect_language_udf
    from processing.core.writers.hudi_writer import HudiWriter
except ImportError:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
    from processing.core.config_loader import ConfigLoader
    from processing.core.router import DynamicRouter
    from processing.core.parsers.html_parser import parse_html_udf
    from processing.core.parsers.document_parser import parse_document_udf
    from processing.core.transformers.data_cleaner import DataCleaner, detect_language_udf
    from processing.core.writers.hudi_writer import HudiWriter

import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.executorEnv.PYTHONPATH", "/app") \
        .getOrCreate()

def main():
    start_time = time.time()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--source_system", required=True)
    parser.add_argument("--domain_pattern", required=False)
    args = parser.parse_args()

    logger.info(f"Starting SILVER job (ZERO-OOM Architecture): {args.source_system}")

    loader = ConfigLoader(args.config)
    source_system = args.source_system
    domain_pattern = args.domain_pattern if args.domain_pattern else f"*{source_system.lower()}*"
    
    spark = get_spark_session(f"UniversityData_Silver_{source_system}")
    
    # 1. Lecture MinIO (Metadata-Driven)
    df_html_normalized = None
    df_docs_normalized = None
    
    try:
        # --- HTML ---
        if args.execution_date.lower() == "all":
            base_json_html = f"s3a://raw-web-html/{domain_pattern}/year=*/month=*/day=*/*_metadata.json"
        else:
            year, month, day = args.execution_date.split("-")
            base_json_html = f"s3a://raw-web-html/{domain_pattern}/year={year}/month={month}/day={day}/*_metadata.json"
        
        try:
            df_meta_html = spark.read.option("multiLine", "true").json(base_json_html)
            # Repartition légère pour forcer le traitement parallèle propre sans OOM
            df_meta_html = df_meta_html.repartition(20)
            
            df_html_normalized = df_meta_html \
                .withColumn("parsed_html", parse_html_udf(F.col("storage_bucket"), F.col("storage_path"))) \
                .withColumn("title", F.col("parsed_html.title")) \
                .withColumn("normalized_text", F.col("parsed_html.text")) \
                .drop("parsed_html")
            logger.info("HTML metadata loaded successfully.")
        except Exception as e:
            logger.info(f"No HTML data found or skipped: {e}")
                
        # --- Documents (PDF, etc) ---
        if args.execution_date.lower() == "all":
            base_json_docs = f"s3a://raw-documents/{domain_pattern}/year=*/month=*/day=*/*_metadata.json"
        else:
            base_json_docs = f"s3a://raw-documents/{domain_pattern}/year={year}/month={month}/day={day}/*_metadata.json"
            
        try:
            df_meta_docs = spark.read.option("multiLine", "true").json(base_json_docs)
            df_meta_docs = df_meta_docs.repartition(20)
            
            # L'UDF télécharge directement le fichier en streamant, évitant de charger le binaire dans Spark
            df_docs_normalized = df_meta_docs \
                .withColumn("parsed_doc", parse_document_udf(F.col("storage_bucket"), F.col("storage_path"))) \
                .withColumn("title_tika", F.col("parsed_doc.title")) \
                .withColumn("normalized_text", F.col("parsed_doc.text")) \
                .drop("parsed_doc")
            
            # Resolve title fallback
            if "title" in df_docs_normalized.columns:
                df_docs_normalized = df_docs_normalized.withColumn("title", 
                    F.when(F.col("title_tika") != "", F.col("title_tika")).otherwise(F.col("title"))
                ).drop("title_tika")
            else:
                df_docs_normalized = df_docs_normalized.withColumnRenamed("title_tika", "title")
                
            logger.info("Document metadata loaded successfully.")
        except Exception as e:
            logger.info(f"No Document data found or skipped: {e}")
                
    except Exception as e:
        logger.error(f"Error reading MinIO: {e}")
        return

    # Union
    df_raw = None
    if df_html_normalized is not None and df_docs_normalized is not None:
        df_raw = df_html_normalized.unionByName(df_docs_normalized, allowMissingColumns=True)
    elif df_html_normalized is not None:
        df_raw = df_html_normalized
    elif df_docs_normalized is not None:
        df_raw = df_docs_normalized
        
    if df_raw is None:
        logger.info("No data to process.")
        return

    # 2. Routing & Enrichment
    df_routed = DynamicRouter.route_dataframe(df_raw, loader.entities)
    
    df_enriched = df_routed \
        .withColumn("source_system", F.lit(source_system)) \
        .withColumn("source_url", F.col("source_url")) \
        .withColumn("crawl_timestamp", F.col("crawl_timestamp").cast("timestamp")) \
        .withColumn("ingestion_timestamp", F.current_timestamp()) \
        .withColumn("is_deleted", F.lit(False)) \
        .withColumn("language", detect_language_udf(F.col("normalized_text"))) \
        .withColumn("record_id", DataCleaner.generate_record_id("source_system", "source_url")) \
        .withColumn("content_hash", DataCleaner.generate_content_hash("normalized_text")) \
        .withColumn("business_timestamp", F.col("crawl_timestamp"))

    # ============================================================
    # CHECKPOINT STRATEGY
    # -----------------------------------------------------------
    # Break lineage to prevent Spark from re-executing UDF logic.
    # ============================================================
    checkpoint_path = f"s3a://curated-zone/_tmp_checkpoint_{source_system}"
    logger.info(f"Writing checkpoint to {checkpoint_path}...")
    
    df_enriched.write.mode("overwrite").parquet(checkpoint_path)
    logger.info("Checkpoint written. Reading back from Parquet...")
    
    df_checkpoint = spark.read.parquet(checkpoint_path)
    
    total_docs = df_checkpoint.count()
    logger.info(f"Total documents to process: {total_docs}")
    
    # 3. Ecriture Hudi
    tables_in_batch = [row["target_table"] for row in df_checkpoint.select("target_table").distinct().collect()]
    hudi_writer = HudiWriter(base_path="s3a://curated-zone")
    
    for table in tables_in_batch:
        df_entity = df_checkpoint.filter(F.col("target_table") == table)
        hudi_writer.upsert(df_entity, table)
        logger.info(f"  -> Hudi table '{table}' written successfully.")

    # Cleanup checkpoint
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(checkpoint_path), hadoop_conf
        )
        fs.delete(spark._jvm.org.apache.hadoop.fs.Path(checkpoint_path), True)
        logger.info("Checkpoint cleaned up.")
    except Exception as e:
        logger.warning(f"Could not clean up checkpoint: {e}")
    
    # --- METRICS DASHBOARD ---
    end_time = time.time()
    duration_sec = end_time - start_time
    throughput = total_docs / duration_sec if duration_sec > 0 else 0
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    ram_mb = mem_info.rss / (1024 * 1024)
    cpu_percent = psutil.cpu_percent()

    def format_table(title, rows, col1_name="CATEGORY", col2_name="COUNT"):
        lines = []
        lines.append(f"+{'-'*35}+{'-'*12}+")
        lines.append(f"| {title:<33} | {col2_name:<10} |")
        lines.append(f"+{'-'*35}+{'-'*12}+")
        for row in rows:
            k = str(row[0]) if row[0] is not None else "NULL"
            if k == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
                k = "application/vnd.openxmlformats-..."
            elif len(k) > 33:
                k = k[:30] + "..."
            v = str(row[1])
            lines.append(f"| {k:<33} | {v:<10} |")
        lines.append(f"+{'-'*35}+{'-'*12}+")
        return "\n".join(lines)

    format_stats = ""
    table_stats = ""
    try:
        if "content_type" in df_checkpoint.columns:
            format_rows = df_checkpoint.groupBy("content_type").count().collect()
            format_stats = "\n" + format_table("FORMATS", format_rows) + "\n"
        
        table_rows = df_checkpoint.groupBy("target_table").count().collect()
        table_stats = "\n" + format_table("HUDI TABLES", table_rows) + "\n"
    except Exception as e:
        logger.warning(f"Could not calculate detailed stats: {e}")

    dashboard = f"""
==================================================
              SILVER JOB STATISTICS
==================================================
{format_stats}{table_stats}

==================================================
        🚀 ZERO-OOM ARCHITECTURE METRICS        
==================================================
[INFO] Source Traitée   : {source_system}
[INFO] Durée Totale     : {duration_sec:.2f} secondes
[INFO] RAM Driver       : {ram_mb:.2f} MB
[INFO] CPU Process      : {cpu_percent}%
--------------------------------------------------
[VOLUMÉTRIE] Documents Lus/Parsés : {total_docs}
[VOLUMÉTRIE] Tables Hudi Ciblées  : {len(tables_in_batch)}
--------------------------------------------------
[PERFORMANCES] Débit Traitement   : {throughput:.2f} docs/sec
==================================================
"""
    logger.info(dashboard)

if __name__ == "__main__":
    main()
