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
    from processing.core.parsers.json_parser import parse_json_udf
    from processing.core.transformers.data_cleaner import DataCleaner, detect_language_udf
    from processing.core.writers.hudi_writer import HudiWriter
    from processing.core.utils.platform_logger import PlatformLogger
except ImportError:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
    from processing.core.config_loader import ConfigLoader
    from processing.core.router import DynamicRouter
    from processing.core.parsers.html_parser import parse_html_udf
    from processing.core.parsers.document_parser import parse_document_udf
    from processing.core.parsers.json_parser import parse_json_udf
    from processing.core.transformers.data_cleaner import DataCleaner, detect_language_udf
    from processing.core.writers.hudi_writer import HudiWriter
    from processing.core.utils.platform_logger import PlatformLogger

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
    parser.add_argument("--force_target_table", required=False, help="Bypass routing and force all records to this table")
    args = parser.parse_args()

    logger.info(f"Starting SILVER job (ZERO-OOM Architecture): {args.source_system}")

    loader = ConfigLoader(args.config)
    source_system = args.source_system
    
    platform_logger = PlatformLogger()
    job_name = f"spark_process_raw_to_hudi_{source_system}"
    
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

        # --- Images (OCR via Tika) ---
        df_images_normalized = None
        if args.execution_date.lower() == "all":
            base_json_images = f"s3a://raw-images/{domain_pattern}/year=*/month=*/day=*/*_metadata.json"
        else:
            base_json_images = f"s3a://raw-images/{domain_pattern}/year={year}/month={month}/day={day}/*_metadata.json"
            
        try:
            df_meta_images = spark.read.option("multiLine", "true").json(base_json_images)
            df_meta_images = df_meta_images.repartition(20)
            
            # On utilise le même document_parser_udf, car Tika gère nativement l'OCR des images
            df_images_normalized = df_meta_images \
                .withColumn("parsed_img", parse_document_udf(F.col("storage_bucket"), F.col("storage_path"))) \
                .withColumn("title_tika", F.col("parsed_img.title")) \
                .withColumn("normalized_text", F.col("parsed_img.text")) \
                .drop("parsed_img")
                
            if "title" in df_images_normalized.columns:
                df_images_normalized = df_images_normalized.withColumn("title", 
                    F.when(F.col("title_tika") != "", F.col("title_tika")).otherwise(F.col("title"))
                ).drop("title_tika")
            else:
                df_images_normalized = df_images_normalized.withColumnRenamed("title_tika", "title")
                
            logger.info("Image metadata (OCR) loaded successfully.")
        except Exception as e:
            logger.info(f"No Image data found or skipped: {e}")
            
        # --- JSON (API Dumps) ---
        df_json_normalized = None
        if args.execution_date.lower() == "all":
            base_json_json = f"s3a://raw-json/{domain_pattern}/year=*/month=*/day=*/*_metadata.json"
        else:
            base_json_json = f"s3a://raw-json/{domain_pattern}/year={year}/month={month}/day={day}/*_metadata.json"
            
        try:
            df_meta_json = spark.read.option("multiLine", "true").json(base_json_json)
            df_meta_json = df_meta_json.repartition(20)
            
            df_json_normalized = df_meta_json \
                .withColumn("parsed_json_array", parse_json_udf(F.col("storage_bucket"), F.col("storage_path"))) \
                .withColumn("parsed_json", F.explode("parsed_json_array")) \
                .withColumn("title_parsed", F.col("parsed_json.title")) \
                .withColumn("normalized_text", F.col("parsed_json.text")) \
                .withColumn("source_url", F.when(F.col("parsed_json.url") != "", F.col("parsed_json.url")).otherwise(F.col("source_url"))) \
                .drop("parsed_json", "parsed_json_array")
                
            if "title" in df_json_normalized.columns:
                df_json_normalized = df_json_normalized.withColumn("title", 
                    F.when(F.col("title_parsed") != "", F.col("title_parsed")).otherwise(F.col("title"))
                ).drop("title_parsed")
            else:
                df_json_normalized = df_json_normalized.withColumnRenamed("title_parsed", "title")
                
            logger.info("JSON metadata loaded successfully.")
        except Exception as e:
            logger.info(f"No JSON data found or skipped: {e}")

                
    except Exception as e:
        logger.error(f"Error reading MinIO: {e}")
        return

    # Union globale des 4 sources
    dataframes_to_union = []
    if df_html_normalized is not None:
        dataframes_to_union.append(df_html_normalized)
    if df_docs_normalized is not None:
        dataframes_to_union.append(df_docs_normalized)
    if df_images_normalized is not None:
        dataframes_to_union.append(df_images_normalized)
    if df_json_normalized is not None:
        dataframes_to_union.append(df_json_normalized)
        
    df_raw = None
    if dataframes_to_union:
        df_raw = dataframes_to_union[0]
        for df_other in dataframes_to_union[1:]:
            df_raw = df_raw.unionByName(df_other, allowMissingColumns=True)
            
    if df_raw is None:
        logger.info("No data to process.")
        return

    # 2. Routing & Enrichment
    if args.force_target_table:
        logger.info(f"Bypassing dynamic routing: forcing target_table to '{args.force_target_table}'")
        df_routed = df_raw.withColumn("target_table", F.lit(args.force_target_table))
    else:
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
    
    # Dédoublonnage par contenu (Evite de stocker N fois la même page scannée sous des URLs différentes)
    logger.info("Dropping duplicates based on content_hash...")
    df_enriched = df_enriched.dropDuplicates(["content_hash"])
    
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

    # --- GATHER METRICS BEFORE CLEANUP ---
    format_rows = []
    table_rows = []
    try:
        if "content_type" in df_checkpoint.columns:
            format_rows = df_checkpoint.groupBy("content_type").count().collect()
        table_rows = df_checkpoint.groupBy("target_table").count().collect()
    except Exception as e:
        logger.warning(f"Could not calculate detailed stats: {e}")

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
    volumetrie_breakdown = f"[VOLUMÉTRIE] Tables Hudi Ciblées  : {len(tables_in_batch)}"
    
    try:
        if format_rows:
            format_stats = "\n" + format_table("FORMATS", format_rows) + "\n"
        if table_rows:
            table_stats = "\n" + format_table("HUDI TABLES", table_rows) + "\n"
            # Générer le détail par table pour la section VOLUMÉTRIE
            volumetrie_breakdown = "\n".join([f"[VOLUMÉTRIE] Table '{str(row[0]):<20}' : {row[1]:>4} documents" for row in table_rows])
    except Exception as e:
        logger.warning(f"Could not format detailed stats: {e}")


    # --- METRICS DASHBOARD ---
    end_time = time.time()
    duration_sec = end_time - start_time
    throughput = total_docs / duration_sec if duration_sec > 0 else 0
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    ram_mb = mem_info.rss / (1024 * 1024)
    cpu_percent = psutil.cpu_percent()

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
{volumetrie_breakdown}
--------------------------------------------------
[PERFORMANCES] Débit Traitement   : {throughput:.2f} docs/sec
==================================================
"""
    logger.info(dashboard)
    platform_logger.log_job_status(job_name, "SUCCESS")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"SILVER JOB FAILED: {e}")
        import sys
        if 'platform_logger' in locals() and 'job_name' in locals():
            platform_logger.log_job_status(job_name, "FAILED", str(e))
        else:
            # Try to log even if we failed early, we can extract from sys.argv
            try:
                from processing.core.utils.platform_logger import PlatformLogger
                pl = PlatformLogger()
                import argparse
                parser = argparse.ArgumentParser()
                parser.add_argument("--source_system", required=False, default="unknown")
                args, _ = parser.parse_known_args()
                pl.log_job_status(f"spark_process_raw_to_hudi_{args.source_system}", "FAILED", str(e))
            except:
                pass
        sys.exit(1)
