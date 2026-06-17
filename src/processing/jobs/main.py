import sys
import argparse
import logging
from pyspark.sql import SparkSession

# Imports des modules core (assumant que src/ est dans le PYTHONPATH lors du spark-submit)
try:
    from processing.core.config_loader import ConfigLoader
    from processing.core.router import DynamicRouter
    from processing.core.parsers.html_parser import parse_html_udf
    from processing.core.transformers.data_cleaner import DataCleaner, detect_language_udf
    from processing.core.writers.hudi_writer import HudiWriter
    from processing.core.writers.es_writer import ElasticsearchWriter
except ImportError:
    # Fallback si exécuté directement sans PYTHONPATH
    import os
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
    from processing.core.config_loader import ConfigLoader
    from processing.core.router import DynamicRouter
    from processing.core.parsers.html_parser import parse_html_udf
    from processing.core.transformers.data_cleaner import DataCleaner, detect_language_udf
    from processing.core.writers.hudi_writer import HudiWriter
    from processing.core.writers.es_writer import ElasticsearchWriter

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
    args = parser.parse_args()

    logger.info(f"Starting processing job with config={args.config}, date={args.execution_date}, source_system={args.source_system}")

    # 1. Chargement de la configuration YAML
    loader = ConfigLoader(args.config)
    source_system = args.source_system
    
    # 2. Initialisation Spark
    spark = get_spark_session(f"UniversityData_Process_{source_system}")
    
    # 3. Lecture depuis MinIO (Incremental load basé sur execution_date)
    try:
        year, month, day = args.execution_date.split("-")
        domain_pattern = f"*{source_system.lower()}*"
        base_path_html = f"s3a://{loader.input_bucket}/{domain_pattern}/year={year}/month={month}/day={day}/*.html"
        base_path_json = f"s3a://{loader.input_bucket}/{domain_pattern}/year={year}/month={month}/day={day}/*_metadata.json"
        
        logger.info(f"Reading HTML files from {base_path_html}")
        # Lecture du contenu des fichiers (RDD converti en DataFrame)
        rdd_html = spark.sparkContext.wholeTextFiles(base_path_html)
        if rdd_html.isEmpty():
            logger.error("No HTML files found for this date.")
            return
            
        df_html = rdd_html.toDF(["raw_path", "content"])
        
        logger.info(f"Reading Metadata JSON from {base_path_json}")
        df_meta = spark.read.option("multiLine", "true").json(base_path_json)
        
        # Reconstruction du raw_path s3a pour la jointure
        df_meta = df_meta.withColumn(
            "raw_path_meta", 
            F.concat(F.lit("s3a://"), F.col("storage_bucket"), F.lit("/"), F.col("storage_path"))
        )
        
        # Jointure pour rassembler le contenu HTML et ses métadonnées (URL, timestamps...)
        df_raw = df_html.join(df_meta, df_html.raw_path == df_meta.raw_path_meta, how="inner")
        
        raw_count = df_raw.count()
        logger.info(f"===> DEBUG: Rows after join = {raw_count}")
        if raw_count == 0:
            logger.info("===> DEBUG: df_html and df_meta did not join. Here is a sample of paths:")
            df_html.select("raw_path").show(3, truncate=False)
            df_meta.select("raw_path_meta", "source_url").show(3, truncate=False)
        
    except Exception as e:
        logger.error(f"Error reading from MinIO: {e}")
        return
    
    # 4. Routage dynamique
    df_routed = DynamicRouter.route_dataframe(df_raw, loader.entities)
    routed_count = df_routed.count()
    logger.info(f"===> DEBUG: Rows after routing = {routed_count}")
    
    # 5. Parsers & Normalisation (Application du Schéma Commun)
    df_normalized = df_routed \
        .withColumn("source_system", F.lit(source_system)) \
        .withColumn("source_url", F.col("source_url")) \
        .withColumn("crawl_timestamp", F.col("crawl_timestamp").cast("timestamp")) \
        .withColumn("ingestion_timestamp", F.current_timestamp()) \
        .withColumn("is_deleted", F.lit(False)) \
        .withColumn("parsed_html", parse_html_udf(F.col("content"))) \
        .withColumn("title", F.col("parsed_html.title")) \
        .withColumn("normalized_text", F.col("parsed_html.text")) \
        .drop("parsed_html") \
        .withColumn("language", detect_language_udf(F.col("normalized_text")))
    # 6. Nettoyage et Hachage
    df_enriched = df_normalized \
        .withColumn("record_id", DataCleaner.generate_record_id("source_system", "source_url")) \
        .withColumn("content_hash", DataCleaner.generate_content_hash("normalized_text")) \
        .withColumn("business_timestamp", F.col("crawl_timestamp")) \
        .drop("content", "raw_path", "raw_path_meta") # OPTIMISATION : Suppression du HTML brut
        
    # 6. Écriture Hudi & ES (Ventilation Dynamique)
    # On cache le DataFrame enrichi en mémoire pour éviter de le recalculer pour chaque table
    df_enriched.cache()
    
    # On récupère la liste unique des tables cibles trouvées dans ce batch
    tables_in_batch = [row["target_table"] for row in df_enriched.select("target_table").distinct().collect()]
    logger.info(f"Entities found in this batch: {tables_in_batch}")
    
    hudi_writer = HudiWriter(base_path="s3a://curated-zone")
    # es_writer = ElasticsearchWriter()  # Désactivé pour le moment (MVP) car le conteneur elasticsearch n'est pas présent
    
    for table_name in tables_in_batch:
        logger.info(f"Writing data to Hudi table: {table_name}")
        # On filtre le DataFrame pour ne garder que l'entité en cours
        df_entity = df_enriched.filter(F.col("target_table") == table_name)
        
        # Upsert dans la table Hudi spécifique (ex: faculty_profiles)
        hudi_writer.upsert(df_entity, table_name)
        
    # logger.info("Writing all entities to global Elasticsearch index: university_content")
    # On écrit tout d'un coup dans le moteur de recherche global
    # es_writer.write_index(df_enriched, "university_content")
    
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
                v = str(row[1])
                lines.append(f"| {k:<33} | {v:<10} |")
            lines.append(f"+{'-'*35}+{'-'*12}+")
            return "\n" + "\n".join(lines)

        content_rows = df_meta.groupBy("content_type").count().collect()
        status_rows = df_meta.groupBy("http_status").count().collect()
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
