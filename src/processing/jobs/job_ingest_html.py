import logging
import sys
import os
# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession

# Add parent directory to path to allow importing modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from mappers.news_mapper import map_html_to_hudi_schema
from utils.hudi_writer import write_to_hudi

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session():
    # Read MinIO credentials from env variables (injected by docker-compose)
    s3_endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000")
    s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    return (SparkSession.builder
            .appName("Ingest_HTML_To_Hudi")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.executorEnv.PYTHONPATH", "/opt/spark/jobs")
            .getOrCreate())

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("=========================================")
    logger.info("STARTING HTML INGESTION JOB")
    logger.info("=========================================")
    
    # Use wildcards to match nested directories: domain/year/month/day/file
    input_path = "s3a://raw-web-html/*/*/*/*/*" 
    base_output_path = "s3a://curated-zone"
    
    try:
        # 1. Read all files from the MinIO bucket
        rdd = spark.sparkContext.wholeTextFiles(input_path)
        
        # Filter to keep ONLY .html files (exclude _metadata.json and others)
        rdd = rdd.filter(lambda x: x[0].endswith(".html"))
        
        if rdd.isEmpty():
            logger.info("No HTML files found matching the path. Exiting.")
            return
            
        raw_df = rdd.toDF(["source_url", "raw_content"])
        
        num_files = raw_df.count()
        logger.info(f"-> Nombre de fichiers HTML lus : {num_files}")
        
        if num_files == 0:
            logger.info("No files found. Exiting.")
            return

        # 2. Parse HTML and Map to Hudi schema
        mapped_df = map_html_to_hudi_schema(raw_df)
        mapped_df.cache() # Cache because we will filter multiple times
        
        num_parsed = mapped_df.count()
        logger.info(f"-> Nombre de pages parsées : {num_parsed}")
        
        # 3. Get distinct tables to write
        tables_df = mapped_df.select("target_table").distinct().collect()
        tables = [row['target_table'] for row in tables_df]
        
        total_written = 0
        
        # 4. Write each category to its respective Hudi table
        for table_name in tables:
            table_df = mapped_df.filter(mapped_df.target_table == table_name).drop("target_table")
            count = table_df.count()
            logger.info(f"Préparation de l'écriture de {count} enregistrements pour la table : {table_name}")
            
            # Call Hudi writer
            write_to_hudi(table_df, table_name, base_output_path)
            total_written += count
            
        logger.info(f"-> Nombre total d'enregistrements écrits dans Hudi : {total_written}")
        logger.info("HTML Ingestion Job completed successfully.")
        
    except Exception as e:
        logger.error(f"Job failed during execution: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
