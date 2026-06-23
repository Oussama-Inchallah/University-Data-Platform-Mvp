"""
Job Spark - Zone Analytique : Sync Hudi -> Hive Metastore
---------------------------------------------------------
Ce script découvre dynamiquement toutes les tables Hudi présentes dans MinIO
(Zone Curée) et les enregistre dans le catalogue Hive.
Cela permet à Metabase d'interroger directement ces 7+ tables en SQL via le Thrift Server.
"""
import sys
import argparse
import logging
import os
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Initialisation de la Spark Session configurée pour Hudi et MinIO
    spark = SparkSession.builder \
        .appName("Sync_Hudi_To_Hive_Catalog") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .enableHiveSupport() \
        .getOrCreate()

    # Connexion au FileSystem Hadoop (MinIO S3A)
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    curated_zone_path = "s3a://curated-zone/"
    path_obj = Path(curated_zone_path)
    fs = path_obj.getFileSystem(sc._jsc.hadoopConfiguration())

    logger.info(f"Scanning {curated_zone_path} for Hudi tables...")

    try:
        # Lister tous les dossiers dans la curated-zone
        statuses = fs.listStatus(Path(curated_zone_path))
        
        for status in statuses:
            if status.isDirectory():
                table_name = status.getPath().getName()
                table_path = f"{curated_zone_path}{table_name}"
                
                # Vérifier si c'est bien une table Hudi en cherchant le dossier .hoodie
                is_hudi = fs.exists(Path(f"{table_path}/.hoodie"))
                
                if is_hudi:
                    logger.info(f"Found Hudi table: {table_name}. Registering in Hive Metastore...")
                    try:
                        # Création de la table externe dans Hive via Spark SQL
                        spark.sql(f"CREATE DATABASE IF NOT EXISTS default")
                        spark.sql(f"USE default")
                        # La syntaxe USING hudi LOCATION permet au catalogue d'inférer automatiquement le schéma
                        spark.sql(f"""
                            CREATE TABLE IF NOT EXISTS {table_name} 
                            USING hudi 
                            LOCATION '{table_path}'
                        """)
                        logger.info(f"✅ Successfully registered {table_name}.")
                    except Exception as e:
                        logger.error(f"❌ Failed to register {table_name}: {str(e)}")
                else:
                    logger.info(f"Skipping {table_name}: Not a Hudi table (no .hoodie folder).")
                    
    except Exception as e:
        logger.error(f"Could not read from MinIO curated-zone: {str(e)}")
        
    logger.info("Catalog sync complete.")
    spark.stop()

if __name__ == "__main__":
    main()
