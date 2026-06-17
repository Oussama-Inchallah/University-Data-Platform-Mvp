"""
Script de vérification des tables Hudi dans la curated-zone.
Usage: docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/verify_curated.py
"""
import os
# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession

def get_spark_session():
    s3_endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000")
    s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    return (SparkSession.builder
            .appName("Verify_Curated_Zone")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate())

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    base_path = "s3a://curated-zone"
    tables = [
        "unclassified_documents",
        "research_publications",
        "course_catalog",
        "faculty_profiles",
        "university_news"
    ]
    
    print("\n" + "=" * 70)
    print("  VÉRIFICATION DE LA CURATED ZONE (Apache Hudi)")
    print("=" * 70)
    
    for table_name in tables:
        path = f"{base_path}/{table_name}"
        try:
            df = spark.read.format("hudi").load(path)
            count = df.count()
            print(f"\n{'─' * 70}")
            print(f"  TABLE : {table_name}")
            print(f"  Nombre d'enregistrements : {count}")
            print(f"{'─' * 70}")
            
            # Show schema
            print("  Colonnes :")
            for field in df.schema.fields:
                print(f"    - {field.name} ({field.dataType.simpleString()})")
            
            # Show first 5 rows (truncated text for readability)
            print(f"\n  Aperçu (5 premières lignes) :")
            df.select(
                "_hoodie_partition_path",
                "_hoodie_file_name",
                "record_id",
                "source_system",
                "source_url",
                "language",
                "title",
                "normalized_text"
            ).show(2, truncate=False)
            
        except Exception:
            print(f"\n  TABLE : {table_name} -> (vide ou inexistante)")
    
    print("\n" + "=" * 70)
    print("  VÉRIFICATION TERMINÉE")
    print("=" * 70 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    main()
