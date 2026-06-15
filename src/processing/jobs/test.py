# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import *
# pyrefly: ignore [missing-import]
from pyspark.sql.types import *
  

# Initialize a SparkSession with Hudi support
spark = (
    SparkSession.builder
    .appName("Hudi_Test")
    .master("spark://spark-master:7077")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .getOrCreate()
)

# to not overload our CLI
spark.sparkContext.setLogLevel("WARN")

# Check if Spark is working
print("Spark Session Created Successfully!")

# pour tester : docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/test.py