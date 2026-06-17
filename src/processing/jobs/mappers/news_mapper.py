# pyrefly: ignore [missing-import]
from pyspark.sql.functions import col, lit, current_timestamp, md5, when
import sys
import os

# Ensure parsers module can be loaded
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from parsers.html_parser import parse_html_udf

def map_html_to_hudi_schema(df):
    """
    Applies HTML parsing UDF and maps columns to the final Hudi schema.
    Categorizes the record into the correct Hudi table based on the URL.
    """
    # 1. Parse HTML content
    parsed_df = df.withColumn("parsed", parse_html_udf(col("raw_content")))
    
    # 2. Map required Hudi fields
    mapped_df = parsed_df \
        .withColumn("title", col("parsed.title")) \
        .withColumn("normalized_text", col("parsed.normalized_text")) \
        .withColumn("language", col("parsed.language")) \
        .withColumn("content_hash", col("parsed.content_hash")) \
        .withColumn("record_id", md5(col("source_url"))) \
        .withColumn("source_system", lit("web_crawler")) \
        .withColumn("crawl_timestamp", current_timestamp()) \
        .withColumn("business_timestamp", current_timestamp()) \
        .withColumn("is_deleted", lit(False))
        
    # 3. Categorize based on URL logic
    categorized_df = mapped_df.withColumn(
        "target_table",
        when(col("source_url").rlike("(?i)actualites|news|event|annonce|agenda|recrut|events|partenariat|evenements|offre|jobs|stage|fetes|manifestations|journees|portes_ouvertes|manifestation"), "university_news")
        .when(col("source_url").rlike("(?i)enseignants|prof|personnel|dean"), "faculty_profiles")
        .when(col("source_url").rlike("(?i)formation|master|cursus|filiere|licence|doctorat|diplome"), "course_catalog")
        .when(col("source_url").rlike("(?i)recherche|publications|laboratoire|phd"), "research_publications")
        .otherwise("unclassified_documents")
    )
    
    # 4. Select the final columns requested
    final_cols = [
        "record_id", "source_system", "source_url", "content_hash",
        "crawl_timestamp", "business_timestamp", "is_deleted", 
        "language", "normalized_text", "title", "target_table"
    ]
    
    return categorized_df.select(*final_cols)
