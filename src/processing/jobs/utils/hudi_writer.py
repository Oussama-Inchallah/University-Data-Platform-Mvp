import logging

logger = logging.getLogger(__name__)

def write_to_hudi(df, table_name, base_path):
    """
    Writes a Spark DataFrame to Apache Hudi format in MinIO.
    """
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'record_id',
        'hoodie.datasource.write.partitionpath.field': '',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'crawl_timestamp',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2,
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE'
    }
    
    path = f"{base_path}/{table_name}"
    
    try:
        logger.info(f"Starting Hudi write operation for table '{table_name}' at '{path}'")
        
        df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(path)
            
        logger.info(f"Successfully wrote data to Hudi table '{table_name}'.")
    except Exception as e:
        logger.error(f"Failed to write to Hudi table '{table_name}': {e}")
