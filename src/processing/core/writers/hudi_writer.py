from pyspark.sql import DataFrame

class HudiWriter:
    """
    Gestionnaire d'écriture vers le Data Lakehouse Apache Hudi (Curated Zone).
    """
    def __init__(self, base_path: str = "s3a://curated-zone"):
        """
        :param base_path: Le bucket MinIO de destination.
        """
        self.base_path = base_path

    def upsert(self, df: DataFrame, table_name: str):
        """
        Effectue une opération UPSERT (Insert/Update) dans la table Hudi spécifiée.
        Utilise les colonnes communes exigées dans le schéma cible.
        """
        table_path = f"{self.base_path}/{table_name}"
        
        hudi_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': 'record_id',
            'hoodie.datasource.write.partitionpath.field': 'source_system',
            'hoodie.datasource.write.precombine.field': 'crawl_timestamp',
            'hoodie.datasource.write.operation': 'upsert',
            # Optimisations pour le MVP local (évite de spawner trop de tasks)
            'hoodie.upsert.shuffle.parallelism': 2, 
            'hoodie.insert.shuffle.parallelism': 2,
            # Indexation de base
            'hoodie.index.type': 'BLOOM',
            # Configuration Hive Metastore (Optionnel, à ajuster si Hive est sécurisé)
            # 'hoodie.datasource.hive_sync.enable': 'true',
            # 'hoodie.datasource.hive_sync.database': 'default',
            # 'hoodie.datasource.hive_sync.table': table_name,
        }
        
        # Le mode 'append' avec l'opération 'upsert' de Hudi met à jour les existants ou ajoute les nouveaux
        df.write.format("hudi"). \
            options(**hudi_options). \
            mode("append"). \
            save(table_path)
