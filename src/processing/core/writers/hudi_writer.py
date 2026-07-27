# ==============================================================================
# © 2026 University Data Platform
# Ce fichier fait partie du système central de la Plateforme Data Universitaire.
# ==============================================================================

"""
Connecteur Apache Hudi (Lakehouse / Zone Silver & Gold)
-------------------------------------------------------
Module gérant l'écriture transactionnelle (ACID) des DataFrames Spark sur MinIO.
Assure l'intégrité des données via des opérations UPSERT (Insert/Update) basées
sur les clés primaires métier (record_id).
"""

from pyspark.sql import DataFrame

class HudiWriter:
    """
    Gestionnaire d'écriture Data Lakehouse (Apache Hudi).
    
    Architecture (DataOps) :
    - Mode 'UPSERT' natif : Met à jour les documents modifiés sans dupliquer l'historique brut.
    - Partitionnement par 'source_system' pour accélérer les requêtes analytiques (Pruning).
    - Optimisé avec l'index 'BLOOM' pour la recherche rapide des clés lors de l'Upsert.
    """
    def __init__(self, base_path: str = "s3a://curated-zone"):
        """
        Initialise le writer Hudi.
        
        Args:
            base_path (str): URI S3A du bucket de destination (Lakehouse).
        """
        self.base_path = base_path

    def upsert(self, df: DataFrame, table_name: str):
        """
        Exécute une opération UPSERT sur la table Hudi cible.
        
        Args:
            df (DataFrame): Le DataFrame Spark transformé à persister.
            table_name (str): Nom de la table Hudi (ex: faculty_profiles).
        """
        table_path = f"{self.base_path}/{table_name}"
        
        hudi_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': 'record_id',
            'hoodie.datasource.write.partitionpath.field': 'source_system',
            'hoodie.datasource.write.precombine.field': 'crawl_timestamp',
            'hoodie.datasource.write.operation': 'upsert',
            # Optimisations I/O (DataOps)
            'hoodie.upsert.shuffle.parallelism': 8, 
            'hoodie.insert.shuffle.parallelism': 8,
            # Indexation rapide
            'hoodie.index.type': 'BLOOM',
            # Évolution automatique du schéma
            'hoodie.datasource.write.reconcile.schema': 'true',
        }
        
        df.write.format("hudi"). \
            options(**hudi_options). \
            mode("append"). \
            save(table_path)
