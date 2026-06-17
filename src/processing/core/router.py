from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit
from typing import List, Dict, Any

class DynamicRouter:
    """
    Module responsable du routage des données brutes vers les tables Hudi cibles.
    """
    
    @staticmethod
    def route_dataframe(df: DataFrame, entities_config: List[Dict[str, Any]]) -> DataFrame:
        """
        Ajoute une colonne 'target_table' au DataFrame en fonction des règles regex (url_pattern) du YAML.
        L'approche utilise des chaînes `when` Spark natives pour une performance optimale (sans UDF).
        """
        if not entities_config:
            # Fallback de sécurité si le YAML est vide
            return df.withColumn("target_table", lit("documents_registry"))
            
        route_expr = None
        
        for entity in entities_config:
            target = entity.get("target_table")
            pattern = entity.get("url_pattern")
            
            if not target or not pattern:
                continue
                
            if route_expr is None:
                route_expr = when(col("source_url").rlike(pattern), lit(target))
            else:
                route_expr = route_expr.when(col("source_url").rlike(pattern), lit(target))
                
        if route_expr is None:
            route_expr = lit("documents_registry")
        else:
            # Tout ce qui ne matche pas va dans le registre documentaire par défaut
            route_expr = route_expr.otherwise(lit("documents_registry"))
            
        return df.withColumn("target_table", route_expr)
