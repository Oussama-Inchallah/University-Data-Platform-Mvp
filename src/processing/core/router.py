import urllib.parse
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, udf
from pyspark.sql.types import StringType
from typing import List, Dict, Any

class DynamicRouter:
    """
    Module responsable du routage des données brutes vers les tables Hudi cibles.
    """
    
    @staticmethod
    def route_dataframe(df: DataFrame, entities_config: List[Dict[str, Any]]) -> DataFrame:
        """
        Ajoute une colonne 'target_table' au DataFrame en fonction des règles regex (url_pattern) du YAML.
        L'approche décode l'URL brute et évalue le pattern sur l'URL décodée ET sur le titre.
        """
        if not entities_config:
            # Fallback de sécurité si le YAML est vide
            return df.withColumn("target_table", lit("documents_registry"))
            
        # UDF pour décoder l'URL (URL Unquote)
        decode_url_udf = udf(lambda url: urllib.parse.unquote(url) if url else "", StringType())
        
        # Ajouter une colonne temporaire avec l'URL décodée
        df_routed = df.withColumn("decoded_url", decode_url_udf(col("source_url")))
        
        route_expr = None
        
        for entity in entities_config:
            target = entity.get("target_table")
            pattern = entity.get("url_pattern")
            
            if not target or not pattern:
                continue
                
            # On cherche le pattern dans l'URL décodée OU dans le titre
            # RLIKE effectue une recherche regex. (?i) dans le YAML gère l'insensibilité à la casse.
            condition = col("decoded_url").rlike(pattern) | col("title").rlike(pattern)
            
            if route_expr is None:
                route_expr = when(condition, lit(target))
            else:
                route_expr = route_expr.when(condition, lit(target))
                
        if route_expr is None:
            route_expr = lit("documents_registry")
        else:
            # Tout ce qui ne matche pas va dans le registre documentaire par défaut
            route_expr = route_expr.otherwise(lit("documents_registry"))
            
        # Appliquer l'expression et supprimer la colonne temporaire 'decoded_url'
        df_routed = df_routed.withColumn("target_table", route_expr).drop("decoded_url")
        
        return df_routed
