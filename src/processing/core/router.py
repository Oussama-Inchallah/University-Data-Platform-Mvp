# ==============================================================================
# © 2026 University Data Platform
# Ce fichier fait partie du système central de la Plateforme Data Universitaire.
# ==============================================================================

"""
Routeur Sémantique Dynamique (Spark / DataOps)
----------------------------------------------
Moteur de classification de la plateforme. Il applique de manière distribuée (Spark)
les expressions régulières (Regex) issues de la configuration métier (YAML) sur
les URLs et les Titres pour déterminer la table Hudi de destination finale.
"""

import urllib.parse
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, udf
from pyspark.sql.types import StringType
from typing import List, Dict, Any

class DynamicRouter:
    """
    Routeur de données basé sur des règles.
    Permet la ventilation des documents bruts (Data Lake) vers les tables analytiques (Lakehouse).
    """
    
    @staticmethod
    def route_dataframe(df: DataFrame, entities_config: List[Dict[str, Any]]) -> DataFrame:
        """
        Applique la logique de routage métier sur un DataFrame Spark distribué.
        
        Fonctionnement :
        1. Décodage de l'URL (urllib.unquote) pour la gestion correcte des accents/caractères spéciaux.
        2. Construction dynamique d'une expression Spark SQL 'CASE WHEN' basée sur les regex.
        3. Assignation de la table cible ou fallback sur le registre documentaire par défaut.
        
        Args:
            df (DataFrame): Le DataFrame Spark contenant les données brutes.
            entities_config (List[Dict[str, Any]]): Règles métier issues du ConfigLoader.
            
        Returns:
            DataFrame: Le DataFrame original enrichi de la colonne analytique 'target_table'.
        """
        if not entities_config:
            # Fallback de sécurité : routage global vers le registre par défaut
            return df.withColumn("target_table", lit("documents_registry"))
            
        # Enregistrement de l'UDF pour le décodage des URLs HTTP (ex: %20 -> espace)
        decode_url_udf = udf(lambda url: urllib.parse.unquote(url) if url else "", StringType())
        
        df_routed = df.withColumn("decoded_url", decode_url_udf(col("source_url")))
        
        route_expr = None
        
        for entity in entities_config:
            target = entity.get("target_table")
            pattern = entity.get("url_pattern")
            
            if not target or not pattern:
                continue
                
            # Évaluation du pattern (insensible à la casse) sur l'URL décodée OU le titre du document
            condition = col("decoded_url").rlike(pattern) | col("title").rlike(pattern)
            
            if route_expr is None:
                route_expr = when(condition, lit(target))
            else:
                route_expr = route_expr.when(condition, lit(target))
                
        if route_expr is None:
            route_expr = lit("documents_registry")
        else:
            # Classement par défaut pour les contenus non catégorisés
            route_expr = route_expr.otherwise(lit("documents_registry"))
            
        # Application de l'arbre conditionnel et nettoyage de la colonne temporaire
        df_routed = df_routed.withColumn("target_table", route_expr).drop("decoded_url")
        
        return df_routed
