# ==============================================================================
# © 2026 University Data Platform
# Ce fichier fait partie du système central de la Plateforme Data Universitaire.
# ==============================================================================

"""
Nettoyeur de Données (Transformations Spark)
--------------------------------------------
Module utilitaire fournissant des fonctions natives Spark (UDFs et SQL Expressions)
pour le nettoyage sémantique, la génération de clés métier déterministes (hachage)
et la détection linguistique.
"""

from langdetect import detect, DetectorFactory
from pyspark.sql.functions import udf
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

DetectorFactory.seed = 0

def _detect_language(text: str) -> str:
    """Fonction atomique de détection de langue (wrapper robuste)."""
    if not text or len(text.strip()) < 15:
        return "unknown"
    try:
        return detect(text)
    except Exception:
        return "unknown"

detect_language_udf = udf(_detect_language, StringType())

class DataCleaner:
    """
    Collection de transformateurs de données Spark.
    
    Architecture (DataOps) :
    - Privilégie le moteur C/JVM de Spark (F.col, F.sha2) pour éviter l'overhead
      de sérialisation Python/JVM lors de l'ETL massif.
    """
    
    @staticmethod
    def generate_record_id(source_system_col: str, source_url_col: str):
        """
        Génère une clé primaire métier unique (UUID déterministe).
        
        Args:
            source_system_col (str): Nom de la colonne source système.
            source_url_col (str): Nom de la colonne URL.
            
        Returns:
            Column: Expression Spark SQL calculant le SHA-256 combiné.
        """
        return F.sha2(F.concat_ws("_", F.col(source_system_col), F.col(source_url_col)), 256)

    @staticmethod
    def generate_content_hash(text_col: str, fallback_col: str = "record_id"):
        """
        Calcule l'empreinte cryptographique du contenu pour la déduplication (Upsert Hudi).
        
        Args:
            text_col (str): Colonne contenant le texte brut.
            fallback_col (str): Colonne de secours si le texte est vide (ex: PDF scanné sans OCR).
            
        Returns:
            Column: Expression Spark SQL générant le content_hash.
        """
        return F.when(
            (F.col(text_col).isNull()) | (F.length(F.trim(F.col(text_col))) == 0),
            F.col(fallback_col)
        ).otherwise(
            F.sha2(F.col(text_col), 256)
        )
        
    @staticmethod
    def clean_whitespaces(text_col: str):
        """Standardise les espaces (Regex native Spark) pour optimiser le modèle NLP."""
        return F.trim(F.regexp_replace(F.col(text_col), r"\s+", " "))
