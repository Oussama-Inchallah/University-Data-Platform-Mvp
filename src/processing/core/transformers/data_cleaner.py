"""
Nettoyeur de Données (Transformations)
--------------------------------------
Ce module contient des UDFs Spark pour nettoyer le texte :
- Détection de la langue (langdetect)
- Génération d'un ID unique (hachage de l'URL)
- Génération d'un Content Hash (détection de doublons)
"""
from langdetect import detect, DetectorFactory
from pyspark.sql.functions import udf
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

# Assure que la détection de langue est déterministe (même texte = même résultat)
DetectorFactory.seed = 0

def _detect_language(text: str) -> str:
    """
    Détecte la langue principale d'un texte.
    Retourne 'unknown' si le texte est trop court ou s'il y a une erreur.
    """
    if not text or len(text.strip()) < 15:
        return "unknown"
    try:
        return detect(text)
    except Exception:
        return "unknown"

# UDF Spark pour la détection de langue
detect_language_udf = udf(_detect_language, StringType())

class DataCleaner:
    """
    Collection d'utilitaires Spark pour le nettoyage et l'enrichissement.
    Privilégie les fonctions natives Spark (C/JVM) pour des performances maximales.
    """
    
    @staticmethod
    def generate_record_id(source_system_col: str, source_url_col: str):
        """
        Génère la clé primaire métier (record_id) via SHA-256.
        Fonction 100% native Spark (très rapide).
        """
        # Concaténation de 'FSAC' et de 'http://...' puis hash SHA256
        return F.sha2(F.concat_ws("_", F.col(source_system_col), F.col(source_url_col)), 256)

    @staticmethod
    def generate_content_hash(text_col: str, fallback_col: str = "record_id"):
        """
        Génère l'empreinte du contenu (content_hash) pour la déduplication.
        Si le texte est vide (ex: image sans OCR), utilise fallback_col (record_id)
        pour éviter que tous les documents vides ne soient considérés comme des doublons !
        """
        return F.when(
            (F.col(text_col).isNull()) | (F.length(F.trim(F.col(text_col))) == 0),
            F.col(fallback_col)
        ).otherwise(
            F.sha2(F.col(text_col), 256)
        )
        
    @staticmethod
    def clean_whitespaces(text_col: str):
        """
        Nettoie les espaces multiples en utilisant les regex natives Spark.
        """
        return F.trim(F.regexp_replace(F.col(text_col), r"\s+", " "))
