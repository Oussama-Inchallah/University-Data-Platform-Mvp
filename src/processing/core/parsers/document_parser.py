import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

def extract_document_content(binary_data: bytes) -> Dict[str, str]:
    """
    Extrait le texte et le titre d'un document binaire (PDF, DOCX, XLS, etc.) via Apache Tika.
    
    Returns:
        Dict contenant 'text' (texte normalisé) et 'title' (titre s'il existe).
        En cas d'erreur ou si le document est vide, retourne des chaînes vides et log l'erreur.
    """
    if not binary_data:
        return {"text": "", "title": ""}
        
    try:
        from tika import parser
        # Utilisation de Tika pour parser le buffer binaire.
        # Note: Lors de la première exécution sur un worker, tika-python téléchargera le tika-server.jar.
        parsed = parser.from_buffer(binary_data)
        
        content = parsed.get("content", "")
        metadata = parsed.get("metadata", {})
        
        if content is None:
            content = ""
            
        # Nettoyage très basique du texte (le DataCleaner s'occupera du reste)
        text = " ".join(content.split())
        
        # Tentative de récupération d'un titre depuis les métadonnées Tika
        # Les métadonnées peuvent être sous forme de liste ou de chaîne
        title = metadata.get("title", "")
        if isinstance(title, list):
            title = title[0] if title else ""
        if not title:
            # Fallback sur un autre champ métadonnées classique
            title = metadata.get("dc:title", "")
            if isinstance(title, list):
                title = title[0] if title else ""
        
        return {"text": text, "title": title}
        
    except Exception as e:
        # Enregistrement explicite de l'erreur dans les logs Spark sans faire crasher le worker
        logger.error(f"Erreur lors du parsing du document via Tika: {str(e)}")
        return {"text": "", "title": ""}

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf

# Définition du schéma de retour de la UDF
document_schema = StructType([
    StructField("text", StringType(), True),
    StructField("title", StringType(), True)
])

# Création de la UDF PySpark
parse_document_udf = udf(extract_document_content, document_schema)
