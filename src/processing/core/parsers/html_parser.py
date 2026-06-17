from bs4 import BeautifulSoup
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType
from typing import Dict

def _parse_html_content(html_content: str) -> Dict[str, str]:
    """
    Extrait le titre et le texte utile d'une page HTML.
    Nettoie les balises invisibles ou structurelles (scripts, styles, nav...).
    """
    if not html_content:
        return {"title": "", "text": ""}
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extraction du titre
        title_tag = soup.find('title')
        title = title_tag.get_text(strip=True) if title_tag else ""
        if not title:
            h1_tag = soup.find('h1')
            title = h1_tag.get_text(strip=True) if h1_tag else ""
        
        # Suppression des éléments non textuels ou parasites
        for element in soup(["script", "style", "noscript", "nav", "footer", "header"]):
            element.decompose()
            
        text = soup.get_text(separator=' ')
        
        # Nettoyage des espaces multiples et sauts de ligne
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return {"title": title, "text": text}
    except Exception:
        # En cas de page malformée, on retourne vide
        return {"title": "", "text": ""}

# Définition du schéma de retour de l'UDF
parse_html_schema = StructType([
    StructField("title", StringType(), True),
    StructField("text", StringType(), True)
])

# UDF (User Defined Function) Spark prête à l'emploi
parse_html_udf = udf(_parse_html_content, parse_html_schema)
