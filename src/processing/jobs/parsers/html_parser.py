# pyrefly: ignore [missing-import]
from pyspark.sql.types import StructType, StructField, StringType
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import udf
import re
import hashlib

def parse_html_python(html_content):
    """
    Parses HTML content, extracts title, language, and normalized text.
    """
    try:
        # pyrefly: ignore [missing-import]
        from bs4 import BeautifulSoup
    except ImportError:
        # Fallback if bs4 is not installed on workers, though it's required.
        return ("", "", "fr", "")

    if not html_content:
        return ("", "", "fr", "")
        
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Extract title
    title_tag = soup.find("title")
    title = title_tag.text.strip() if title_tag else ""
    
    # Extract language
    html_tag = soup.find("html")
    lang = html_tag.get("lang", "fr") if html_tag else "fr"
    
    # Extract full text without HTML tags
    text = soup.get_text(separator=' ')
    
    # Normalize: strip extra spaces and newlines
    normalized_text = re.sub(r'\s+', ' ', text).strip()
    
    # Ensure UTF-8 (though python handles unicode strings, we sanitize)
    normalized_text = normalized_text.encode('utf-8', 'ignore').decode('utf-8')
    
    # Generate content hash
    content_hash = hashlib.md5(normalized_text.encode('utf-8')).hexdigest()
    
    return (title, normalized_text, lang, content_hash)

# Define the schema of the parsed data
html_parse_schema = StructType([
    StructField("title", StringType(), True),
    StructField("normalized_text", StringType(), True),
    StructField("language", StringType(), True),
    StructField("content_hash", StringType(), True)
])

# Create the PySpark UDF
parse_html_udf = udf(parse_html_python, html_parse_schema)
