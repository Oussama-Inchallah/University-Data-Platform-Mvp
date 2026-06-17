import yaml
import os
from typing import Dict, Any, List

class ConfigLoader:
    """
    Charge et valide le fichier de configuration YAML spécifique à une université.
    """
    def __init__(self, config_path: str):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        self.config_path = config_path
        self.config = self._load_yaml()

    def _load_yaml(self) -> Dict[str, Any]:
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    @property
    def input_bucket(self) -> str:
        return self.config.get("input_bucket", "raw-web-html")

    @property
    def entities(self) -> List[Dict[str, Any]]:
        """
        Retourne la liste des règles de routage (target_table, url_pattern, mappings).
        """
        return self.config.get("entities", [])
