# ==============================================================================
# © 2026 University Data Platform
# Ce fichier fait partie du système central de la Plateforme Data Universitaire.
# ==============================================================================

"""
Chargeur de Configuration de Routage (Zone Silver)
--------------------------------------------------
Module chargé d'ingérer et de valider les règles métier (YAML) spécifiques
à chaque institution (ex: FSAC, FSTM). Ces règles de routage déterminent
vers quelles tables Hudi les documents bruts seront ventilés.
"""

import os
import yaml
from typing import Dict, Any, List

class ConfigLoader:
    """
    Charge et valide le fichier de configuration YAML de l'université.
    
    Responsabilités :
    - Vérification de l'existence du fichier physique.
    - Désérialisation sécurisée (safe_load) du YAML.
    - Accès typé aux métadonnées (input_bucket, entities).
    """
    
    def __init__(self, config_path: str):
        """
        Initialise le chargeur de configuration.
        
        Args:
            config_path (str): Chemin absolu ou relatif vers le fichier rules.yaml.
            
        Raises:
            FileNotFoundError: Si le fichier de configuration spécifié est introuvable.
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Fichier de configuration introuvable : {config_path}")
        self.config_path = config_path
        self.config = self._load_yaml()

    def _load_yaml(self) -> Dict[str, Any]:
        """Charge de manière sécurisée le contenu du fichier YAML."""
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    @property
    def input_bucket(self) -> str:
        """Nom du bucket MinIO source par défaut (raw-web-html)."""
        return self.config.get("input_bucket", "raw-web-html")

    @property
    def entities(self) -> List[Dict[str, Any]]:
        """
        Extrait la liste des entités (règles de routage).
        
        Returns:
            List[Dict[str, Any]]: Liste contenant les clés 'target_table', 'url_pattern' et 'mappings'.
        """
        return self.config.get("entities", [])
