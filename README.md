# University Data Platform MVP

Projet MVP de Data Engineering permettant l'ingestion, le traitement et l'analyse de données universitaires.

## Installation rapide

1. **Cloner le projet :**
  
  ```bash
   git clone https://github.com/Oussama-Inchallah/University-Data-Platform-Mvp.git
   cd "University-Data-Platform-Mvp"
   ```

2. **Installer l'environnement Python :**

   ```bash
   python -m venv udp-env
   source udp-env/bin/activate  # Sur Windows: .\udp-env\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Démarrer l'infrastructure (MinIO) :**

   ```bash
   # Accéder au dossier
   cd infra\docker

   # Créer le fichier .env
   cp .env.example .env
   
   # Lancer Docker
   docker compose up -d

   # Revenir à la racine
   cd ../..

   ```
