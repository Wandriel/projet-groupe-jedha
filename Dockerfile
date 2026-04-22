# 1. Utiliser une version légère de Python
FROM python:3.9-slim

# 2. Définir le dossier de travail dans le conteneur
WORKDIR /app

# 3. Installer les dépendances système nécessaires pour psycopg2 (PostgreSQL)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 4. Copier le fichier des dépendances Python
COPY requirements.txt .

# 5. Installer les bibliothèques Python
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copier tout le reste du code source (.py et dossiers)
COPY . .

# 7. La commande par défaut au lancement du conteneur
# On lance le script de pilotage run_all_py.py
CMD ["python", "run_all_py.py"]