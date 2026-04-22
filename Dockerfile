FROM python:3.11-slim

WORKDIR /app

# Installation de tzdata pour l'heure de Paris
RUN apt-get update && apt-get install -y tzdata && rm -rf /var/lib/apt/lists/*
ENV TZ=Europe/Paris

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# On copie tout (y compris le dossier scripts_sql)
COPY . .

# C'est run_all_py.py qui lancera ton script_connexion_RDS.py
CMD ["python", "run_all_py.py"]