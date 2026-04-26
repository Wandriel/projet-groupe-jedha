"""Configuration centralisée."""
import os
from dotenv import load_dotenv

load_dotenv()

# S3
S3_BUCKET = "projet-accidents-jedha"
S3_REGION = "eu-west-3"

# RDS
RDS_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME')}"

# Années + séparateurs CSV
YEARS = [2021, 2022, 2023, 2024]
CSV_SEP = {2024: ',', 2021: ';', 2022: ';', 2023: ';'}

# Mapping Silver → RDS
SILVER_TO_RDS = {
    'caracteristiques_cleaned.csv': 'fact_caracteristiques',
    'usagers_cleaned.csv': 'dim_usagers',
    'vehicules_cleaned.csv': 'dim_vehicules',
    'lieux_cleaned.csv': 'fact_lieux',
    'referentiel_vacances.csv': 'dim_vac_scolaire'
}

# Colonnes à remplacer -1 → NaN
SENTINEL_COLS = {
    'caract': ['luminosite', 'intersection', 'meteo', 'type_collision'],
    'usagers': ['place', 'grav', 'sexe', 'trajet', 'secu1', 'secu2', 'secu3', 'locp', 'actp', 'etatp'],
    'lieux': ['catr', 'nbv', 'circ', 'prof', 'surf', 'infra', 'situ', 'vma'],
    'vehicules': ['catv', 'obs', 'obsm', 'motor']
}

# Renommage colonnes
RENAME = {
    'num_acc': 'Num_Acc', 'dep': 'departement', 'com': 'commune', 'agg': 'agglomeration',
    'int': 'intersection', 'atm': 'meteo', 'col': 'type_collision', 'lum': 'luminosite',
    'catu': 'categorie_usager', 'grav': 'gravite', 'an_nais': 'annee_naissance',
    'trajet': 'motif_trajet', 'secu1': 'equipement_secu1', 'secu2': 'equipement_secu2',
    'secu3': 'equipement_secu3', 'locp': 'localisation_pieton', 'actp': 'action_pieton',
    'catr': 'categorie_route', 'circ': 'regime_circulation', 'nbv': 'nb_voies',
    'prof': 'profil_route', 'surf': 'etat_surface', 'infra': 'infrastructure',
    'situ': 'situation_accident', 'vma': 'vitesse_max_autorisee',
    'catv': 'categorie_vehicule', 'obs': 'obstacle_fixe', 'obsm': 'obstacle_mobile',
    'motor': 'motorisation',
}

# Vacances zones → depts
ZONE_DEPT = {
    'Bordeaux': 'Gironde', 'Lyon': 'Rhône', 'Paris': 'Paris', 'Versailles': 'Yvelines',
    'Créteil': 'Val-de-Marne', 'Aix-Marseille': 'Bouches-du-Rhône', 'Nice': 'Alpes-Maritimes',
    'Nantes': 'Loire-Atlantique', 'Rennes': 'Ille-et-Vilaine', 'Lille': 'Nord',
    'Montpellier': 'Hérault', 'Toulouse': 'Haute-Garonne', 'Strasbourg': 'Bas-Rhin',
}

# Dossier vues SQL
SQL_FOLDER = "Codes_Tables_SQL_DANGER"
