import pandas as pd
import numpy as np

def clean_caracteristiques(df):
    """
    Nettoyage de la table Caractéristiques - Version Synchronisée V1/V2
    """
    # 1. On passe tout en minuscules immédiatement
    df.columns = [c.lower().strip() for c in df.columns]
    
    # 2. RENOMMAGE (On aligne TOUT sur la V1 des collègues)
    mapping = {
        'accident_id': 'Num_Acc',
        'num_acc': 'Num_Acc',
        'dep': 'departement',    # Ajouté
        'com': 'commune',        # Ajouté
        'atm': 'meteo',
        'col': 'type_collision',
        'agg': 'agglomeration',
        'int': 'intersection'
    }
    df = df.rename(columns=mapping)

    # --- CAS SPÉCIAL DE LA DATE ---
    # Si la colonne 'date' n'existe pas, on la crée à partir de an, mois, jour
    if 'date' not in df.columns and 'an' in df.columns:
        # On crée une chaîne YYYY-MM-DD
        df['date'] = df['an'].astype(str) + '-' + df['mois'].astype(str) + '-' + df['jour'].astype(str)

    # 3. Nettoyage de la clé Num_Acc
    if 'Num_Acc' in df.columns:
        df['Num_Acc'] = df['Num_Acc'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # 4. Remplacement des -1 par NaN
    cols_a_traiter = ['meteo', 'type_collision']
    for col in cols_a_traiter:
        if col in df.columns:
            df[col] = df[col].replace([-1, "-1"], np.nan)
    
    # 5. Sélection des colonnes finales (Maintenant elles existeront toutes !)
    cols_to_keep = [
        'Num_Acc', 'lum', 'departement', 'commune', 'agglomeration', 
        'intersection', 'meteo', 'type_collision', 'lat', 'long', 'date'
    ]
    df = df[[c for c in cols_to_keep if c in df.columns]].copy()

    return df

    # 4. Remplacement des -1 par NaN
    cols_a_traiter = ['meteo', 'type_collision']
    for col in cols_a_traiter:
        if col in df.columns:
            df[col] = df[col].replace([-1, "-1"], np.nan)
    
    # 5. Sélection des colonnes finales
    cols_to_keep = [
        'Num_Acc', 'lum', 'departement', 'commune', 'agglomeration', 
        'intersection', 'meteo', 'type_collision', 'lat', 'long', 'date'
    ]
    df = df[[c for c in cols_to_keep if c in df.columns]].copy()

    return df

def clean_usagers(df):
    """
    Nettoyage de la table Usagers (Fidèle à nettoyage_usagers.ipynb)
    """
    # 1. Sélection des colonnes (comme dans ton notebook cellule 15)
    cols_to_keep = [
        'Num_Acc', 'id_usager', 'id_vehicule', 'num_veh', 'catu', 
        'grav', 'sexe', 'an_nais', 'trajet', 'secu1', 'secu2', 
        'secu3', 'locp', 'actp'
    ]
    # On ne garde que les colonnes qui existent vraiment dans le fichier
    df = df[[c for c in cols_to_keep if c in df.columns]].copy()

    # 2. Harmonisation RADICALE des clés (Sécurité pour la jointure)
    df['Num_Acc'] = df['Num_Acc'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    if 'id_vehicule' in df.columns:
        df['id_vehicule'] = df['id_vehicule'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # 3. Remplacement des valeurs -1 par NaN (comme dans ton notebook cellule 16)
    df = df.replace(-1, np.nan)
    df = df.replace("-1", np.nan) # Sécurité si c'est du texte
        
    return df

def clean_lieux(df):
    """
    Nettoyage de la table Lieux.
    """
    # 1. Harmonisation de la clé (La base de tout !)
    df['Num_Acc'] = df['Num_Acc'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # 2. Nettoyage des colonnes numériques (vma = Vitesse Maximale Autorisée)
    # Parfois il y a des espaces ou des textes bizarres
    if 'vma' in df.columns:
        df['vma'] = pd.to_numeric(df['vma'], errors='coerce')

    # 3. Remplacement des valeurs par défaut (-1 ou 0 parfois utilisé pour NaN)
    # On remplace -1 par NaN sur tout le dataframe
    df = df.replace([-1, "-1"], np.nan)
    
    # 4. On garde les colonnes principales (ajuste selon les besoins de ton groupe)
    cols_to_keep = ['Num_Acc', 'catr', 'nbv', 'vma', 'surf', 'infra', 'situ']
    df = df[[c for c in cols_to_keep if c in df.columns]].copy()
    
    return df

def clean_vehicules(df):
    """
    Nettoyage de la table Véhicules.
    """
    # 1. Harmonisation des DEUX clés de jointure
    df['Num_Acc'] = df['Num_Acc'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    if 'id_vehicule' in df.columns:
        df['id_vehicule'] = df['id_vehicule'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # 2. Remplacement des valeurs par défaut (-1 ou 0 selon les années)
    df = df.replace([-1, "-1"], np.nan)
    
    # 3. Sélection des colonnes utiles (catv = catégorie véhicule, motor = motorisation)
    cols_to_keep = ['Num_Acc', 'id_vehicule', 'num_veh', 'catv', 'obs', 'obsm', 'choc', 'manv', 'motor']
    df = df[[c for c in cols_to_keep if c in df.columns]].copy()
    
    return df