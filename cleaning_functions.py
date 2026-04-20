import pandas as pd
import numpy as np


# ══════════════════════════════════════════════════════════════════════════════
# cleaning_functions.py — Version synchronisée avec les notebooks de l'équipe
# Référence : alex_nettoyage_catct / lieux / usagers / vehicule_clean.ipynb
# ══════════════════════════════════════════════════════════════════════════════


def clean_caracteristiques(df, annee):
    """
    Nettoyage de la table Caractéristiques.
    Synchronisé avec alex_nettoyage_catct.ipynb.

    Corrections vs version précédente :
    - Ajout colonne 'annee'
    - Correction lat/long : virgule → point (années 2021-2023)
    - Ajout de 'lum' et 'intersection' dans les colonnes sentinelles (-1 → NaN)
    - Renommage lum → luminosite (aligné avec notebooks)
    - Construction date correcte depuis caract_df (pas df_2024)
    - Suppression colonnes inutiles : an, mois, jour, hrmn, adr, Accident_Id
    """

    # 1. Passe en minuscules
    df.columns = [c.lower().strip() for c in df.columns]

    # 2. Correction spécifique 2022 : Accident_Id → Num_Acc (AVANT tout le reste)
    if 'accident_id' in df.columns and 'num_acc' not in df.columns:
        df = df.rename(columns={'accident_id': 'num_acc'})

    # 3. Renommage complet aligné sur les notebooks
    mapping = {
        'num_acc':  'Num_Acc',
        'dep':      'departement',
        'com':      'commune',
        'agg':      'agglomeration',
        'int':      'intersection',
        'atm':      'meteo',
        'col':      'type_collision',
        'lum':      'luminosite',
    }
    df = df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})

    # 4. Colonne annee
    df['annee_source'] = annee

    # 5. Nettoyage clé Num_Acc → string
    df['Num_Acc'] = df['Num_Acc'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # 6. Construction colonne date depuis caract_df (pas df_2024 seul !)
    if 'date' not in df.columns and 'an' in df.columns:
        h_fix = df['hrmn'].astype(str).str.replace(':', '').str.replace('nan', '0000').str.zfill(4)
        date_string = (
            df['an'].astype(str) + '-' +
            df['mois'].astype(str).str.zfill(2) + '-' +
            df['jour'].astype(str).str.zfill(2) + ' ' +
            h_fix.str[:2] + ':' + h_fix.str[2:]
        )
        df['date'] = pd.to_datetime(date_string, errors='coerce')

    # 7. Correction lat/long : virgule → point (2021-2023 utilisent la virgule)
    for col in ['lat', 'long']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '.').pipe(pd.to_numeric, errors='coerce')

    # 8. Remplacement -1 → NaN sur TOUTES les colonnes concernées
    #    (lum et intersection étaient manquants dans la version précédente)
    cols_sentinel = ['luminosite', 'intersection', 'meteo', 'type_collision']
    for col in cols_sentinel:
        if col in df.columns:
            df[col] = df[col].replace([-1, '-1'], np.nan)

    # 9. Suppression colonnes inutiles (remplacées par 'date' et 'annee_source')
    cols_to_drop = ['an', 'mois', 'jour', 'hrmn', 'adr', 'accident_id']
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')

    # 10. Sélection colonnes finales
    cols_to_keep = [
        'Num_Acc', 'luminosite', 'departement', 'commune', 'agglomeration',
        'intersection', 'meteo', 'type_collision', 'lat', 'long', 'date','annee_source'
    ]
    df = df[[c for c in cols_to_keep if c in df.columns]].copy()

    return df


def clean_usagers(df, annee):
    """
    Nettoyage de la table Usagers.
    Synchronisé avec alex_nettoyage_usagers.ipynb.

    Corrections vs version précédente :
    - Ajout colonne 'annee'
    - Suppression colonnes 'place' et 'etatp' (décision équipe)
    - Renommage complet des colonnes (catu→categorie_usager, grav→gravite, etc.)
    - Correction an_nais aberrantes (> 2010 → NaN)
    """

    # 1. Colonne annee
    df['annee_source'] = annee

    # 2. Nettoyage clés → string
    df['Num_Acc'] = df['Num_Acc'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    if 'id_vehicule' in df.columns:
        df['id_vehicule'] = df['id_vehicule'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # 3. Remplacement -1 → NaN
    cols_sentinel = ['place', 'grav', 'sexe', 'trajet', 'secu1', 'secu2', 'secu3', 'locp', 'actp', 'etatp']
    for col in cols_sentinel:
        if col in df.columns:
            df[col] = df[col].replace([-1, '-1'], np.nan)

    # 4. Correction an_nais aberrantes (> 2010 = âge < 14 ans → erreur de saisie)
    if 'an_nais' in df.columns:
        df.loc[df['an_nais'] > 2010, 'an_nais'] = np.nan

    # 5. Suppression colonnes inutiles (décision équipe)
    #    place : position dans véhicule, peu pertinent
    #    etatp : état piéton, 92% NaN
    cols_to_drop = ['place', 'etatp', 'num_veh']
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')

    # 6. Renommage complet aligné sur les notebooks
    rename_map = {
        'catu':    'categorie_usager',
        'grav':    'gravite',
        'an_nais': 'annee_naissance',
        'trajet':  'motif_trajet',
        'secu1':   'equipement_secu1',
        'secu2':   'equipement_secu2',
        'secu3':   'equipement_secu3',
        'locp':    'localisation_pieton',
        'actp':    'action_pieton',
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # 7. Sélection colonnes finales
    cols_to_keep = [
        'Num_Acc', 'id_usager', 'id_vehicule',
        'categorie_usager', 'gravite', 'sexe', 'annee_naissance',
        'motif_trajet', 'equipement_secu1', 'equipement_secu2', 'equipement_secu3',
        'localisation_pieton', 'action_pieton', 'annee_source'
    ]
    df = df[[c for c in cols_to_keep if c in df.columns]].copy()

    return df


def clean_lieux(df, annee):
    """
    Nettoyage de la table Lieux.
    Synchronisé avec alex_nettoyage_lieux.ipynb.

    Corrections vs version précédente :
    - Ajout colonne 'annee'
    - Renommage complet des colonnes
    - Colonnes circ et prof conservées (décision équipe — valeur analytique)
    - Double remplacement -1 entier ET '-1' string
    - nbv converti en float (types mixtes entre années)
    - drop_duplicates déjà présent — conservé ✅
    """

    # 1. Colonne annee
    df['annee_source'] = annee

    # 2. Nettoyage clé → string
    df['Num_Acc'] = df['Num_Acc'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # 3. Dédoublonnage AVANT merge (conservé — crucial pour éviter multiplication lignes)
    df = df.drop_duplicates(subset=['Num_Acc'], keep='first')

    # 4. Remplacement -1 → NaN (entier ET string selon les années)
    cols_sentinel = ['catr', 'nbv', 'circ', 'prof', 'surf', 'infra', 'situ', 'vma']
    for col in cols_sentinel:
        if col in df.columns:
            df[col] = df[col].replace([-1, '-1'], np.nan)

    # 5. nbv en float (types mixtes entre années causent un DtypeWarning)
    if 'nbv' in df.columns:
        df['nbv'] = pd.to_numeric(df['nbv'], errors='coerce')

    # 6. vma en float
    if 'vma' in df.columns:
        df['vma'] = pd.to_numeric(df['vma'], errors='coerce')

    # 7. Renommage aligné sur les notebooks
    rename_map = {
        'catr':  'categorie_route',
        'circ':  'regime_circulation',
        'nbv':   'nb_voies',
        'prof':  'profil_route',
        'surf':  'etat_surface',
        'infra': 'infrastructure',
        'situ':  'situation_accident',
        'vma':   'vitesse_max_autorisee',
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # 8. Sélection colonnes finales
    #    circ et prof conservés (décision équipe — valeur analytique confirmée)
    #    plan supprimé (redondant avec prof et circ)
    cols_to_keep = [
        'Num_Acc', 'categorie_route', 'regime_circulation', 'nb_voies',
        'profil_route', 'etat_surface', 'infrastructure',
        'situation_accident', 'vitesse_max_autorisee', 'annee_source'
    ]
    df = df[[c for c in cols_to_keep if c in df.columns]].copy()

    return df


def clean_vehicules(df, annee):
    """
    Nettoyage de la table Véhicules.
    Synchronisé avec vehicule_clean.ipynb.

    Corrections vs version précédente :
    - Ajout colonne 'annee'
    - Suppression senc, choc, manv, occutc (décision équipe)
    - Renommage complet des colonnes
    """

    # 1. Colonne annee
    df['annee_source'] = annee

    # 2. Nettoyage clés → string
    df['Num_Acc'] = df['Num_Acc'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    if 'id_vehicule' in df.columns:
        df['id_vehicule'] = df['id_vehicule'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # 3. Remplacement -1 → NaN
    cols_sentinel = ['catv', 'obs', 'obsm', 'motor']
    for col in cols_sentinel:
        if col in df.columns:
            df[col] = df[col].replace([-1, '-1'], np.nan)

    # 4. Suppression colonnes inutiles (décision équipe)
    #    senc : sens de circulation, redondant avec lieux
    #    choc : point de choc, trop granulaire
    #    manv : manœuvre, trop granulaire
    #    occutc : occupants TC, 99% NaN
    cols_to_drop = ['senc', 'choc', 'manv', 'occutc', 'num_veh']
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')

    # 5. Renommage aligné sur les notebooks
    rename_map = {
        'catv':  'categorie_vehicule',
        'obs':   'obstacle_fixe',
        'obsm':  'obstacle_mobile',
        'motor': 'motorisation',
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # 6. Sélection colonnes finales
    cols_to_keep = [
        'Num_Acc', 'id_vehicule',
        'categorie_vehicule', 'obstacle_fixe', 'obstacle_mobile', 'motorisation',
        'annee_source'
    ]
    df = df[[c for c in cols_to_keep if c in df.columns]].copy()

    return df

def clean_vacances(df):
    """
    Nettoyage et expansion du calendrier scolaire.
    Transforme les plages [start, end] en lignes journalières par département.
    """
    
    # --- AJOUT SÉCURITÉ : Renommage si l'API change les noms ---
    # On s'assure que les colonnes ont les noms attendus par la suite du code
    rename_map = {
        'start_date': 'start_date', 
        'end_date': 'end_date',
        'location': 'location',
        'zones': 'zones',
        'description': 'description'
    }
    # On ne renomme que si la colonne existe (pour éviter de planter)
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # 1. Conversion des dates
    df = df.copy()
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')

    # On supprime les lignes sans dates valides (évite que explode plante)
    df = df.dropna(subset=['start_date', 'end_date'])

    # 2. Expansion (Explode) : On crée une ligne pour CHAQUE jour entre début et fin
    def generate_dates(row):
        return pd.date_range(start=row['start_date'], end=row['end_date']).tolist()

    df['date'] = df.apply(generate_dates, axis=1)
    df_unfolded = df.explode('date')

    # 3. Mapping des Départements (Le dictionnaire de ton ami)
    mapping_dept = {
        'Bordeaux': 'Gironde', 'Lyon': 'Rhône', 'Paris': 'Paris',
        'Saint Pierre et Miquelon': 'Saint-Pierre-et-Miquelon',
        'Versailles': 'Yvelines', 'Créteil': 'Val-de-Marne',
        'Aix-Marseille': 'Bouches-du-Rhône', 'Nice': 'Alpes-Maritimes',
        'Nantes': 'Loire-Atlantique', 'Rennes': 'Ille-et-Vilaine',
        'Lille': 'Nord', 'Montpellier': 'Hérault',
        'Toulouse': 'Haute-Garonne', 'Strasbourg': 'Bas-Rhin',
        'Nancy-Metz': 'Moselle', 'Reims': 'Marne',
        'Amiens': 'Somme', 'Rouen': 'Seine-Maritime',
        'Caen': 'Calvados', 'Orléans-Tours': 'Loiret',
        'Poitiers': 'Vienne', 'Limoges': 'Haute-Vienne',
        'Clermont-Ferrand': 'Puy-de-Dôme', 'Dijon': "Côte-d'Or",
        'Besançon': 'Doubs', 'Grenoble': 'Isère',
        'Corse': 'Corse-du-Sud', 'Guadeloupe': 'Guadeloupe',
        'Martinique': 'Martinique', 'Guyane': 'Guyane',
        'La Réunion': 'La Réunion', 'Mayotte': 'Mayotte',
        'Wallis et Futuna': 'Wallis-et-Futuna',
        'Polynésie Française': 'Polynésie-Française',
        'Nouvelle Calédonie': 'Nouvelle-Calédonie',
        'Normandie': 'Normandie', 'polynésie': 'Polynésie française',
        'Réunion': 'Île de la Réunion'
    }

    # Sécurité : Si 'location' manque, on crée la colonne vide pour ne pas faire planter le .map()
    if 'location' not in df_unfolded.columns:
        df_unfolded['location'] = "Inconnu"

    df_unfolded['Departement'] = df_unfolded['location'].map(mapping_dept).fillna('Inconnu')

    # 4. Formatage final de la date pour le CSV (YYYY/MM/DD)
    df_unfolded['date'] = pd.to_datetime(df_unfolded['date']).dt.strftime('%Y/%m/%d')

    # 5. Sélection des colonnes finales
    cols_to_keep = ['date', 'description', 'zones', 'location', 'Departement']
    df_final = df_unfolded[[c for c in cols_to_keep if c in df_unfolded.columns]].copy()

    return df_final