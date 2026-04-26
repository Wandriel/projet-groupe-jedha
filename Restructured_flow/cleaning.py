"""Nettoyage données - Logique métier."""
import pandas as pd
import numpy as np
from config import SENTINEL_COLS, RENAME, ZONE_DEPT


def _clean_cols(df):
    """Minuscules + strip."""
    df.columns = [c.lower().strip() for c in df.columns]
    return df


def _fix_id(df, col):
    """ID numeric → string."""
    if col in df.columns:
        df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    return df


def _sentinel(df, cols):
    """Remplace -1 → NaN."""
    for col in cols:
        if col in df.columns:
            df[col] = df[col].replace([-1, '-1'], np.nan)
    return df


def clean_caract(df, year):
    """Nettoyage Caractéristiques."""
    df = _clean_cols(df)
    if 'accident_id' in df.columns:
        df = df.rename(columns={'accident_id': 'num_acc'})
    df = df.rename(columns={k: v for k, v in RENAME.items() if k in df.columns})
    df['annee_source'] = year
    df = _fix_id(df, 'Num_Acc')
    
    if 'date' not in df.columns and 'an' in df.columns:
        h = df['hrmn'].astype(str).str.replace(':', '').str.replace('nan', '0000').str.zfill(4)
        df['date'] = pd.to_datetime(
            df['an'].astype(str) + '-' + df['mois'].astype(str).str.zfill(2) + '-' + 
            df['jour'].astype(str).str.zfill(2) + ' ' + h.str[:2] + ':' + h.str[2:], 
            errors='coerce'
        )
    
    for col in ['lat', 'long']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')
    
    df = _sentinel(df, SENTINEL_COLS['caract'])
    df = df.drop(columns=[c for c in ['an', 'mois', 'jour', 'hrmn', 'adr', 'accident_id'] if c in df.columns], errors='ignore')
    
    final = ['Num_Acc', 'luminosite', 'departement', 'commune', 'agglomeration', 'intersection', 
             'meteo', 'type_collision', 'lat', 'long', 'date', 'annee_source']
    return df[[c for c in final if c in df.columns]]


def clean_usagers(df, year):
    """Nettoyage Usagers."""
    df = _clean_cols(df)
    df['annee_source'] = year
    df = _fix_id(df, 'Num_Acc')
    df = _fix_id(df, 'id_vehicule')
    df = _sentinel(df, SENTINEL_COLS['usagers'])
    
    if 'an_nais' in df.columns:
        df.loc[df['an_nais'] > 2010, 'an_nais'] = np.nan
    
    df = df.drop(columns=[c for c in ['place', 'etatp', 'num_veh'] if c in df.columns], errors='ignore')
    df = df.rename(columns={k: v for k, v in RENAME.items() if k in df.columns})
    
    final = ['Num_Acc', 'id_usager', 'id_vehicule', 'categorie_usager', 'gravite', 'sexe', 
             'annee_naissance', 'motif_trajet', 'equipement_secu1', 'equipement_secu2', 
             'equipement_secu3', 'localisation_pieton', 'action_pieton', 'annee_source']
    return df[[c for c in final if c in df.columns]]


def clean_lieux(df, year):
    """Nettoyage Lieux."""
    df = _clean_cols(df)
    df['annee_source'] = year
    df = _fix_id(df, 'Num_Acc')
    df = df.drop_duplicates(subset=['Num_Acc'], keep='first')
    df = _sentinel(df, SENTINEL_COLS['lieux'])
    
    for col in ['nbv', 'vma']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df = df.rename(columns={k: v for k, v in RENAME.items() if k in df.columns})
    
    final = ['Num_Acc', 'categorie_route', 'regime_circulation', 'nb_voies', 'profil_route', 
             'etat_surface', 'infrastructure', 'situation_accident', 'vitesse_max_autorisee', 'annee_source']
    return df[[c for c in final if c in df.columns]]


def clean_vehicules(df, year):
    """Nettoyage Véhicules."""
    df = _clean_cols(df)
    df['annee_source'] = year
    df = _fix_id(df, 'Num_Acc')
    df = _fix_id(df, 'id_vehicule')
    df = _sentinel(df, SENTINEL_COLS['vehicules'])
    df = df.drop(columns=[c for c in ['senc', 'choc', 'manv', 'occutc', 'num_veh'] if c in df.columns], errors='ignore')
    df = df.rename(columns={k: v for k, v in RENAME.items() if k in df.columns})
    
    final = ['Num_Acc', 'id_vehicule', 'categorie_vehicule', 'obstacle_fixe', 'obstacle_mobile', 'motorisation', 'annee_source']
    return df[[c for c in final if c in df.columns]]


def clean_vacances(df):
    """Nettoyage Vacances (dépliage)."""
    df = df.copy()
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
    df = df.dropna(subset=['start_date', 'end_date'])
    
    df['date'] = df.apply(lambda r: pd.date_range(r['start_date'], r['end_date']).tolist(), axis=1)
    df = df.explode('date')
    
    if 'location' not in df.columns:
        df['location'] = 'Inconnu'
    
    df['Departement'] = df['location'].map(ZONE_DEPT).fillna('Inconnu')
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y/%m/%d')
    
    final = ['date', 'description', 'zones', 'location', 'Departement']
    return df[[c for c in final if c in df.columns]]
