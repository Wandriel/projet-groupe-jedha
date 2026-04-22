import pandas as pd

def check_quality(df, name="Dataset"):
    """
    ANALYSE DE QUALITÉ GLOBALE
    Objectif : Vérifier la volumétrie et détecter les données manquantes (NaN).
    """
    print(f"\n{'='*10} AUDIT DE QUALITÉ : {name} {'='*10}")
    
    # Étape 1 : Vérification de la volumétrie (Lignes/Colonnes)
    print(f"[INFO] Nombre de lignes détectées : {len(df)}")
    print(f"[INFO] Nombre de colonnes détectées : {len(df.columns)}")
    
    # Étape 2 : Identification des valeurs manquantes (NaN)
    print("\n--- Analyse des valeurs manquantes ---")
    nan_counts = df.isnull().sum()
    
    # On filtre pour n'afficher que les colonnes problématiques (gain de lisibilité)
    if nan_counts.sum() > 0:
        print("[ALERTE] Colonnes avec données manquantes :")
        print(nan_counts[nan_counts > 0])
    else:
        print("[OK] Aucune donnée manquante (NaN) détectée sur l'ensemble du dataset.")
    
    return len(df)

def compare_bronze_silver(df_bronze, df_silver):
    """
    VÉRIFICATION DU FLUX (LINEAGE)
    Objectif : S'assurer qu'aucune donnée n'a été perdue accidentellement entre 
    le fichier brut (Bronze) et le fichier nettoyé (Silver).
    """
    count_b = len(df_bronze)
    count_s = len(df_silver)
    diff = count_b - count_s
    
    print(f"\n{'*'*10} RÉCONCILIATION BRONZE VS SILVER {'*'*10}")
    print(f"-> Entrée (Couche Bronze) : {count_b} lignes")
    print(f"-> Sortie (Couche Silver) : {count_s} lignes")
    
    # Analyse de la différence
    if diff == 0:
        print("✅ SUCCÈS : Intégrité totale. Aucune ligne supprimée.")
    elif diff > 0:
        print(f"ℹ️ LOG : {diff} lignes filtrées durant le nettoyage (doublons/erreurs).")
    else:
        print(f"⚠️ ATTENTION : {abs(diff)} lignes ajoutées (vérifier si jointure effectuée).")

def check_nans_by_year(df, year_col):
    """
    AUDIT DE COMPLÉTUDE TEMPORELLE
    Objectif : Rassurer l'équipe sur la fiabilité des données pour chaque année.
    Calcule le pourcentage de vide pour chaque colonne, groupé par année.
    """
    print(f"\n--- ANALYSE DE COMPLÉTUDE PAR ANNÉE (Colonne pivot: {year_col}) ---")
    
    # Identification des colonnes ayant au moins un NaN
    cols_with_nans = df.columns[df.isnull().any()].tolist()
    
    if not cols_with_nans:
        print("[OK] 100% de complétude pour toutes les années du dataset.")
        return
        
    # Calcul du ratio de NaN (en %) pour chaque groupe d'année
    # Formule : (Nombre de NaN / Total de lignes de l'année) * 100
    report = df.groupby(year_col)[cols_with_nans].apply(lambda x: (x.isnull().sum() / len(x)) * 100)
    
    print("[INFO] Pourcentage de vide (%) par colonne et par année :")
    print(report)