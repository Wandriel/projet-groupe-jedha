import pandas as pd
from s3_utils import read_s3_csv, upload_to_s3, get_s3_client
import cleaning_functions as cf

def get_all_files(s3_client, bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

def process_and_upload_silver(all_files, keyword, cleaning_func, silver_name):
    dfs = []
    print(f"⏳ Préparation du fichier Silver : {silver_name}...")
    
    for f in all_files:
        if keyword in f.lower():
            # Détection auto du séparateur
            sep = ',' if '2024' in f else ';'
            print(f"   -> Lecture de {f}")
            df_raw = read_s3_csv(f, separator=sep)
            df_clean = cleaning_func(df_raw)
            dfs.append(df_clean)
    
    if not dfs:
        print(f"⚠️ Aucun fichier trouvé pour {keyword}")
        return pd.DataFrame()
    
    df_silver = pd.concat(dfs, ignore_index=True)
    
    # On uploade en V2 pour ne rien casser
    upload_to_s3(df_silver, f"{silver_name}.csv", folder="silver")
    print(f"✅ Silver {silver_name} uploadé ({len(df_silver):,} lignes)")
    
    return df_silver

def main():
    bucket = "projet-accidents-jedha" # Vérifie bien ce nom !
    s3 = get_s3_client()
    all_files = get_all_files(s3, bucket, 'bronze/')

    # --- ÉTAPE 1 : GÉNÉRATION DES FICHIERS SILVER V2 ---
    # On change les noms ici pour la sécurité
    df_usagers = process_and_upload_silver(all_files, 'usagers', cf.clean_usagers, "usagers_cleaned_v2")
    df_caract = process_and_upload_silver(all_files, 'caract', cf.clean_caracteristiques, "caracteristiques_cleaned_v2")
    df_lieux = process_and_upload_silver(all_files, 'lieux', cf.clean_lieux, "lieux_cleaned_v2")
    df_veh = process_and_upload_silver(all_files, 'vehicules', cf.clean_vehicules, "vehicules_cleaned_v2")

    # --- ÉTAPE 2 : GÉNÉRATION DU FICHIER GOLD V2 ---
    print("\n🔗 Création de la table Gold V2 (Master Merge)...")
    
    # Jointures successives (On utilise on=['Num_Acc', 'id_vehicule', 'num_veh'] pour Usagers + Veh)
    master = pd.merge(df_usagers, df_veh, on=['Num_Acc', 'id_vehicule', 'num_veh'], how='left')
    master = pd.merge(master, df_caract, on='Num_Acc', how='left')
    master = pd.merge(master, df_lieux, on='Num_Acc', how='left')

    # --- ÉTAPE 3 : UPLOAD GOLD V2 ---
    upload_to_s3(master, "master_accidents_final_v2.csv", folder="gold")
    
    print("\n🚀 TOUT EST À JOUR EN V2 :")
    print(f"- 4 fichiers Silver '_v2' uploadés.")
    print(f"- 1 fichier Gold '_v2' de {len(master):,} lignes uploadé.")
    # Petit check de sécurité final pour te rassurer sur 2022
    if 'Num_Acc' in master.columns:
        annees = master['Num_Acc'].str[:4].unique()
        print(f"- Années traitées dans le Master : {annees}")

if __name__ == "__main__":
    main()