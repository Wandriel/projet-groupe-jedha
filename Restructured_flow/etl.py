"""Pipeline ETL : Bronze → Silver → Gold → RDS."""
import pandas as pd
import os
from config import S3_BUCKET, S3_REGION, YEARS, CSV_SEP, SILVER_TO_RDS, SQL_FOLDER
from connectors import S3, RDS, API
from cleaning import clean_caract, clean_usagers, clean_lieux, clean_vehicules, clean_vacances


class Pipeline:
    """ETL orchestration."""
    
    def __init__(self, dry_run=False):
        self.s3 = S3(S3_BUCKET, S3_REGION)
        self.dry_run = dry_run
    
    def _get_year(self, path):
        for y in YEARS:
            if str(y) in path:
                return y
        return None
    
    def process_table(self, keyword, clean_func, name):
        """Lit, nettoie, concatene."""
        dfs = []
        print(f"⏳ {name}...")
        
        for f in self.s3.list_csv('bronze'):
            if keyword.lower() not in f.lower():
                continue
            
            year = self._get_year(f)
            if not year:
                continue
            
            sep = CSV_SEP.get(year, ';')
            df = self.s3.read_csv(f, sep)
            dfs.append(clean_func(df, year))
        
        if not dfs:
            print(f"   ❌ Aucun fichier")
            return pd.DataFrame()
        
        df_result = pd.concat(dfs, ignore_index=True)
        
        if not self.dry_run:
            self.s3.write_csv(df_result, f"{name}.csv", "silver")
        
        print(f"   ✅ {len(df_result):,} lignes")
        return df_result
    
    def etl(self):
        """ETL : Bronze → Silver → Gold."""
        print(f"\n{'═'*60}")
        print("🚀 ETL PIPELINE")
        print(f"{'═'*60}\n")
        
        # Vacances API
        print("📅 Vacances scolaires...")
        try:
            df_vac = API().fetch()
            if df_vac is not None:
                df_vac = clean_vacances(df_vac)
                if not self.dry_run:
                    self.s3.write_csv(df_vac, "referentiel_vacances.csv", "silver")
                print(f"   ✅ {len(df_vac):,} lignes\n")
        except Exception as e:
            print(f"   ⚠️  Erreur : {e}\n")
        
        # Silver (4 tables)
        print("🧹 Nettoyage (Silver)...")
        df_u = self.process_table('usagers', clean_usagers, "usagers_cleaned")
        df_c = self.process_table('caract', clean_caract, "caracteristiques_cleaned")
        df_l = self.process_table('lieux', clean_lieux, "lieux_cleaned")
        df_v = self.process_table('vehicules', clean_vehicules, "vehicules_cleaned")
        
        if any(df.empty for df in [df_u, df_c, df_l, df_v]):
            print("❌ Erreur critique")
            return None
        
        # Gold (merge)
        print("\n🔗 Merge (Gold)...")
        master = pd.merge(df_u, df_c, on='Num_Acc', how='left')
        master = pd.merge(master, df_l, on='Num_Acc', how='left')
        master = pd.merge(master, df_v, on=['Num_Acc', 'id_vehicule'], how='left')
        
        dup = [c for c in master.columns if c.endswith(('_x', '_y'))]
        if dup:
            master = master.drop(columns=dup)
        
        if not self.dry_run:
            self.s3.write_csv(master, "master_accidents_final.csv", "silver")
        
        print(f"   ✅ MASTER : {len(master):,} lignes\n")
        return master
    
    def rds(self):
        """Silver → RDS."""
        print(f"\n{'═'*60}")
        print("💾 PUSH RDS")
        print(f"{'═'*60}\n")
        
        from config import RDS_URL
        rds = RDS(RDS_URL)
        
        for csv_file, table in SILVER_TO_RDS.items():
            try:
                print(f"📥 {table}...", end=" ")
                df = self.s3.read_csv(f"silver/{csv_file}", sep=',')
                if not self.dry_run:
                    rds.push(df, table)
                print(f"✅ {len(df):,} lignes")
            except Exception as e:
                print(f"❌ {e}")
        
        # Vues SQL
        print(f"\n🏗️  Vues SQL...")
        if not self.dry_run and os.path.exists(SQL_FOLDER):
            for script in sorted([f for f in os.listdir(SQL_FOLDER) if f.endswith('.sql')]):
                try:
                    rds.exec_sql(os.path.join(SQL_FOLDER, script))
                    print(f"   ✅ {script}")
                except Exception as e:
                    print(f"   ❌ {script} : {e}")
        
        print(f"\n{'═'*60}")
        print("✨ DONE")
        print(f"{'═'*60}\n")
