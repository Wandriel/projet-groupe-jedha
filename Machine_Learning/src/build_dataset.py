import pandas as pd
from src.db_connect import load_view

def build_ml_dataset() -> pd.DataFrame:
    df = load_view("ml_dataset_final")
    print(f"📊 {df.shape[0]:,} lignes × {df.shape[1]} colonnes")
    print(f"Mortels : {df['gravite_binaire'].sum():,} / {len(df):,}")
    return df