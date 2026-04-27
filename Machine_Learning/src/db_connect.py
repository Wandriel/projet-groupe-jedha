import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd

load_dotenv()

def get_engine():
    """Retourne un engine SQLAlchemy connecté au RDS PostgreSQL."""
    host     = os.getenv("DB_HOST")
    port     = os.getenv("DB_PORT", 5432)
    dbname   = os.getenv("DB_NAME")
    user     = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)


def load_view(view_name: str, columns: list = None, limit: int = None) -> pd.DataFrame:
    
    # ✅ Guillemets doubles autour de chaque colonne
    if columns:
        cols = ", ".join(f'"{c}"' for c in columns)
    else:
        cols = "*"
    
    lim   = f"LIMIT {limit}" if limit else ""
    query = f'SELECT {cols} FROM public."{view_name}" {lim}'
    
    print(f"🔍 Query : {query[:150]}")  # Tu dois voir les guillemets ici
    
    with get_engine().connect() as conn:
        df = pd.read_sql(query, conn)
    
    print(f"✅ {view_name} : {df.shape[0]:,} lignes × {df.shape[1]} colonnes")
    return df