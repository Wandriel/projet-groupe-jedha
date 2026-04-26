"""Connecteurs AWS S3, RDS PostgreSQL, API education.gouv.fr."""
import io
import os
import boto3
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


class S3:
    """Opérations S3."""
    def __init__(self, bucket, region):
        self.s3 = boto3.client('s3', region_name=region)
        self.bucket = bucket
    
    def list_csv(self, prefix):
        resp = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return [o['Key'] for o in resp.get('Contents', []) if o['Key'].endswith('.csv')]
    
    def read_csv(self, key, sep=';'):
        r = self.s3.get_object(Bucket=self.bucket, Key=key)
        return pd.read_csv(io.BytesIO(r['Body'].read()), sep=sep, low_memory=False)
    
    def write_csv(self, df, filename, folder):
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        self.s3.put_object(Bucket=self.bucket, Key=f"{folder}/{filename}", Body=buf.getvalue())


class RDS:
    """Opérations PostgreSQL."""
    def __init__(self, url):
        self.engine = create_engine(url)
    
    def truncate(self, table):
        with self.engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE "{table}" CASCADE;'))
    
    def drop_create(self, table):
        with self.engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE;'))
    
    def push(self, df, table):
        try:
            self.truncate(table)
            df.to_sql(table, self.engine, if_exists='append', index=False, method='multi', chunksize=1000)
        except SQLAlchemyError:
            self.drop_create(table)
            df.to_sql(table, self.engine, if_exists='replace', index=False, method='multi', chunksize=1000)
    
    def exec_sql(self, filepath):
        with open(filepath) as f:
            with self.engine.begin() as conn:
                conn.execute(text(f.read()))


class API:
    """Fetch API education.gouv.fr."""
    def __init__(self):
        self.url = "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-calendrier-scolaire/records"
    
    def fetch(self):
        results = []
        for year in ["2020-2021", "2021-2022", "2022-2023", "2023-2024", "2024-2025"]:
            offset = 0
            while True:
                try:
                    r = requests.get(self.url, params={"limit": 100, "offset": offset, "refine": f"annee_scolaire:{year}"}, timeout=10)
                    if r.status_code != 200: break
                    batch = r.json().get('results', [])
                    if not batch: break
                    results.extend(batch)
                    offset += 100
                except: break
        return pd.json_normalize(results) if results else None
