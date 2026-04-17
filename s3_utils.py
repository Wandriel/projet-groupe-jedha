import boto3
import os
import io
import pandas as pd
from dotenv import load_dotenv

# 1. INITIALISATION (Le portier)
load_dotenv()

def get_s3_client():
    """Étape 0 : Allumer la télécommande AWS"""
    return boto3.client('s3')

# 2. LECTURE (Le drone va chercher le linge sale dans Bronze)
def read_s3_csv(file_key, separator=';'):
    """Étape 1 : Lire un fichier depuis S3 et le mettre dans un tableau Python"""
    s3 = get_s3_client()
    bucket = "projet-accidents-jedha"
    
    # On demande l'objet à AWS
    response = s3.get_object(Bucket=bucket, Key=file_key)
    # On transforme les octets reçus en un tableau (DataFrame) que tu peux lire
    df = pd.read_csv(io.BytesIO(response['Body'].read()), sep=separator)
    return df

# 3. ÉCRITURE (Le drone range le linge propre dans Silver)
def upload_to_s3(df, file_name, folder="silver"):
    """Étape 2 : Envoyer le tableau nettoyé vers S3"""
    s3 = get_s3_client()
    bucket = "projet-accidents-jedha"
    
    # On transforme le tableau en texte (CSV) dans la mémoire vive
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # On définit l'adresse d'arrivée
    target_key = f"{folder}/{file_name}"
    
    # On envoie le paquet
    s3.put_object(Bucket=bucket, Key=target_key, Body=csv_buffer.getvalue())
    print(f"✅ Mission accomplie : {target_key} est sur S3 !")