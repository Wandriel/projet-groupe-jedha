import requests  # Bibliothèque pour "appeler" les sites internet (API)
import pandas as pd  # L'outil standard pour manipuler des tableaux de données
import time  # Pour faire des petites pauses et ne pas saturer le serveur

def fetch_vacances_data():
    """
    Cette fonction automatise la récupération du calendrier scolaire
    sur plusieurs années en interrogeant l'API officielle.
    """
    
    # 1. PRÉPARATION DE LA MISSION
    # Liste des années cibles pour couvrir tout l'historique du projet
    annees_a_recuperer = ["2019-2020", "2020-2021", "2021-2022", "2022-2023", "2023-2024", "2024-2025"]
    
    # Boîte vide qui va accumuler toutes les lignes de données récupérées
    all_results = []
    
    # Identifiants techniques pour trouver le bon "rayon" dans la bibliothèque de l'API
    dataset_id = "fr-en-calendrier-scolaire"
    base_url = f"https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/{dataset_id}/records"

    print("🚀 Début de la récupération multi-années...")

    # 2. BOUCLE PAR ANNÉE SCOLAIRE
    for annee in annees_a_recuperer:
        offset = 0  # On commence à la ligne 0 pour chaque nouvelle année
        limit = 100 # On demande les données par paquets de 100 lignes
        
        print(f"📅 Traitement de l'année scolaire : {annee}")
        
        # 3. BOUCLE DE PAGINATION (Tant qu'il y a des pages à lire...)
        while True:
            # Configuration de la demande spécifique à envoyer à l'API
            params = {
                "limit": limit,     # Combien de lignes on veut
                "offset": offset,   # À partir de quelle ligne on commence
                "refine": f"annee_scolaire:{annee}" # Filtre pour n'avoir QUE cette année
            }
            
            try:
                # On passe l'appel "téléphonique" à l'API
                response = requests.get(base_url, params=params, timeout=10)
                
                # Si le serveur répond mal (erreur 404 ou 500), on arrête cette année
                if response.status_code != 200:
                    print(f"⚠️ Erreur serveur sur l'année {annee}")
                    break
                
                # On transforme la réponse du serveur (JSON) en format lisible par Python
                data = response.json()
                batch = data.get('results', []) # On extrait la liste des résultats
                
                # S'il n'y a plus de lignes dans cette page, c'est qu'on a fini l'année !
                if not batch:
                    break
                
                # On ajoute le paquet de 100 lignes à notre grande boîte "all_results"
                all_results.extend(batch)
                
                # On prépare la page suivante (ex: on passe de la ligne 0 à 100)
                offset += limit
                
                # POLITESSE : On attend 0.1 seconde pour ne pas brusquer le serveur
                time.sleep(0.1) 
                
            except Exception as e:
                # En cas de coupure internet ou bug imprévu, on s'arrête proprement
                print(f"❌ Erreur critique : {e}")
                break
    
    # 4. TRANSFORMATION FINALE
    # On transforme notre liste géante de résultats JSON en un beau tableau (DataFrame)
    # json_normalize permet d'aplatir les données complexes pour les mettre en colonnes
    df_raw = pd.json_normalize(all_results)
    
    print(f"✅ Terminé ! {len(df_raw)} lignes prêtes pour le nettoyage.")
    
    # On renvoie le tableau brut pour qu'il soit nettoyé par la fonction clean_vacances
    return df_raw