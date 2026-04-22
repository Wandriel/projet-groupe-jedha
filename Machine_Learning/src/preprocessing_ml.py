import pandas as pd
from sklearn.preprocessing import LabelEncoder

# Mapping gravité → binaire (mortel vs non mortel)
MAP_GRAVITE_BINAIRE = {
    1: 0,   # Indemne       → non mortel
    2: 1,   # Tué           → MORTEL ← classe cible
    3: 0,   # Blessé hospit → non mortel
    4: 0,   # Blessé léger  → non mortel
}

FEATURES_CATEGORIQUES = [
    "luminosite_label", "agglomeration_label", "intersection_label",
    "meteo_label", "type_collision_label", "tranche_horaire_label",
    "categorie_route_label", "etat_surface_label", "infrastucture_label",
    "categorie_usager_label", "motif_trajet_label", "equipement_secu_1_label",
    "categorie_vehicule_label", "motorisation_label",
    "tranche_age", "sexe",
]

FEATURES_NUMERIQUES = [
    "vacances_scolaires_flag",  # 0/1 déjà numérique
    "annee_source",
]


def prepare_features(df: pd.DataFrame):
    """
    Prépare X et y pour scikit-learn.
    Retourne X (DataFrame encodé), y (Series binaire), feature_names
    """
    df = df.copy()

    # Variable cible binaire
    df["target"] = df["gravite"].map(MAP_GRAVITE_BINAIRE)
    df = df.dropna(subset=["target"])
    y = df["target"].astype(int)

    # Sélection des features
    X = df[FEATURES_CATEGORIQUES + FEATURES_NUMERIQUES].copy()

    # Remplissage des NaN
    X[FEATURES_CATEGORIQUES] = X[FEATURES_CATEGORIQUES].fillna("Inconnu")
    X[FEATURES_NUMERIQUES]   = X[FEATURES_NUMERIQUES].fillna(0)

    # Encodage ordinal (pour Random Forest scikit-learn)
    for col in FEATURES_CATEGORIQUES:
        le = LabelEncoder()
        X[col] = le.fit_transform(X[col].astype(str))

    print(f"✅ X : {X.shape} | y (mortels) : {y.sum():,} / {len(y):,} ({y.mean()*100:.2f}%)")
    return X, y