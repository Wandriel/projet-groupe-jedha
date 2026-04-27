import pandas as pd
from sklearn.preprocessing import LabelEncoder

MAP_GRAVITE = {
    1: 0,  # Indemne       → non mortel
    2: 1,  # Tué           → MORTEL
    3: 1,  # Blessé hospit → non mortel
    4: 0,  # Blessé léger  → non mortel
}

# ── Features catégorielles (texte → LabelEncoder) ─────────────────────────────
FEATURES_CATEGORIQUES = [
    "type_collision_label",
    "categorie_vehicule_label",
    "motorisation_label",
    "equipement_secu_1_label",
    "categorie_usager_label",
    "motif_trajet_label",
    "tranche_age",
    "agglomeration_label",
    "intersection_label",
    "luminosite_label",
    "meteo_label",
    "categorie_route_label",
    "etat_surface_label",
    "infrastucture_label",
    "tranche_horaire_label",
    "obstacle_mobile_label",
    "obstacle_fixe_label",
    "sexe",                    # ← catégoriel maintenant
]

FEATURES_NUMERIQUES = [
    "vacances_scolaires_flag",
    "vitesse_max_autorisee",
    "profil_risque",
    "route_dangereuse",
    "visibilite_degradee",
    "heure_nuit",
    "vehicule_vulnerable",
]

def prepare_features(df: pd.DataFrame):
    df = df.copy()

    # Variable cible
    if "gravite_binaire" in df.columns:
        df["target"] = df["gravite_binaire"]
    else:
        df["target"] = df["gravite"].map(MAP_GRAVITE)

    df = df.dropna(subset=["target"])
    y  = df["target"].astype(int)

    # ── Garde uniquement les colonnes qui existent vraiment dans df ───────────
    cols_cat_ok = [c for c in FEATURES_CATEGORIQUES if c in df.columns]
    cols_num_ok = [c for c in FEATURES_NUMERIQUES    if c in df.columns]

    # ── Colonnes manquantes → warning ─────────────────────────────────────────
    manquantes = set(FEATURES_CATEGORIQUES + FEATURES_NUMERIQUES) - set(df.columns)
    if manquantes:
        print(f"⚠️  Colonnes absentes ignorées : {manquantes}")

    X = df[cols_cat_ok + cols_num_ok].copy()

    # ── Remplissage NaN ───────────────────────────────────────────────────────
    X[cols_cat_ok] = X[cols_cat_ok].fillna("Inconnu")
    X[cols_num_ok] = X[cols_num_ok].fillna(0)

    # ── Encodage ordinal ──────────────────────────────────────────────────────
    for col in cols_cat_ok:
        X[col] = LabelEncoder().fit_transform(X[col].astype(str))

    print(f"✅ X : {X.shape}")
    print(f"✅ Mortels : {y.sum():,} / {len(y):,} ({y.mean()*100:.2f}%)")
    return X, y