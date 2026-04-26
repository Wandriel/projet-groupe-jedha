import pandas as pd
from sklearn.preprocessing import LabelEncoder

MAP_GRAVITE = {
    1: 0,  # Indemne       → non mortel
    2: 1,  # Tué           → MORTEL
    3: 0,  # Blessé hospit → non mortel
    4: 0,  # Blessé léger  → non mortel
}

# ── Features catégorielles (texte → LabelEncoder) ─────────────────────────────
FEATURES_CATEGORIQUES = [
    # Usager
    "categorie_usager_label",
    "motif_trajet_label",
    "equipement_secu_1_label",
    "equipement_secu_2_label",
    "equipement_secu_3_label",
    "tranche_age",
    "sexe",
    # Véhicule
    "categorie_vehicule_label",
    "motorisation_label",
    "obstacle_fixe_label",
    "obstacle_mobile_label",
    # Accident / conditions
    "luminosite_label",        # ← une seule fois
    "agglomeration_label",     # ← une seule fois
    "intersection_label",
    "meteo_label",
    "type_collision_label",
    "tranche_horaire_label",   # ← texte donc ICI, pas dans NUM
    # Lieu
    "categorie_route_label",
    "etat_surface_label",
    "infrastucture_label"
]

# ── Features numériques (déjà des nombres) ────────────────────────────────────
FEATURES_NUMERIQUES = [
    "vacances_scolaires_flag",   # 0/1                     # 1/2
    "vitesse_max_autorisee",     # nombre entier
    # "annee_source",            # supprimé : non prédictif
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