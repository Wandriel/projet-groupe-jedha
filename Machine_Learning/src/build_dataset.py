import pandas as pd
from src.db_connect import load_view

def build_ml_dataset() -> pd.DataFrame:

    # ── 1. Usagers (cible : gravite) ──────────────────────────────────────────
    usagers = load_view("view_usager", columns=[
        "Num_Acc", "id_vehicule", "gravite",
        "sexe", "tranche_age", "categorie_usager_label",
        "motif_trajet_label", "equipement_secu_1_label",
    ])

    # ── 2. Caract + Lieux fusionnés (SANS vacances_scolaires_flag) ────────────
    caract_lieux = load_view("view_global_caract_lieux", columns=[
        "Num_Acc",
        "luminosite_label", "agglomeration_label", "intersection_label",
        "meteo_label", "type_collision_label", "tranche_horaire_label",
        "categorie_route_label", "etat_surface_label", "infrastucture_label",
        "annee_source",
    ])

    # ── 3. view_caract pour récupérer vacances_scolaires_flag ─────────────────
    vacances = load_view("view_caract", columns=[
        "Num_Acc",
        "vacances_scolaires_flag",
    ])

    # ── 4. Véhicules ──────────────────────────────────────────────────────────
    vehicules = load_view("view_vehicules", columns=[
        "Num_Acc", "id_vehicule",
        "categorie_vehicule_label", "motorisation_label",
    ])

    # ── 5. Jointures ──────────────────────────────────────────────────────────
    df = (usagers
          .merge(caract_lieux, on="Num_Acc", how="left")
          .merge(vacances,     on="Num_Acc", how="left")
          .merge(vehicules,    on=["Num_Acc", "id_vehicule"], how="left"))

    print(f"\n📊 Dataset final : {df.shape[0]:,} lignes × {df.shape[1]} colonnes")
    print(f"Répartition gravite :\n{df['gravite'].value_counts()}\n")
    return df