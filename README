# Pipeline ETL — Analyse des Accidents de la Route en France Métropolitaine (BAAC 2021–2024)

Pipeline de données **End-to-End** automatisant l'extraction, le nettoyage et le chargement des données BAAC (Bulletin d'Analyse des Accidents Corporels) vers PostgreSQL, avec alimentation d'un rapport Power BI.

---

## Objectifs

- Construire un pipeline modulaire et reproductible sur architecture Medallion (Bronze → Silver → Gold)
- Traiter des données réelles imparfaites (valeurs sentinelles, incohérences inter-années, types erronés)
- Enrichir les données de trafic avec des signaux externes (calendrier scolaire via API)
- Préparer une table de faits unique pour des analyses avancées dans Power BI
- Alimenter un modèle ML prédictif des pics de risque (axe post-formation)

---

## Sources de données

- **BAAC** — Données annuelles des accidents corporels (data.gouv.fr, 2021–2024) — 4 datasets × 4 années = **16 fichiers**
- **API Éducation Nationale** — Calendrier scolaire officiel (identification des périodes de vacances)
- **AWS S3** — Stockage intermédiaire des couches Bronze et Silver

---

## Architecture du pipeline

```text
AWS S3 BRONZE
  (CSV bruts — 16 fichiers)
      │
      ▼
[main.py + cleaning_functions.py]
  Nettoyage & normalisation
      │
      ▼
AWS S3 SILVER
  (CSV standardisés)
      │
      ▼
[main.py — Gold]
  Jointure des 4 tables BAAC
  + enrichissement vacances (vacances_api.py)
      │
      ▼
[script_connexion_RDS.py]
  Chargement PostgreSQL
  + reconstruction des vues SQL
      │
      ▼
AWS RDS (PostgreSQL)       →  Power BI
                           →  Module ML (post-formation)
```

| Couche | Contenu |
|--------|---------|
| **Bronze** | CSV bruts BAAC par année (usagers, caractéristiques, lieux, véhicules) |
| **Silver** | Données nettoyées, typées, enrichies (lat/long, dates, colonne `annee`) |
| **Gold** | Table de faits `fact_accidents` — jointure sur `Num_Acc` / `id_vehicule` |
| **Serving** | Vues SQL analytiques dans RDS, consommées par Power BI |

> **Performances :** run complet en ~2min45 (Bronze → Gold) / ~3min10 (Gold → RDS + vues SQL)

---

## Structure des fichiers

```text
.
├── run_all_py.py              # Orchestrateur — point d'entrée unique
├── main.py                   # Cycle de vie S3 : Bronze → Silver → Gold
├── cleaning_functions.py     # Logique métier de nettoyage (Data Quality)
├── vacances_api.py           # Appel API Éducation Nationale (calendrier scolaire)
├── s3_utils.py               # Utilitaires boto3 pour AWS S3
├── script_connexion_RDS.py   # Export PostgreSQL + automatisation des vues SQL
├── data_audit.py             # Audit qualité : NaN, réconciliation Bronze vs Silver
├── Codes_Tables_SQL_DANGER/  # Scripts .sql des vues analytiques Power BI
└── requirements.txt
```

---

## Installation

**Prérequis :** Python 3.10+, un bucket AWS S3, une instance AWS RDS (PostgreSQL).

```bash
git clone https://github.com/votre-username/votre-repo.git
cd votre-repo

python -m venv .venv
.venv\Scripts\activate       # Windows
# source .venv/bin/activate  # macOS / Linux

pip install -r requirements.txt
```

---

## Configuration

Créez un fichier `.env` à la racine (**ne jamais committer ce fichier**) :

```env
# AWS
AWS_ACCESS_KEY_ID=votre_clef
AWS_SECRET_ACCESS_KEY=votre_secret
AWS_DEFAULT_REGION=eu-west-3

# RDS PostgreSQL
DB_USER=votre_user
DB_PASSWORD=votre_password
DB_HOST=votre_instance_endpoint
DB_NAME=postgres
DB_PORT=5432
```

---

## Exécution

```bash
python run_all_py.py
```

Ce script unique enchaîne automatiquement :

1. **Nettoyage** des 4 tables BAAC pour chaque année (2021–2024)
2. **Enrichissement** via l'API calendrier scolaire (détection des vacances)
3. **Jointure** sur `Num_Acc` / `id_vehicule` → table `fact_accidents`
4. **Contrôle qualité** : aucune ligne perdue entre Bronze et Silver
5. **Chargement** dans RDS (mode APPEND ou DROP selon config)
6. **Reconstruction** des vues SQL dans `Codes_Tables_SQL_DANGER/`

### Ajouter une nouvelle année (ex : 2025)

1. Déposer les 4 CSV bruts dans S3 Bronze
2. Ajouter `2025` à la liste `ANNEES` dans `main.py`
3. Relancer `python run_all_py.py`

---

## Défis & Solutions

**Incohérences inter-années (séparateurs, noms de colonnes)**
→ Détection dynamique du séparateur (`,` pour 2024, `;` pour 2021–2023) ; renommage `Accident_Id` → `Num_Acc` sur 2022 **avant** le concat (sinon 25 % de NaN)

**Valeurs sentinelles BAAC (`-1` = non renseigné)**
→ Remplacement systématique par `NaN` sur toutes les colonnes concernées avant tout calcul ou modélisation

**Clé de jointure `Num_Acc` corrompue en float**
→ Lecture forcée en `dtype=str` dès `pd.read_csv()` pour éviter la conversion scientifique (`2.024e+11` casse les jointures)

**Données géospatiales inutilisables**
→ `lat`/`long` en string avec virgule comme séparateur décimal (2021–2023) : remplacement virgule → point + conversion `pd.to_numeric`

**Bug de construction de la colonne `date`**
→ La version initiale construisait la date sur `df_2024` uniquement, laissant 166 642 `NaT` pour 2021–2023 ; corrigé en utilisant `caract_df` (dataframe complet)

**Limites de l'API calendrier scolaire**
→ Traitement par batch + mécanisme de retry pour garantir la complétude de l'enrichissement

---

## Stack technique

- **Python 3.10+** — orchestration, transformation, appels API
- **Pandas / NumPy** — nettoyage et jointures
- **boto3** — interactions AWS S3
- **SQLAlchemy / psycopg2** — chargement PostgreSQL (AWS RDS)
- **Requests** — API Éducation Nationale (vacances scolaires)
- **Power BI** — rapport analytique final (connexion ODBC suite à des problèmes SSL entre AWS et Microsoft)
- **Scikit-learn** *(post-formation)* — modèle ML prédictif des pics de risque

---

## Vues SQL analytiques (Power BI)

Le dossier `Codes_Tables_SQL_DANGER/` contient les vues reconstruites automatiquement à chaque run :

| Vue | Objectif |
|-----|----------|
| `view_caract` | Labels lisibles (météo, luminosité, intersection, type de collision), tranche horaire, jointure vacances scolaires par région |
| Autres vues | Gravité des accidents par zone géographique, profilage des usagers impliqués |

Connexion Power BI : `PostgreSQL → ton-endpoint.rds.amazonaws.com → securite_routiere `

---

## Audit & Qualité des données

`data_audit.py` assure en continu :
- Détection des `NaN` par colonne et par année
- Réconciliation Bronze vs Silver (garantit qu'aucune ligne n'est perdue durant le traitement)
- Vérification de l'absence de doublons sur les clés `(Num_Acc, id_usager)`

---

## Pistes d'évolution

**Intégration de données supplémentaires**
- Facteurs comportementaux : alcoolémie, fatigue, inattention
- Facteurs légaux : vétusté du véhicule, validité du permis, solde de points

**Corrélation avec les dispositifs de contrôle**
- Croiser la cartographie des zones accidentogènes avec l'implantation des radars
- Croiser avec les données de verbalisation (ceinture, vitesse)

**Évaluation de la limitation à 80 km/h**
- Analyse rétrospective de l'impact du passage aux 80 km/h sur les routes départementales depuis 2018
- Mesurer si la baisse de vitesse a infléchi la courbe de gravité sur le long terme

**Optimisation ML**
- Amélioration des performances du modèle de prédiction de gravité
- Modèle prédictif des pics de risque selon la météo et le calendrier scolaire

---

*Projet réalisé dans le cadre de la certification Analytic Engineer — 2026*
