# 🚗 ETL Accidents Routiers France

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20RDS-orange.svg)](https://aws.amazon.com/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Pipeline ETL production-grade. Données brutes (BAAC + calendrier scolaire) → S3 (nettoyé) → PostgreSQL RDS → Power BI.

## 🏗️ Architecture

```
Bronze (brut)     Silver (nettoyé)   Gold (joiné)       RDS (enrichi)
───────────────────────────────────────────────────────────────────
4 tables          Nettoyage        Master merge        fact_ & dim_
4 années       +  Renommage    +   Left join       +  SQL vues
16 fichiers       Sentinelles      Dédupliq.          Power BI
```

**Données** : 500k usagers, 750k véhicules, 35k jours vacances  
**Temps** : ~20 min (4 années)

## ⚡ Quick Start

```bash
# 1. Setup
pip install -r requirements.txt
cp .env.template .env           # Édite : AWS_* et DB_*

# 2. Lancer
python main.py full             # ETL + RDS (tout)
python main.py etl              # Juste S3 (Bronze → Silver → Gold)
python main.py rds              # Juste RDS (Silver → PostgreSQL)

# 3. Vérifier
python main.py full --dry-run   # Simuler sans écrire
```

## 📁 Fichiers Python (5 seulement)

| Fichier | Rôle |
|---------|------|
| `main.py` | CLI entry point (argparse) |
| `config.py` | Constantes centralisées (S3, RDS, mappings) |
| `connectors.py` | Classes S3, RDS, API (connecteurs) |
| `cleaning.py` | 5 fonctions nettoyage (caract, usagers, lieux, véhicules, vacances) |
| `etl.py` | Classe Pipeline (orchestration) |

## 🧹 Nettoyage appliqué

```python
# Toutes les tables
- Noms colonnes minuscules + renommage standardisé
- Clés numériques → string (1234.0 → '1234')
- Sentinelles (-1) → NaN
- Colonnes inutiles supprimées

# Caractéristiques
- Lat/long : virgule → point (2021-2023)
- Date recalculée depuis an/mois/jour/hrmn

# Usagers
- Années naissance > 2010 → aberrantes → NaN

# Lieux
- Dédouplonnage (unique Num_Acc)

# Vacances
- Range [start, end] → 1 ligne/jour
- Zones → département mapping
```

## 🔑 .env.template

```env
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=eu-west-3

DB_USER=postgres
DB_PASSWORD=...
DB_HOST=xxx.rds.amazonaws.com
DB_PORT=5432
DB_NAME=accidents_db
```

## 📊 Structure Silver → RDS

```
Silver CSV                      RDS Table
────────────────────────────────────────
caracteristiques_cleaned.csv → fact_caracteristiques
usagers_cleaned.csv          → dim_usagers
vehicules_cleaned.csv        → dim_vehicules
lieux_cleaned.csv            → fact_lieux
referentiel_vacances.csv     → dim_vac_scolaire
```

Puis : `Codes_Tables_SQL_DANGER/*.sql` → Vues Power BI

## 🚀 Options

```bash
python main.py full --dry-run      # Pas d'écriture S3/RDS
```

## 🐛 Troubleshooting

```bash
# AWS auth fail
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... python main.py etl

# RDS connection fail
psql -h <DB_HOST> -U <DB_USER> -d <DB_NAME> -c "SELECT 1;"

# Logs détaillés
python main.py full 2>&1 | tee pipeline.log
```

## 📈 Métriques

| Étape | Temps | Lignes |
|-------|-------|--------|
| Fetch S3 Bronze | 2 min | ~1.2M |
| Nettoyage | 5 min | ~1.2M |
| Merge | 3 min | ~500k |
| Push RDS | 10 min | ~2M |
| Vues SQL | 2 min | - |
| **Total** | **22 min** | - |

## ✅ Checklist déploiement

- [ ] `pip install -r requirements.txt`
- [ ] `.env` créé et rempli
- [ ] `Codes_Tables_SQL_DANGER/` peuplé (.sql présents)
- [ ] AWS S3 accessible
- [ ] RDS accessible
- [ ] `python main.py full --dry-run` OK
- [ ] `python main.py full` lancé
- [ ] RDS tables peuplées

## 💻 Dépendances

```
boto3>=1.26.0              # AWS S3
pandas>=1.5.0             # Données
sqlalchemy>=2.0.0         # ORM RDS
psycopg2-binary>=2.9.0    # Driver PostgreSQL
python-dotenv>=0.21.0     # .env
requests>=2.28.0          # API HTTP
```

## 📝 Notes

- **Dry-run** : teste sans écrire S3/RDS
- **Clé merge** : [Num_Acc, id_vehicule] pour véhicules
- **Dédupliq** : Lieux (keep first)
- **Vues SQL** : Exécutées après RDS push

## 🎯 Architecture médaillon

Suit le pattern **Medallion (Bronze/Silver/Gold)** :

1. **Bronze** : Données brutes, CSV du gouvernement
2. **Silver** : Nettoyées, typées, standardisées
3. **Gold** : Jointes, enrichies, prêtes analytics

→ **RDS** : Tables fact/dim pour Power BI

## 📜 Licence

MIT

---

**Créé pour Jedha | Projet accidents routiers France**
