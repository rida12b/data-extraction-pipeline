# Extraction de Données Multi-Sources


## Description
Ce projet consiste à construire un pipeline d'extraction de données multi-sources pour le compte d'Adventure Works. Il combine des données issues de fichiers Parquet, CSV compressés, et d'une base SQL Azure, afin de centraliser, nettoyer et transformer les données pour l'équipe Data Science.

---

## Fonctionnalités principales
- **Extraction des fichiers Parquet** :
  - Téléchargement des fichiers depuis un Data Lake Azure.
  - Décodage des images encodées dans les fichiers Parquet et sauvegarde en format `.webp`.
  - Extraction des métadonnées sous forme de fichier CSV.

- **Traitement des fichiers CSV compressés** :
  - Téléchargement et décompression des fichiers ZIP.
  - Transformation et sauvegarde des données sous forme de fichiers CSV prêts à l'emploi.

- **Extraction depuis une base SQL** :
  - Connexion sécurisée à une base SQL Azure via `SQLAlchemy`.
  - Exécution de requêtes pour extraire des données tabulaires.
  - Sauvegarde des résultats en CSV.

- **Sécurisation** :
  - Utilisation de tokens SAS pour accéder aux fichiers du Data Lake Azure.
  - Gestion des informations sensibles via un fichier `.env`.

---

## Prérequis
### **1. Dépendances Python**
Installe les bibliothèques nécessaires avec pip :
```bash
pip install -r requirements.txt


2. Configuration
Crée un fichier .env à la racine du projet et ajoute-y les informations suivantes :

env
Copier le code
# Variables Azure
AZURE_ACCOUNT_NAME=your_account_name
AZURE_CONTAINER_NAME=your_container_name
AZURE_SAS_EXPIRY=2024-12-01T00:00:00Z

# Variables SQL
SQL_SERVER=your_sql_server
SQL_DATABASE=your_database
SQL_USERNAME=your_username
SQL_PASSWORD=your_password
SQL_DRIVER=ODBC Driver 18 for SQL Server

3. Autorisations
Azure CLI : Assure-toi que le CLI Azure est configuré et que tu as accès au Data Lake.
Git Bash : Vérifie que Git Bash est installé sur ton système pour exécuter les scripts Bash.
Structure du projet
plaintext
Copier le code
## Structure du projet

Le projet est organisé de la manière suivante :

```plaintext
data-extraction-pipeline/
├── logs/                        # Contient les journaux d'exécution des scripts
├── output/                      # Contient les résultats générés
│   ├── images/                  # Images extraites des fichiers Parquet
│   ├── csv/                     # Fichiers CSV générés
├── scripts/                     # Contient les scripts Python et Bash
│   ├── extract_csv.py           # Script pour télécharger et traiter les fichiers ZIP (CSV)
│   ├── extract_parquet.py       # Script pour extraire les fichiers Parquet et traiter les images
│   ├── extract_sql.py           # Script pour extraire les données SQL
│   ├── generate_blob_sas.sh     # Script Bash pour générer les SAS Tokens
│   ├── main.py                  # Orchestrateur principal qui lance tout le pipeline
├── .env                         # Fichier de configuration pour les variables sensibles (non inclus dans le dépôt)
├── .gitignore                   # Liste des fichiers ignorés par Git
├── README.md                    # Documentation du projet
├── requirements.txt             # Liste des dépendances Python

Comment exécuter le projet ?

## Utilisation

### Exécution du pipeline complet
Le fichier `main.py` permet d'exécuter l'intégralité du pipeline de traitement des données, incluant :
- L'extraction des fichiers Parquet contenant des images encodées.
- Le téléchargement et le traitement des fichiers ZIP contenant des données CSV.
- L'extraction des données depuis la base de données SQL.

#### Commande pour exécuter le pipeline complet :
```bash
python scripts/main.py


Ce projet a été réalisé par Rida12b 