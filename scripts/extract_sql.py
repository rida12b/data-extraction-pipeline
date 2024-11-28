import pandas as pd
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.env"))
if not os.path.exists(dotenv_path):
    print(f"Erreur : Le fichier .env est introuvable au chemin : {dotenv_path}")
    exit()
load_dotenv(dotenv_path)

# Configurer les logs
os.makedirs("logs", exist_ok=True)
logging.basicConfig(filename='logs/extraction_log.txt', level=logging.INFO)

# Charger les variables SQL
print("Chargement des variables d'environnement SQL...")
sql_server = os.getenv("SQL_SERVER")
sql_database = os.getenv("SQL_DATABASE")
sql_username = os.getenv("SQL_USERNAME")
sql_password = os.getenv("SQL_PASSWORD")
sql_driver = os.getenv("SQL_DRIVER")

# Vérifier les variables nécessaires
if not all([sql_server, sql_database, sql_username, sql_password, sql_driver]):
    print("Erreur : Certaines variables d'environnement SQL sont manquantes.")
    print(f"SQL_SERVER = {sql_server}")
    print(f"SQL_DATABASE = {sql_database}")
    print(f"SQL_USERNAME = {sql_username}")
    print(f"SQL_PASSWORD = {sql_password}")
    print(f"SQL_DRIVER = {sql_driver}")
    exit()

# Créer la chaîne de connexion SQL
connection_string = (
    f"mssql+pyodbc://{sql_username}:{sql_password}@{sql_server}/{sql_database}?driver={sql_driver}"
)

# Étape 1 : Connexion à la base de données
try:
    print("Connexion à la base SQL...")
    engine = create_engine(connection_string)
    connection = engine.connect()
    print("Connexion réussie à la base SQL.")
    logging.info("Connexion réussie à la base SQL.")
except Exception as e:
    print(f"Erreur lors de la connexion à la base SQL : {e}")
    logging.error(f"Erreur lors de la connexion à la base SQL : {e}")
    exit()

# Étape 2 : Vérification du schéma de la table
try:
    print("Vérification du schéma de la table...")
    schema_query = """
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = 'Product'
    """
    schema_df = pd.read_sql(schema_query, connection)
    print("Schéma de la table 'Product' récupéré avec succès :")
    print(schema_df)
    logging.info("Schéma de la table 'Product' récupéré avec succès.")
except Exception as e:
    print(f"Erreur lors de la récupération du schéma : {e}")
    logging.error(f"Erreur lors de la récupération du schéma : {e}")

# Étape 3 : Extraction des données
try:
    print("Extraction des données de la table 'Product'...")
    query = """
    SELECT ProductID, Name, StandardCost, ListPrice, ModifiedDate
    FROM Production.Product
    """
    df = pd.read_sql(query, connection)
    print("Requête SQL exécutée avec succès.")
    logging.info("Requête SQL exécutée avec succès.")
except Exception as e:
    print(f"Erreur lors de l'exécution de la requête SQL : {e}")
    logging.error(f"Erreur lors de l'exécution de la requête SQL : {e}")
    exit()

# Étape 4 : Sauvegarde des données
try:
    print("Sauvegarde des données extraites...")
    os.makedirs("output/csv", exist_ok=True)
    output_path = "output/csv/products_from_sql.csv"
    df.to_csv(output_path, index=False)
    print(f"Les données SQL ont été sauvegardées dans {output_path}.")
    logging.info(f"Les données SQL ont été sauvegardées dans {output_path}.")
except Exception as e:
    print(f"Erreur lors de la sauvegarde du fichier CSV : {e}")
    logging.error(f"Erreur lors de la sauvegarde du fichier CSV : {e}")
