from azure.storage.blob import BlobClient
import os
import zipfile
import pandas as pd
import logging
from io import BytesIO
from dotenv import load_dotenv
import subprocess

# Charger les variables d'environnement
load_dotenv()

# Configurer les logs
os.makedirs('logs', exist_ok=True)
logging.basicConfig(filename='logs/csv_extraction_log.txt', level=logging.INFO)

# Fonction pour générer une URL SAS via le script Bash
def generate_blob_sas(blob_name):
    try:
        # Commande pour appeler le script Bash
        # Récupère le chemin absolu du script `generate_blob_sas.sh`
        script_dir = os.path.dirname(os.path.abspath(__file__))
        generate_sas_path = os.path.join(script_dir, "generate_blob_sas.sh")

        # Commande corrigée
        command = ["bash", generate_sas_path, blob_name]

        print(f"Commande exécutée : {' '.join(command)}")

        # Exécuter la commande et capturer la sortie avec encodage UTF-8
        result = subprocess.run(command, check=True, capture_output=True, text=True, encoding="utf-8")
        print(f"Sortie brute du script Bash :\n{result.stdout}")

        # Extraire l'URL générée depuis la sortie
        for line in result.stdout.splitlines():
            if line.startswith("Debug : URL générée - "):
                return line.replace("Debug : URL générée - ", "").strip()

        raise ValueError("URL Blob introuvable dans la sortie du script Bash.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Erreur lors de l'exécution du script Bash : {e.stderr}")
        raise RuntimeError(f"Erreur lors de l'exécution du script Bash : {e.stderr}")

# Fonction pour télécharger et traiter un fichier ZIP
def download_and_process_zip(blob_name):
    try:
        print(f"Téléchargement et traitement de {blob_name}...")

        # Générer l'URL SAS
        blob_url = generate_blob_sas(blob_name)
        print(f"URL Blob générée : {blob_url}")

        # Créer un BlobClient avec l'URL SAS
        blob_client = BlobClient.from_blob_url(blob_url=blob_url)

        # Obtenir la taille totale du fichier
        blob_properties = blob_client.get_blob_properties()
        total_size = blob_properties.size
        print(f"Taille du fichier : {total_size} octets.")

        # Télécharger le fichier ZIP en mémoire
        zip_stream = blob_client.download_blob().readall()
        print("Téléchargement complet. Décompression en cours...")

        # Vérifier si le fichier est un fichier ZIP valide
        if not zipfile.is_zipfile(BytesIO(zip_stream)):
            raise ValueError("Le fichier téléchargé n'est pas un fichier ZIP valide.")

        # Ouvrir et traiter le fichier ZIP
        with zipfile.ZipFile(BytesIO(zip_stream)) as zip_file:
            print("Contenu du fichier ZIP :")
            zip_file.printdir()

            # Extraire et traiter chaque fichier CSV
            for file_name in zip_file.namelist():
                if file_name.endswith(".csv"):
                    print(f"Extraction et traitement du fichier CSV : {file_name}...")

                    # Lire le fichier CSV dans un DataFrame Pandas
                    with zip_file.open(file_name) as csv_file:
                        data = pd.read_csv(csv_file)

                    # Sauvegarder le fichier CSV dans le dossier output/csv
                    os.makedirs("output/csv", exist_ok=True)
                    output_path = f"output/csv/{os.path.basename(file_name)}"
                    data.to_csv(output_path, index=False)
                    print(f"Fichier traité et sauvegardé : {output_path}.")
                    logging.info(f"Fichier traité et sauvegardé : {output_path}.")

    except Exception as e:
        print(f"Erreur lors du traitement de {blob_name} : {e}")
        logging.error(f"Erreur lors du traitement de {blob_name} : {e}")

# Liste des fichiers à télécharger et traiter
files = [
    "machine_learning/reviews.zip"
]

# Traiter chaque fichier
for blob_name in files:
    download_and_process_zip(blob_name)
