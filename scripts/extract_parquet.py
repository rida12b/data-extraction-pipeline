from azure.storage.blob import BlobClient
import os
import subprocess
from tqdm import tqdm
from dotenv import load_dotenv
from urllib.parse import urlparse

import pandas as pd
import pyarrow.parquet as pq
from PIL import Image
from io import BytesIO

# Charger les variables d'environnement
load_dotenv()

# Définir le sous-dossier pour les fichiers Parquet
SUBFOLDER = "product_eval"  # Sous-dossier dans le conteneur Azure

# Chemin complet vers bash.exe (assurez-vous que ce chemin est correct sur votre machine)
GIT_BASH_PATH = "C:/Program Files/Git/usr/bin/bash.exe"
if not os.path.exists(GIT_BASH_PATH):
    print("Erreur : Git Bash n'est pas installé ou le chemin est incorrect.")
    exit()

# Fonction pour appeler le script Bash et générer le SAS Token
def generate_blob_sas(blob_name):
    try:
        # Inclure le sous-dossier dans le chemin du blob
        blob_with_subfolder = f"product_eval/{blob_name}"  # Retirer 'data/' de l'URL

        # Commande pour appeler le script Bash directement
        # Récupère le chemin absolu du script `generate_blob_sas.sh`
        script_dir = os.path.dirname(os.path.abspath(__file__))
        generate_sas_path = os.path.join(script_dir, "generate_blob_sas.sh")

        # Commande corrigée
        command = ["bash", generate_sas_path, blob_with_subfolder]

        print(f"Commande exécutée : {' '.join(command)}")  # Debug : Affiche la commande

        # Exécuter la commande
        result = subprocess.run(command, check=True, capture_output=True, text=True, encoding="utf-8")

        # Afficher la sortie brute pour débogage
        print(f"Sortie brute du script Bash :\n{result.stdout}")

        # Parse la sortie pour trouver l'URL Blob
        for line in result.stdout.splitlines():
            if line.startswith("Debug : URL générée - "):
                return line.replace("Debug : URL générée - ", "").strip()
        raise ValueError("URL Blob introuvable dans la sortie du script Bash.")
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de l'exécution du script Bash : {e.stderr}")
        exit()

# Fonction de téléchargement avec barre de progression
def download_blob_with_progress(blob_url, output_path):
    try:
        # Parser l'URL du blob
        parsed_url = urlparse(blob_url)
        account_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        path_parts = parsed_url.path.lstrip('/').split('/')
        container_name = path_parts[0]
        blob_name = '/'.join(path_parts[1:])
        sas_token = parsed_url.query

        # Créer le BlobClient avec les composants parsés
        blob_client = BlobClient(
            account_url=account_url,
            container_name=container_name,
            blob_name=blob_name,
            credential=sas_token
        )

        # Obtenir les propriétés du blob pour la taille totale
        properties = blob_client.get_blob_properties()
        total_size = properties.size

        with open(output_path, "wb") as file:
            stream = blob_client.download_blob()
            with tqdm(total=total_size, unit="B", unit_scale=True, desc=output_path) as pbar:
                for chunk in stream.chunks():
                    file.write(chunk)
                    pbar.update(len(chunk))
        return True
    except Exception as e:
        print(f"Erreur lors du téléchargement avec progression : {e}")
        return False

# Fonction pour traiter le fichier Parquet
def process_parquet_file(parquet_file_path):
    try:
        print(f"Traitement du fichier Parquet : {parquet_file_path}")
        table = pq.read_table(parquet_file_path)
        df = table.to_pandas()

        print(f"Nombre de lignes lues : {len(df)}")
        print(f"Colonnes disponibles : {df.columns.tolist()}")

        os.makedirs("output/images", exist_ok=True)
        os.makedirs("output/metadata", exist_ok=True)

        metadata_list = []

        total_products = 0
        products_with_image = 0
        products_without_image = 0

        for index, row in df.iterrows():
            total_products += 1
            product_id = row.get('item_ID', f'unknown_{index}')
            title = row.get('title', '')
            query = row.get('query', '')
            position = row.get('position', '')
            image_data = row.get('image', None)

            if isinstance(image_data, dict) and 'bytes' in image_data:
                try:
                    # Extraire les données binaires de l'image
                    image_bytes = image_data['bytes']
                    image = Image.open(BytesIO(image_bytes))

                    # Construire le chemin pour sauvegarder l'image
                    image_filename = f"{product_id}_{index}.webp"  # Format WEBP
                    image_path = os.path.join("output/images", image_filename)

                    # Sauvegarder l'image
                    image.save(image_path, format="WEBP")
                    # print(f"Image sauvegardée : {image_path}")

                    products_with_image += 1
                except Exception as e:
                    print(f"Erreur lors du traitement de l'image pour le produit {product_id} : {e}")
                    image_filename = None
                    products_without_image += 1
            else:
                print(f"Image non valide ou non encodée pour le produit {product_id}")
                image_filename = None
                products_without_image += 1

            metadata_list.append({
                'product_id': product_id,
                'title': title,
                'query': query,
                'position': position,
                'image_filename': image_filename
            })

        metadata_df = pd.DataFrame(metadata_list)
        metadata_output_path = os.path.join("output/metadata", f"{os.path.basename(parquet_file_path)}.csv")
        metadata_df.to_csv(metadata_output_path, index=False)
        print(f"Métadonnées sauvegardées : {metadata_output_path}")

        print(f"Nombre total de produits traités : {total_products}")
        print(f"Produits avec image : {products_with_image}")
        print(f"Produits sans image : {products_without_image}")

    except Exception as e:
        print(f"Erreur lors du traitement du fichier Parquet {parquet_file_path} : {e}")

# Liste des fichiers à télécharger
files = [
    "test-00000-of-00003.parquet",
    "test-00001-of-00003.parquet",
    "test-00002-of-00003.parquet",
]

# Télécharger et traiter chaque fichier
for blob_name in files:
    try:
        print(f"Téléchargement et traitement de {blob_name}...")
        blob_url = generate_blob_sas(blob_name)
        print(f"URL Blob générée : {blob_url}")

        output_dir = "output/parquet"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, blob_name)

        if download_blob_with_progress(blob_url, output_path):
            print(f"Fichier téléchargé avec succès : {output_path}")
            # Traiter le fichier Parquet
            process_parquet_file(output_path)
        else:
            print(f"Échec du téléchargement : {blob_name}")
    except Exception as e:
        print(f"Erreur lors du traitement de {blob_name} : {e}")
