#!/bin/bash

# Variables globales
STORAGE_ACCOUNT="datalakedeviavals"
CONTAINER="data"
EXPIRATION="2024-12-01T00:00:00Z"  # Date d'expiration du SAS token

# Vérifie que le nom du blob est fourni
if [ -z "$1" ]; then
    echo "Erreur : Nom du blob (BLOB_NAME) non fourni."
    echo "Usage : bash scripts/generate_blob_sas.sh <nom_du_blob>"
    exit 1
fi

# Nom du fichier à traiter (PAS DE PRÉFIXE product_eval)
BLOB_NAME="$1"

# Debug : Affiche les informations de débogage
echo "Debug : Début du script"
echo "STORAGE_ACCOUNT=$STORAGE_ACCOUNT"
echo "CONTAINER=$CONTAINER"
echo "BLOB_NAME=$BLOB_NAME"
echo "EXPIRATION=$EXPIRATION"

# Génération du SAS Token avec Azure CLI
echo "Debug : Génération du SAS Token avec Azure CLI..."
SAS_TOKEN=$(az storage blob generate-sas \
    --account-name "$STORAGE_ACCOUNT" \
    --container-name "$CONTAINER" \
    --name "$BLOB_NAME" \
    --permissions r \
    --expiry "$EXPIRATION" \
    --output tsv)

# Vérifie si le SAS token a été généré correctement
if [ $? -ne 0 ]; then
    echo "Erreur : Échec de la génération du SAS Token."
    exit 1
fi

# Affiche le SAS Token généré
echo "Debug : SAS Token généré : $SAS_TOKEN"

# Construire l'URL complet
BLOB_URL="https://${STORAGE_ACCOUNT}.blob.core.windows.net/${CONTAINER}/${BLOB_NAME}?${SAS_TOKEN}"

# Affiche l'URL générée
echo "Debug : URL générée - $BLOB_URL"

# Téléchargement du fichier avec curl pour tester immédiatement
OUTPUT_DIR="output/parquet"
mkdir -p "$OUTPUT_DIR"  # Créer le dossier si inexistant
curl -o "${OUTPUT_DIR}/$(basename "$BLOB_NAME")" "$BLOB_URL"

# Vérifie si le téléchargement a réussi
if [ $? -eq 0 ]; then
    echo "Fichier téléchargé avec succès : ${OUTPUT_DIR}/$(basename "$BLOB_NAME")"
else
    echo "Erreur lors du téléchargement du fichier : $BLOB_NAME"
    exit 1
fi
