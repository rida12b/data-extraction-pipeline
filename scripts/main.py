import subprocess
import logging
import os

# Crée le dossier logs s'il n'existe pas
os.makedirs("logs", exist_ok=True)

# Configurer les logs
logging.basicConfig(filename="logs/main_log.txt", level=logging.INFO, format="%(asctime)s - %(message)s")

def run_script(script_path):
    """
    Fonction pour exécuter un script Python et capturer sa sortie.
    """
    try:
        print(f"=== Exécution du script : {script_path} ===")
        logging.info(f"Début de l'exécution : {script_path}")

        result = subprocess.run(
            ["python", script_path], check=True, capture_output=True, text=True
        )

        print(result.stdout)  # Affiche la sortie standard du script
        logging.info(f"Succès : {script_path}\n{result.stdout}")
        print(f"=== Script terminé avec succès : {script_path} ===\n")
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de l'exécution de {script_path} :")
        print(e.stderr)  # Affiche les erreurs rencontrées
        logging.error(f"Erreur lors de l'exécution : {script_path}\n{e.stderr}")
        print(f"=== Fin avec erreur pour le script : {script_path} ===\n")

if __name__ == "__main__":
    print("=== Début de l'orchestration ===")
    logging.info("=== Début de l'orchestration ===")

    # Chemin absolu du répertoire du projet
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Liste des scripts à exécuter avec leurs chemins absolus
    scripts = [
        os.path.join(project_dir, "scripts", "extract_sql.py"),
        os.path.join(project_dir, "scripts", "extract_parquet.py"),
        os.path.join(project_dir, "scripts", "extract_csv.py"),
    ]

    # Exécuter chaque script
    for script in scripts:
        run_script(script)

    print("=== Orchestration terminée avec succès ===")
    logging.info("=== Orchestration terminée avec succès ===")
