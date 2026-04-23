# config.py
import os

TARGET_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # Apunta a C:\...\Viz\Pr2-Visualizacion
BASE_DIR = os.path.dirname(TARGET_DIR) # Apunta a Viz

source_data_dir = os.path.join(BASE_DIR, "data-P5")
DATA_P5_DIR = "data-P5"

_token = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO_URL = (
    f"https://{_token}@github.com/alu0101430720/Pr2-Visualizacion.git"
    if _token
    else "https://github.com/alu0101430720/Pr2-Visualizacion.git"
)

GITHUB_BRANCH = "Proyecto_Final"

