import subprocess
import os

def pull_or_clone_repo(repo_url: str, branch: str, target_dir: str, logger) -> None:
    """
    Sincroniza un repositorio Git (pull si existe, clone si no).
    """
    if os.path.exists(target_dir) and os.path.isdir(os.path.join(target_dir, ".git")):
        logger.info(f"El repositorio ya existe en {target_dir}. Haciendo pull de la rama {branch}...")
        try:
            result = subprocess.run(
                ["git", "pull", "origin", branch],
                cwd=target_dir,
                check=True,
                capture_output=True,
                text=True
            )
            logger.info("Pull completado: " + result.stdout)
        except subprocess.CalledProcessError as e:
            logger.error("Error al hacer pull: " + e.stderr)
            raise
    else:
        logger.info(f"Clonando el repositorio {repo_url} (rama {branch}) en {target_dir}...")
        try:
            result = subprocess.run(
                ["git", "clone", "-b", branch, repo_url, target_dir],
                check=True,
                capture_output=True,
                text=True
            )
            logger.info("Clonación completada: " + result.stdout)
        except subprocess.CalledProcessError as e:
            logger.error("Error al clonar: " + e.stderr)
            raise
