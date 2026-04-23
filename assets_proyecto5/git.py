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
                text=True,
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
                text=True,
            )
            logger.info("Clonación completada: " + result.stdout)
        except subprocess.CalledProcessError as e:
            logger.error("Error al clonar: " + e.stderr)
            raise


def _git(cmd: list[str], cwd: str, logger) -> str:
    """Ejecuta un comando git y devuelve stdout. Lanza excepción si falla."""
    result = subprocess.run(cmd, cwd=cwd, check=True, capture_output=True, text=True)
    if result.stdout.strip():
        logger.info(" ".join(cmd) + " → " + result.stdout.strip())
    return result.stdout.strip()


def configure_git_identity(target_dir: str, logger,
                           name: str = "Dagster Pipeline",
                           email: str = "dagster@pipeline.local") -> None:
    """
    Establece user.name y user.email locales al repositorio si aún no están
    configurados. Necesario para que git commit no falle en entornos CI/CD
    donde no existe configuración global de Git.
    """
    for key, value in [("user.name", name), ("user.email", email)]:
        try:
            current = _git(["git", "config", "--local", key], target_dir, logger)
            if not current:
                raise ValueError("vacío")
        except Exception:
            _git(["git", "config", "--local", key, value], target_dir, logger)
            logger.info(f"Git identity configurada: {key} = {value}")


def stage_plots(plots_dir: str, target_dir: str, logger) -> list[str]:
    """
    Añade al índice de Git todos los PNG generados dentro de plots_dir
    que sean nuevos o hayan cambiado respecto al último commit.

    Devuelve la lista de ficheros staged (puede estar vacía si no hay
    cambios, lo que significa que los plots no han cambiado desde el
    último commit y no es necesario un nuevo commit).
    """
    # Ruta relativa al repositorio para el git add
    rel_plots = os.path.relpath(plots_dir, target_dir)
    png_pattern = os.path.join(rel_plots, "*.png")

    # git add en modo relativo al repo
    _git(["git", "add", png_pattern], target_dir, logger)

    # Comprobar qué ficheros quedaron realmente staged
    status = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        cwd=target_dir,
        capture_output=True,
        text=True,
    )
    staged = [f for f in status.stdout.strip().splitlines() if f.endswith(".png")]
    logger.info(f"{len(staged)} PNG(s) staged: {staged}")
    return staged


def commit_plots(staged: list[str], target_dir: str, logger,
                 message: str = "ci: actualizar gráficos generados por Dagster") -> str | None:
    """
    Realiza el commit de los ficheros staged.
    Si no hay nada staged devuelve None (nada que commitear).
    Devuelve el hash corto del commit creado.
    """
    if not staged:
        logger.info("No hay cambios en los plots respecto al último commit. Commit omitido.")
        return None

    _git(["git", "commit", "-m", message], target_dir, logger)
    commit_hash = _git(["git", "rev-parse", "--short", "HEAD"], target_dir, logger)
    logger.info(f"Commit creado: {commit_hash} — {len(staged)} fichero(s)")
    return commit_hash


def push_branch(branch: str, target_dir: str, logger) -> None:
    """
    Hace push de la rama al remoto 'origin'.
    Si el push falla por credenciales, relanza la excepción con un mensaje
    claro para que Dagster lo registre como fallo del asset.
    """
    try:
        _git(["git", "push", "origin", branch], target_dir, logger)
        logger.info(f"Push completado → origin/{branch}")
    except subprocess.CalledProcessError as e:
        logger.error(
            f"Push fallido. Comprueba las credenciales Git "
            f"(GIT_TOKEN / SSH key) y que la rama '{branch}' existe en origin.\n"
            f"stderr: {e.stderr}"
        )
        raise