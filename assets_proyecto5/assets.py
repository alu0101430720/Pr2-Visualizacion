from dagster import asset, get_dagster_logger
import os
import shutil
import config
from git import pull_or_clone_repo

@asset
def extraer_repositorio_github() -> str:
    """
    Asset que permite la carga/sincronización del repositorio desde GitHub.
    Realiza un git pull o clone utilizando la configuración y lógica separada.
    """
    logger = get_dagster_logger()
    
    pull_or_clone_repo(
        repo_url=config.GITHUB_REPO_URL,
        branch=config.GITHUB_BRANCH,
        target_dir=config.TARGET_DIR,
        logger=logger
    )
            
    return config.TARGET_DIR

@asset(deps=[extraer_repositorio_github])
def ingestar_datos_p5() -> str:
    """
    Asset para la ingesta de datos del proyecto que se encuentran en data-P5.
    Estos datos se preparan y se añaden al repositorio.
    """
    logger = get_dagster_logger()
    
    # Origen y destino de los datos
    source_data_dir = config.DATA_P5_DIR
    target_data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
    
    logger.info(f"Ingestando datos desde {source_data_dir} hacia el repositorio en {target_data_dir}")
    
    # Copiar o asegurar que los datos estén en la carpeta del repositorio
    if os.path.exists(source_data_dir):
        if not os.path.exists(target_data_dir):
            shutil.copytree(source_data_dir, target_data_dir)
            logger.info("Datos copiados al repositorio local.")
        else:
            logger.info("Los datos ya existen en el repositorio. Verificando/Actualizando...")
            import distutils.dir_util
            distutils.dir_util.copy_tree(source_data_dir, target_data_dir)
    else:
        logger.warning(f"La carpeta origen {source_data_dir} no existe. No se pudo ingestar.")

    return target_data_dir
