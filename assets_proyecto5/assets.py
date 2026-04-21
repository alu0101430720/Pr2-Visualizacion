import os
import shutil
import glob
import pandas as pd
from dagster import asset, get_dagster_logger
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
    source_data_dir = config.source_data_dir
    target_data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
    
    logger.info(f"Ingestando datos desde {source_data_dir} hacia el repositorio en {target_data_dir}")
    
    # Copiar o asegurar que los datos estén en la carpeta del repositorio
    if os.path.exists(source_data_dir):
        if not os.path.exists(target_data_dir):
            shutil.copytree(source_data_dir, target_data_dir)
            logger.info("Datos copiados al repositorio local.")
        else:
            logger.info("Los datos ya existen en el repositorio. Verificando/Actualizando...")
            shutil.copytree(source_data_dir, target_data_dir, dirs_exist_ok=True)
    else:
        logger.warning(f"La carpeta origen {source_data_dir} no existe. No se pudo ingestar.")

    return target_data_dir

@asset(deps=[ingestar_datos_p5])
def preprocesar_datos_p5() -> str:
    """
    Asset para preprocesar los datos CSV de data-P5.
    Limpia números, corrige nombres de lugares, y elimina columnas espurias.
    Guarda los resultados por separado en una subcarpeta 'processed'.
    """
    logger = get_dagster_logger()
    
    # Usamos la ruta destino donde se copió data-P5 dentro del repositorio
    source_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
    processed_dir = os.path.join(source_dir, "processed")
    os.makedirs(processed_dir, exist_ok=True)
    
    csv_files = glob.glob(os.path.join(source_dir, "*.csv"))
    
    for file_path in csv_files:
        filename = os.path.basename(file_path)
        logger.info(f"Procesando {filename}...")
        try:
            df = pd.read_csv(file_path)
            
            # 1. Limpiar espacios en nombres de columnas
            df.columns = df.columns.str.strip()
            
            # Identificar columnas tipo 'object' (strings)
            string_cols = df.select_dtypes(include=['object']).columns
            
            for col in string_cols:
                # 1.a Trim espacios en blanco para limpiar la cadena completamente antes del regex
                try:
                    # Usar str.strip() si es posible y reemplazar 'nan' strings a verdaderos NaN
                    mask = df[col].notna()
                    df.loc[mask, col] = df.loc[mask, col].astype(str).str.strip()
                except Exception:
                    pass
                
                # 2. Formateo de lugares: INE a menudo exporta "Gomera, La" o "Palmas, Las"
                df[col] = df[col].replace(r'(?i)^([^,]+),\s*(La|El|Los|Las)$', r'\2 \1', regex=True)
                
                # 3. Reemplazar formato numérico de csv español (coma por punto)
                # Verifica si toda la celda es 'numero,numero' o '-numero,numero'
                df[col] = df[col].replace(r'^(-?\d+),(\d+)$', r'\1.\2', regex=True)
                
                # Intentar conversión a numérico para poder operar mejor en Pandas
                try:
                    df[col] = df[col].astype(float)
                except ValueError:
                    pass

            # 4. Eliminar columnas espurias
            # Generalmente son 'Unnamed' creadas al final de líneas mal formadas o columnas totalmente vacias
            cols_to_drop = [c for c in df.columns if 'Unnamed' in str(c)]
            df = df.drop(columns=cols_to_drop, errors='ignore')
            df = df.dropna(how='all', axis=1) # Limpiar columnas vacías
            
            # Guardar el dataset limpio
            out_path = os.path.join(processed_dir, filename)
            df.to_csv(out_path, index=False)
            logger.info(f"Procesamiento exitoso y guardado en: {out_path}")
            
        except Exception as e:
            logger.error(f"Error procesando el archivo {filename}: {str(e)}")
            
    return processed_dir
