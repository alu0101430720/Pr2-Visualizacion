import os
import shutil
import glob
import pandas as pd
from dagster import asset, get_dagster_logger, MetadataValue, AssetExecutionContext
import config
from git import (
    pull_or_clone_repo,
    configure_git_identity,
    stage_plots,
    commit_plots,
    push_branch,
)

@asset(group_name="ingesta")
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

@asset(deps=[extraer_repositorio_github], group_name="ingesta")
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

@asset(deps=[ingestar_datos_p5], group_name="preprocesado")
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

@asset(
    deps=[
        "plot_distribucion_lineas",
        "plot_actividad_barras",
        "plot_ocupacion_divergente",
        "plot_renta_cajas",
        "plot_mapa_distribucion_renta",
        "plot_mapa_generico",
        "plot_brecha_salarial",
        "plot_mapa_brecha_salarial",
        "plot_renta_violin",
    ],
    group_name="publicacion",
    description=(
        "Commitea y hace push al repositorio GitHub de todos los PNG generados "
        "por los assets de visualización. Solo crea commit si hay cambios reales "
        "respecto al último commit (idempotente). "
        "Requiere que Git esté configurado con credenciales válidas "
        "(HTTPS token en la URL o clave SSH)."
    ),
)
def commitear_plots_a_github(context: AssetExecutionContext) -> None:
    logger = context.log
 
    plots_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR, "plots")
 
    if not os.path.isdir(plots_dir):
        raise FileNotFoundError(
            f"El directorio de plots no existe: {plots_dir}. "
            "Asegúrate de que al menos un asset de visualización se ha materializado."
        )
 
    png_count = len(glob.glob(os.path.join(plots_dir, "*.png")))
    logger.info(f"Directorio de plots: {plots_dir} ({png_count} PNG encontrados)")
 
    # 1. Identidad Git (necesaria en entornos CI sin .gitconfig global)
    configure_git_identity(config.TARGET_DIR, logger)
 
    # 2. Stage de todos los PNG nuevos o modificados
    staged = stage_plots(plots_dir, config.TARGET_DIR, logger)
 
    # 3. Commit (omitido automáticamente si no hay cambios)
    commit_hash = commit_plots(
        staged=staged,
        target_dir=config.TARGET_DIR,
        logger=logger,
        message=(
            f"ci: actualizar {len(staged)} gráfico(s) generados por Dagster "
            f"[{', '.join(os.path.basename(f) for f in staged)}]"
        ),
    )
 
    # 4. Push
    if commit_hash:
        push_branch(config.GITHUB_BRANCH, config.TARGET_DIR, logger)
 
    # 5. Metadata visible en la UI de Dagster
    context.add_output_metadata({
        "plots_staged":  MetadataValue.int(len(staged)),
        "commit_hash":   MetadataValue.text(commit_hash or "sin cambios"),
        "rama":          MetadataValue.text(config.GITHUB_BRANCH),
        "repositorio":   MetadataValue.url(config.GITHUB_REPO_URL),
        "ficheros":      MetadataValue.md(
            "### Ficheros commiteados\n\n"
            + (
                "\n".join(f"- `{os.path.basename(f)}`" for f in staged)
                if staged else "_Sin cambios respecto al último commit._"
            )
        ),
    })