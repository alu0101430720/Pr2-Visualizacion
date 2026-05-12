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

_ISLAS      = {"Canarias", "Tenerife", "Gran Canaria", "La Palma", "La Gomera",
               "El Hierro", "Lanzarote", "Fuerteventura"}
_PROVINCIAS = {"Las Palmas"}

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
    Preprocesa CSV y TSV de data-P5. Guarda todo en processed/ como CSV.

    CSV (comportamiento original sin cambios):
      - Limpia espacios, corrige formato invertido "Gomera, La",
        sustituye separador decimal español, elimina columnas Unnamed/vacías.

    TSV — gini.tsv y rentas.tsv (lógica adicional):
      - Elimina columnas totalmente NaN (ESTADO_OBSERVACION,
        CONFIDENCIALIDAD_OBSERVACION no aportan información útil).
      - Deduplica Santa Cruz de Tenerife: ISTAC incluye el municipio
        y la provincia homónima bajo el mismo nombre. Se conserva el
        valor MÍNIMO por (TERRITORIO, TIME_PERIOD, MEDIDAS) ya que el
        municipio tiene siempre Gini menor que su provincia.
      - Añade columna tipo_territorio ('isla'|'provincia'|'municipio')
        para facilitar filtrado en los assets de plot sin repetir lógica.
      - Guarda como .csv (sin tabulaciones) para consistencia con el
        resto del pipeline.
    """
    logger = get_dagster_logger()
    source_dir    = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
    processed_dir = os.path.join(source_dir, "processed")
    os.makedirs(processed_dir, exist_ok=True)

    # ── CSV ───────────────────────────────────────────────────────────────────
    for file_path in glob.glob(os.path.join(source_dir, "*.csv")):
        filename = os.path.basename(file_path)
        logger.info(f"Procesando CSV: {filename}")
        try:
            sep = ';' if 'contratos' in filename else ','
            df = pd.read_csv(file_path, sep=sep)
            df.columns = df.columns.str.strip()
            for col in df.select_dtypes(include=["object"]).columns:
                try:
                    mask = df[col].notna()
                    df.loc[mask, col] = df.loc[mask, col].astype(str).str.strip()
                except Exception:
                    pass
                df[col] = df[col].replace(
                    r"(?i)^([^,]+),\s*(La|El|Los|Las)$", r"\2 \1", regex=True
                )
                df[col] = df[col].replace(r"^(-?\d+),(\d+)$", r"\1.\2", regex=True)
                try:
                    df[col] = df[col].astype(float)
                except ValueError:
                    pass
            df = df.drop(columns=[c for c in df.columns if "Unnamed" in str(c)],
                         errors="ignore")
            df = df.dropna(how="all", axis=1)
            df.to_csv(os.path.join(processed_dir, filename), index=False)
            logger.info(f"  ✓ {filename}")
        except Exception as e:
            logger.error(f"  ✗ {filename}: {e}")

    # ── TSV ───────────────────────────────────────────────────────────────────
    for file_path in glob.glob(os.path.join(source_dir, "*.tsv")):
        filename = os.path.basename(file_path)
        logger.info(f"Procesando TSV: {filename}")
        try:
            df = pd.read_csv(file_path, sep="\t")
            df.columns = df.columns.str.strip()

            # 1. Eliminar columnas totalmente NaN
            df = df.dropna(how="all", axis=1)

            # 2. Limpiar espacios en strings
            for col in df.select_dtypes(include=["object"]).columns:
                mask = df[col].notna()
                df.loc[mask, col] = df.loc[mask, col].astype(str).str.strip()

            # 3. Corregir formato invertido en TERRITORIO
            if "TERRITORIO" in df.columns:
                df["TERRITORIO"] = df["TERRITORIO"].replace(
                    r"(?i)^([^,]+),\s*(La|El|Los|Las)$", r"\2 \1", regex=True
                )

            # 4. Deduplicar SC Tenerife (municipio vs provincia):
            #    keep='first' tras sort ascendente → valor más bajo = municipio
            if "OBS_VALUE" in df.columns:
                df = (
                    df.sort_values("OBS_VALUE")
                      .drop_duplicates(
                          subset=["TERRITORIO", "TIME_PERIOD", "MEDIDAS"],
                          keep="first",
                      )
                      .reset_index(drop=True)
                )

            # 5. Etiquetar tipo de territorio
            if "TERRITORIO" in df.columns:
                def _tipo(t):
                    if t in _ISLAS:      return "isla"
                    if t in _PROVINCIAS: return "provincia"
                    return "municipio"
                df["tipo_territorio"] = df["TERRITORIO"].apply(_tipo)

            # 6. Guardar como CSV
            out_name = filename.replace(".tsv", ".csv")
            df.to_csv(os.path.join(processed_dir, out_name), index=False)
            logger.info(f"  ✓ {out_name} ({len(df)} filas)")
        except Exception as e:
            logger.error(f"  ✗ {filename}: {e}")

    return processed_dir

@asset(
    deps=[
        "plot_actividad_barras",
        "plot_ocupacion_divergente",
        "plot_brecha_salarial",
        "plot_mapa_brecha_salarial",
        "plot_gini_evolucion_islas",
        "plot_heatmap_segregacion_sectorial",
        "plot_covid_sueldos_islas",
        "plot_covid_prestaciones_islas",
        "plot_brecha_temporal_edad",
        "plot_historico_tipos_contrato_por_edad",
        "plot_actividad_barras_canarias",
        "plot_ocupacion_divergente_canarias",
        "plot_mapa_brecha_salarial_canarias",
        "plot_mapa_feminizacion_municipios",
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