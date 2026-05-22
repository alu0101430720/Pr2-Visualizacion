from dagster import (
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
    define_asset_job,
    AssetSelection,
    sensor,
    SensorEvaluationContext,
    RunRequest,
    DefaultSensorStatus,
    AssetKey
)
import os
import glob
import json
import assets
import checks_p5
import plots_assets

# Cargar todos los assets y checks
all_assets = load_assets_from_modules([assets, plots_assets])
all_checks = load_asset_checks_from_modules([checks_p5])

# ══════════════════════════════════════════════════════════════════════════════
# JOBS
# ══════════════════════════════════════════════════════════════════════════════

# 1. Job completo: ejecuta todo el pipeline (ingesta, preprocesado, plots y publicacion)
completo_job = define_asset_job(
    name="completo_job",
    selection=AssetSelection.all(),
    description="Ejecuta todo el pipeline: desde la ingesta hasta la publicación en GitHub."
)

# 2. Job de ingesta y preprocesado: solo obtiene y procesa los datos fuente
ingesta_preprocesado_job = define_asset_job(
    name="ingesta_preprocesado_job",
    selection=AssetSelection.groups("ingesta", "preprocesado"),
    description="Descarga y preprocesa los datos iniciales sin actualizar las visualizaciones."
)

# 3. Job de visualizaciones: genera únicamente los gráficos a partir de datos ya procesados
visualizaciones_job = define_asset_job(
    name="visualizaciones_job",
    selection=AssetSelection.groups("viz_estructura_laboral", "viz_desigualdad_y_renta"),
    description="Genera los gráficos del proyecto a partir de los datos preprocesados."
)

# 4. Job de publicación: realiza el commit y push de las imágenes al repositorio
publicacion_job = define_asset_job(
    name="publicacion_job",
    selection=AssetSelection.groups("publicacion"),
    description="Sube a GitHub los gráficos que se encuentren en la carpeta de plots."
)

# ══════════════════════════════════════════════════════════════════════════════
# SENSORS
# ══════════════════════════════════════════════════════════════════════════════

@sensor(
    job=completo_job,
    default_status=DefaultSensorStatus.RUNNING,
    description="Sensor que vigila la carpeta de datos data-P5 y lanza el pipeline completo ante cualquier cambio."
)
def dataset_sensor(context: SensorEvaluationContext):
    """
    Sensor que monitorea los archivos CSV y TSV en la carpeta de origen.
    Si se añade o modifica algún archivo de datos, lanza el 'completo_job'.
    """
    import config
    
    # Directorio de vigilancia
    watch_dir = config.source_data_dir
    # Si por alguna razón el directorio de origen no existe, monitoreamos el local en el repo
    if not os.path.exists(watch_dir):
        watch_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
        
    if not os.path.exists(watch_dir):
        context.log.warning(f"El directorio de datos a vigilar no existe: {watch_dir}")
        return
        
    # Obtener archivos de datos (.csv y .tsv)
    csv_files = glob.glob(os.path.join(watch_dir, "*.csv"))
    tsv_files = glob.glob(os.path.join(watch_dir, "*.tsv"))
    all_files = csv_files + tsv_files
    
    # Usamos un cursor guardado en JSON (dict de filename -> mtime)
    last_cursor = json.loads(context.cursor) if context.cursor else {}
    
    current_cursor = {}
    has_changes = False
    
    for f in all_files:
        basename = os.path.basename(f)
        try:
            mtime = os.path.getmtime(f)
            current_cursor[basename] = mtime
            
            if basename not in last_cursor or last_cursor[basename] != mtime:
                has_changes = True
                context.log.info(f"Archivo nuevo o modificado detectado: {basename}")
        except OSError as e:
            context.log.error(f"Error leyendo mtime de {basename}: {e}")
            
    # Detectar también si se han eliminado archivos
    if set(last_cursor.keys()) != set(current_cursor.keys()):
        has_changes = True
        context.log.info("Se detectaron archivos eliminados en el directorio de datos.")
        
    # Guardar el nuevo cursor
    context.update_cursor(json.dumps(current_cursor))
    
    if has_changes:
        # Usar la marca de tiempo máxima como run_key para evitar lanzamientos redundantes en el mismo instante
        max_mtime = max(current_cursor.values()) if current_cursor else 0
        run_key = f"datasets_updated_{max_mtime}"
        context.log.info(f"Sensor activado. Lanzando completo_job con run_key: {run_key}")
        return RunRequest(
            run_key=run_key,
            message="Se detectaron adiciones o modificaciones en los conjuntos de datos de data-P5."
        )


@sensor(
    job=visualizaciones_job,
    default_status=DefaultSensorStatus.RUNNING,
    description="Sensor que detecta si falta algún gráfico en la carpeta plots y lo genera automáticamente."
)
def missing_plots_sensor(context: SensorEvaluationContext):
    """
    Sensor que lee 'plots.yaml', verifica la existencia de cada archivo PNG
    en la carpeta 'plots' y lanza una ejecución para generar los que falten.
    """
    import yaml
    import config
    
    # Ruta del plots.yaml
    yaml_path = os.path.join(os.path.dirname(__file__), "plots.yaml")
    if not os.path.exists(yaml_path):
        context.log.warning(f"No se encontró el archivo plots.yaml en: {yaml_path}")
        return
        
    with open(yaml_path, "r", encoding="utf-8") as f:
        plots_cfg = yaml.safe_load(f) or {}
        
    expected_plots = plots_cfg.get("expected_plots", [])
    if not expected_plots:
        context.log.warning("No hay gráficos definidos en plots.yaml.")
        return
        
    # Directorio de plots
    plot_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR, "plots")
    
    # Identificar cuáles gráficos faltan
    missing_assets = []
    missing_filenames = []
    
    for item in expected_plots:
        filename = item.get("filename")
        asset_name = item.get("asset_name")
        if not filename or not asset_name:
            continue
            
        file_path = os.path.join(plot_dir, filename)
        if not os.path.exists(file_path):
            missing_assets.append(AssetKey(asset_name))
            missing_filenames.append(filename)
            
    if not missing_assets:
        context.log.info("Todos los gráficos esperados están presentes en la carpeta plots.")
        return
        
    context.log.info(f"Gráficos faltantes detectados: {missing_filenames}. Generando ejecución para recrearlos...")
    
    # Creamos un run_key basado en la lista de archivos que faltan para no duplicar ejecuciones si ya está en curso
    missing_filenames.sort()
    run_key = f"missing_plots_{'_'.join(missing_filenames)}"
    
    return RunRequest(
        run_key=run_key,
        asset_selection=missing_assets,
        message=f"Ejecución disparada automáticamente por falta de gráficos: {', '.join(missing_filenames)}"
    )

# ══════════════════════════════════════════════════════════════════════════════
# DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    jobs=[completo_job, ingesta_preprocesado_job, visualizaciones_job, publicacion_job],
    sensors=[dataset_sensor, missing_plots_sensor],
)