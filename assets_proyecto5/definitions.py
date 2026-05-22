from dagster import (
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
    define_asset_job,
    AssetSelection,
    sensor,
    SensorEvaluationContext,
    RunRequest,
    DefaultSensorStatus
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

# ══════════════════════════════════════════════════════════════════════════════
# DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    jobs=[completo_job, ingesta_preprocesado_job, visualizaciones_job, publicacion_job],
    sensors=[dataset_sensor],
)