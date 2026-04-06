import os

from dagster import (
    AssetKey,
    AssetSelection,
    Definitions,
    RunRequest,
    define_asset_job,
    load_asset_checks_from_modules,
    load_assets_from_modules,
    sensor,
)

from pr2_assets import git_ops, renta, codislas, nivelestudios, checks, ia_viz

# ── Assets y checks ────────────────────────────────────────────────────────────

all_assets = load_assets_from_modules([git_ops, renta, codislas, nivelestudios, ia_viz])
all_checks = load_asset_checks_from_modules([checks])


# ── Jobs ───────────────────────────────────────────────────────────────────────

job_completo = define_asset_job(
    name="pipeline_completo",
    selection=AssetSelection.all(),
)

job_graficos_y_commit = define_asset_job(
    name="graficos_y_commit",
    selection=AssetSelection.keys(AssetKey("commit_ejercicio3")).upstream(),
)

job_limpieza = define_asset_job(
    name="solo_limpieza",
    selection=AssetSelection.keys(AssetKey("guardar_nivelestudios_limpio")).upstream(),
)

# Job exclusivo para el pipeline IA — útil para lanzarlo de forma aislada
# sin necesidad de re-ejecutar la limpieza y transformación de datos.
job_ia = define_asset_job(
    name="pipeline_ia",
    selection=AssetSelection.keys(AssetKey("commit_visualizacion_ia")).upstream(),
)


# ── Sensor: dispara pipeline_completo cuando cambia algún fichero de datos ─────
#
# Vigila los tres ficheros de datos crudos del proyecto. Si cualquiera de ellos
# cambia (mtime), lanza job_completo automáticamente.
#
# Para activarlo: en la UI de Dagster → Sensors → sensor_cambio_datos → ON.

_FICHEROS_VIGILADOS = [
    "distribucion-renta-canarias-checks.csv",
    "codislas-checks.csv",
    "nivelestudios-checks.xlsx",
]

@sensor(job=job_completo, minimum_interval_seconds=30)
def sensor_cambio_datos(context):
    """
    Vigila los ficheros de datos crudos en datasets-check/.
    Si detecta un cambio en el mtime de cualquiera de ellos lanza job_completo.

    El cursor almacena los últimos mtimes conocidos como cadena separada por '|'
    en el mismo orden que _FICHEROS_VIGILADOS.
    """
    from config import REPO_DIR

    mtimes_actuales = []
    for nombre in _FICHEROS_VIGILADOS:
        ruta = os.path.join(REPO_DIR, "datasets-check", nombre)
        mtimes_actuales.append(
            str(os.path.getmtime(ruta)) if os.path.exists(ruta) else "0"
        )

    cursor_actual = "|".join(mtimes_actuales)
    cursor_previo = context.cursor or ""

    if cursor_actual != cursor_previo:
        partes_previas = cursor_previo.split("|") if cursor_previo else ["0"] * len(_FICHEROS_VIGILADOS)
        ficheros_cambiados = [
            _FICHEROS_VIGILADOS[i]
            for i, (a, p) in enumerate(zip(mtimes_actuales, partes_previas))
            if a != p
        ]
        context.log.info(f"Cambios detectados en: {ficheros_cambiados}")
        context.update_cursor(cursor_actual)
        yield RunRequest(run_key=cursor_actual)


# ── Definitions ────────────────────────────────────────────────────────────────

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    jobs=[job_completo, job_graficos_y_commit, job_limpieza, job_ia],
    sensors=[sensor_cambio_datos],
)
