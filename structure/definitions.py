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

from pr2_assets import git_ops, renta, codislas, nivelestudios, checks, ia_viz, mapas

# ── Assets y checks ────────────────────────────────────────────────────────────

all_assets = load_assets_from_modules([git_ops, renta, codislas, nivelestudios, ia_viz, mapas])
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
    selection=(
        AssetSelection.keys(AssetKey("guardar_nivelestudios_limpio")) | 
        AssetSelection.keys(AssetKey("mapa_rentas_python")) |
        AssetSelection.keys(AssetKey("extraer_indicadores_istac"))
    ).upstream(),
)

# Job exclusivo para el pipeline IA (lanzarlo sin re-ejecutar limpieza)
job_ia = define_asset_job(
    name="pipeline_ia",
    selection=AssetSelection.keys(AssetKey("commit_visualizacion_ia")).upstream(),
)

# Job para el mapa municipal de rentas (incluye generación y subida a GitHub)
job_mapa = define_asset_job(
    name="pipeline_mapas_completo",
    selection=(
        # Seleccionamos los dos puntos finales de los mapas
        AssetSelection.keys("commit_mapa_python") | 
        AssetSelection.keys("commit_mapa_paro")
    ).upstream(), # .upstream() arrastra automáticamente la extracción y los PNGs
)


# ── Sensor: dispara pipeline_completo cuando cambian los ficheros de datos ─────
#
# Vigila los tres ficheros de datos crudos en la RAÍZ del repo clonado.
# Si cualquiera cambia (mtime), lanza job_completo automáticamente.
# Para activarlo: Dagster UI → Sensors → sensor_cambio_datos → ON.

_FICHEROS_VIGILADOS = [
    "distribucion-renta-canarias.csv",
    "codislas.csv",
    "nivelestudios.xlsx",
    "Municipios-2024.json",
]

@sensor(job=job_completo, minimum_interval_seconds=30)
def sensor_cambio_datos(context):
    """
    Vigila los ficheros de datos crudos en la raíz de Pr2-Visualizacion/.
    Si detecta un cambio en el mtime de cualquiera, lanza job_completo.

    El cursor almacena los mtimes como cadena separada por '|'
    en el mismo orden que _FICHEROS_VIGILADOS.
    """
    from config import REPO_DIR

    mtimes_actuales = []
    for nombre in _FICHEROS_VIGILADOS:
        ruta = os.path.join(REPO_DIR, nombre)
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
    jobs=[job_completo, job_graficos_y_commit, job_limpieza, job_ia, job_mapa],
    sensors=[sensor_cambio_datos],
)