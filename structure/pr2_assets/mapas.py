import os
import geopandas as gpd
import pandas as pd
from dagster import asset, Output, MetadataValue, OpExecutionContext
from plotnine import ggplot, aes, geom_map, theme_void, labs, scale_fill_cmap, theme, element_text

from config import REPO_DIR, DIR_GRAFICOS, GIT_BRANCH, repo_url
from utils import commit_and_push
from .git_ops import get_github_token

# ── Funciones de Limpieza ──────────────────────────────────────────────────────

def _limpiar_nombre_municipio(texto) -> str:
    """
    Normaliza nombres: 'Palmas de Gran Canaria, Las' -> 'Las Palmas De Gran Canaria'
    y aplica Title Case para asegurar la coincidencia en el merge.
    """
    if pd.isna(texto):
        return ""
    texto = str(texto).strip()
    # Corregir formato de coma (Apellido, Artículo)
    if "," in texto:
        nombre, articulo = [p.strip() for p in texto.split(",", 1)]
        texto = f"{articulo} {nombre}"
    # Normalizar a Title Case y quitar espacios extra
    return texto.title().strip()

# ── Assets ─────────────────────────────────────────────────────────────────────

@asset
def mapa_rentas_python(context: OpExecutionContext, integrar_renta_codislas: pd.DataFrame) -> Output:
    """
    Genera un mapa de coropletas de la renta media por municipio cruzando
    el TopoJSON del ISTAC con los datos del pipeline.
    """
    # 1. Cargar el mapa (TopoJSON/GeoJSON)
    ruta_geojson = os.path.join(REPO_DIR, "Municipios-2024.json")
    if not os.path.exists(ruta_geojson):
        raise FileNotFoundError(f"No se encuentra el archivo de mapa en: {ruta_geojson}")
    
    context.log.info(f"Cargando cartografía desde: {ruta_geojson}")
    gdf = gpd.read_file(ruta_geojson)

    # 2. Limpiar nombres en el JSON (columna 'label')
    # El archivo del ISTAC usa 'label' para el nombre del municipio
    gdf['municipio_clean'] = gdf['label'].apply(_limpiar_nombre_municipio)

    # 3. Preparar datos de renta
    # Filtramos por una fuente específica (Sueldos y Salarios) para el mapa
    df_renta = integrar_renta_codislas[
        (integrar_renta_codislas["Fuente_Renta_Code"] == "SUELDOS_SALARIOS")
    ].copy()
    
    # Aseguramos que los nombres en el DataFrame también estén limpios/normalizados
    df_renta['Territorio_clean'] = df_renta['Territorio'].apply(_limpiar_nombre_municipio)
    
    # Calculamos la media por municipio
    df_mapa_data = df_renta.groupby("Territorio_clean")["Porcentaje"].mean().reset_index()

    # 4. Cruce (Merge)
    gdf_final = gdf.merge(
        df_mapa_data, 
        left_on="municipio_clean", 
        right_on="Territorio_clean", 
        how="left"
    )

    # Auditoría de cruce en logs
    n_total = len(gdf)
    n_con_datos = len(gdf_final.dropna(subset=['Porcentaje']))
    context.log.info(f"Cruce completado: {n_con_datos} de {n_total} municipios vinculados.")
    
    if n_con_datos < n_total:
        faltantes = gdf_final[gdf_final['Porcentaje'].isna()]['municipio_clean'].tolist()
        context.log.warning(f"Municipios sin datos vinculados: {faltantes}")

    # 5. Crear la visualización con plotnine
    mapa = (
        ggplot(gdf_final)
        + geom_map(aes(fill="Porcentaje"))
        + scale_fill_cmap(cmap_name="viridis")
        + theme_void()
        + labs(
            title="Distribución de Rentas por Municipio (Canarias)",
            subtitle="Indicador: Sueldos y Salarios · Fuente: ISTAC",
            fill="Media %"
        )
        + theme(
            plot_title=element_text(size=16, fontweight='bold'),
            legend_position='right'
        )
    )

    # 6. Guardar archivo
    os.makedirs(DIR_GRAFICOS, exist_ok=True)
    ruta_salida = os.path.join(DIR_GRAFICOS, "mapa_rentas_municipios.png")
    mapa.save(ruta_salida, width=12, height=8, dpi=150)
    
    return Output(
        value=ruta_salida,
        metadata={
            "ruta_archivo": MetadataValue.path(ruta_salida),
            "municipios_totales": MetadataValue.int(n_total),
            "municipios_con_exito": MetadataValue.int(n_con_datos),
            "porcentaje_cobertura": MetadataValue.float(round((n_con_datos/n_total)*100, 2))
        }
    )

# @asset
# def commit_mapa_python(
#     context: OpExecutionContext,
#     mapa_rentas_python: str,
# ) -> None:
#     """Sube el mapa generado al repositorio de GitHub."""
#     commit_and_push(
#         repo_dir=REPO_DIR,
#         remote_url=repo_url(get_github_token()),
#         branch=GIT_BRANCH,
#         files=[mapa_rentas_python],
#         message="practica4: actualización de mapa de rentas municipal",
#         ctx=context,
#     )