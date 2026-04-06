import os
import geopandas as gpd
import pandas as pd
from dagster import asset, Output, MetadataValue, OpExecutionContext
from plotnine import ggplot, aes, geom_map, theme_void, labs, scale_fill_cmap
from config import REPO_DIR, DIR_GRAFICOS

@asset
def mapa_rentas_python(context: OpExecutionContext, integrar_renta_codislas: pd.DataFrame) -> Output:
    """
    Genera un mapa de municipios de Canarias coloreado por la renta media.
    """
    # 1. Cargar el GeoJSON (Municipios-2024.json)
    ruta_geojson = os.path.join(REPO_DIR, "Municipios-2024.json")
    context.log.info(f"Cargando mapa desde: {ruta_geojson}")
    gdf = gpd.read_file(ruta_geojson)

    # 2. Preparar los datos de renta del pipeline existente
    # Filtramos por una fuente común (Sueldos y Salarios)
    df_renta = integrar_renta_codislas[
        (integrar_renta_codislas["Fuente_Renta_Code"] == "SUELDOS_SALARIOS")
    ].copy()
    
    # Calculamos la media por municipio (Territorio) 
    df_mapa_data = df_renta.groupby("Territorio")["Porcentaje"].mean().reset_index()

    # 3. Cruce (Merge)
    # Usamos 'NAME' del GeoJSON y 'Territorio' de tu DataFrame limpio
    gdf_final = gdf.merge(
        df_mapa_data, 
        left_on="label", 
        right_on="Territorio", 
        how="left"
    )

    # 4. Crear la visualización con plotnine 
    mapa = (
        ggplot(gdf_final)
        + geom_map(aes(fill="Porcentaje"))
        + scale_fill_cmap(cmap_name="viridis")
        + theme_void()
        + labs(
            title="Distribución de Rentas por Municipio",
            subtitle="Canarias 2024 · Fuente: ISTAC",
            fill="Renta Media (%)"
        )
    )

    # 5. Guardado físico 
    os.makedirs(DIR_GRAFICOS, exist_ok=True)
    ruta_salida = os.path.join(DIR_GRAFICOS, "mapa_rentas_municipios.png")
    mapa.save(ruta_salida, width=12, height=8, dpi=150)
    
    return Output(
        value=ruta_salida,
        metadata={
            "ruta": MetadataValue.path(ruta_salida),
            "municipios_detectados": MetadataValue.int(len(gdf_final.dropna(subset=['Porcentaje'])))
        }
    )