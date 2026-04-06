import os
import geopandas as gpd
import pandas as pd
from dagster import asset, Output, MetadataValue, OpExecutionContext
from plotnine import ggplot, aes, geom_map, theme_void, labs, scale_fill_cmap, theme, element_text
import json 

# Importaciones de configuración y utilidades del proyecto
from config import REPO_DIR, DIR_GRAFICOS, DIR_CLEAN, GIT_BRANCH, repo_url
from utils import commit_and_push
from .git_ops import get_github_token

# ── Funciones de Limpieza ──────────────────────────────────────────────────────

def _limpiar_nombre_municipio(texto) -> str:
    """
    Normaliza nombres para asegurar la coincidencia en el merge:
    'Palmas de Gran Canaria, Las' -> 'Las Palmas De Gran Canaria'.
    """
    if pd.isna(texto):
        return ""
    texto = str(texto).strip()
    # Corregir formato de coma (Apellido, Artículo)
    if "," in texto:
        nombre, articulo = [p.strip() for p in texto.split(",", 1)]
        texto = f"{articulo} {nombre}"
    # Normalizar a Title Case para consistencia con el pipeline
    return texto.title().strip()

# ── Assets ─────────────────────────────────────────────────────────────────────

@asset
def extraer_indicadores_istac(context: OpExecutionContext) -> Output:
    """
    Extrae los indicadores laborales (psal_t, ppar_t, tsal_t) del JSON 
    y los exporta a un CSV para Power BI.
    """
    ruta_json = os.path.join(REPO_DIR, "Municipios-2024.json")
    
    with open(ruta_json, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Navegamos por la estructura específica del TopoJSON del ISTAC
    geometrias = data['objects']['Municipios-2024']['geometries']
    
    # Extraemos las propiedades de cada municipio
    datos_municipios = [g['properties'] for g in geometrias]
    df_indicadores = pd.DataFrame(datos_municipios)
    
    # Limpieza: Renombramos 'label' a 'Territorio' para facilitar el cruce en Power BI
    df_indicadores = df_indicadores.rename(columns={'label': 'Territorio'})
    
    # Guardar en la carpeta de datasets limpios
    os.makedirs(DIR_CLEAN, exist_ok=True)
    ruta_csv = os.path.join(DIR_CLEAN, "indicadores_istac_2024.csv")
    df_indicadores.to_csv(ruta_csv, index=False)
    
    context.log.info(f"Fichero de indicadores generado en: {ruta_csv}")
    
    return Output(
        value=ruta_csv,
        metadata={
            "filas": MetadataValue.int(len(df_indicadores)),
            "columnas": MetadataValue.text(str(list(df_indicadores.columns))),
            "indicadores_clave": MetadataValue.text("psal_t, ppar_t, tsal_t")
        }
    )

@asset
def mapa_rentas_python(context: OpExecutionContext, integrar_renta_codislas: pd.DataFrame) -> Output:
    """
    Genera un mapa de coropletas de la renta municipal para el año más reciente.
    Sigue los principios de la gramática de gráficos y DataOps.
    """
    # 1. Cargar la cartografía (TopoJSON/GeoJSON)
    ruta_geojson = os.path.join(REPO_DIR, "Municipios-2024.json") 
    if not os.path.exists(ruta_geojson):
        raise FileNotFoundError(f"No se encuentra el archivo de mapa en: {ruta_geojson}")
    
    context.log.info(f"Cargando municipios desde: {ruta_geojson}")
    gdf = gpd.read_file(ruta_geojson)

    # 2. Limpieza de nombres en el mapa (columna 'label' identificada en el JSON)
    gdf['municipio_clean'] = gdf['label'].apply(_limpiar_nombre_municipio)

    # 3. Preparación de datos estadísticos
    # Filtramos por el indicador de Sueldos y Salarios
    df_renta = integrar_renta_codislas[
        (integrar_renta_codislas["Fuente_Renta_Code"] == "SUELDOS_SALARIOS")
    ].copy()
    
    # SELECCIÓN TEMPORAL: Tomamos solo el año más reciente disponible 
    ultimo_año = df_renta["Año"].max()
    context.log.info(f"Filtrando datos para el año más reciente: {ultimo_año}")
    df_actual = df_renta[df_renta["Año"] == ultimo_año].copy()
    
    # Normalizamos los nombres en el DataFrame de rentas
    df_actual['Territorio_clean'] = df_actual['Territorio'].apply(_limpiar_nombre_municipio)

    # 4. Cruce de datos (Merge)
    gdf_final = gdf.merge(
        df_actual, 
        left_on="municipio_clean", 
        right_on="Territorio_clean", 
        how="left"
    )

    # Auditoría del cruce para el informe de calidad 
    n_total = len(gdf)
    n_con_datos = len(gdf_final.dropna(subset=['Porcentaje']))
    porcentaje_exito = round((n_con_datos / n_total) * 100, 2)
    
    if n_con_datos < n_total:
        faltantes = gdf_final[gdf_final['Porcentaje'].isna()]['municipio_clean'].tolist()
        context.log.warning(f"Municipios sin datos vinculados ({n_total - n_con_datos}): {faltantes}")

    # 5. Visualización con Plotnine (Gramática de Gráficos) 
    mapa = (
        ggplot(gdf_final)
        + geom_map(aes(fill="Porcentaje")) # Mapeo de variable a estética fill 
        + scale_fill_cmap(cmap_name="viridis")
        + theme_void()
        + labs(
            title=f"Distribución de Rentas por Municipio ({ultimo_año})",
            subtitle="Indicador: Sueldos y Salarios · Fuente: ISTAC",
            fill="Renta (%)"
        )
        + theme(
            plot_title=element_text(size=14, fontweight='bold'),
            legend_position='right'
        )
    )

    # 6. Guardar archivo y registrar metadatos para auditoría 
    os.makedirs(DIR_GRAFICOS, exist_ok=True)
    ruta_salida = os.path.join(DIR_GRAFICOS, "mapa_rentas_municipios.png")
    mapa.save(ruta_salida, width=12, height=8, dpi=150)
    
    return Output(
        value=ruta_salida,
        metadata={
            "año_representado": MetadataValue.int(int(ultimo_año)),
            "cobertura_municipios": MetadataValue.text(f"{n_con_datos} de {n_total}"),
            "porcentaje_exito": MetadataValue.float(porcentaje_exito),
            "ruta_png": MetadataValue.path(ruta_salida)
        }
    )

@asset
def commit_mapa_python(
    context: OpExecutionContext,
    mapa_rentas_python: str,
) -> None:
    """
    Automatiza el envío del mapa a GitHub para su visualización en GitHub Pages.
    """
    commit_and_push(
        repo_dir=REPO_DIR,
        remote_url=repo_url(get_github_token()),
        branch=GIT_BRANCH,
        files=[mapa_rentas_python],
        message=f"practica4: actualización automática mapa de rentas ({mapa_rentas_python})", 
        ctx=context,
    )