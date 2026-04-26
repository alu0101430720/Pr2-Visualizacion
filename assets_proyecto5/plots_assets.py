import os
import numpy as np
import yaml
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.cm import ScalarMappable
from plotnine import *
from dagster import asset, AssetExecutionContext, MetadataValue
from assets import preprocesar_datos_p5
import config

def get_plot_config():
    config_path = os.path.join(os.path.dirname(__file__), "plot_config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_processed_path(filename):
    return os.path.join(config.TARGET_DIR, config.DATA_P5_DIR, "processed", filename)

def get_geojson_path(filename):
    return os.path.join(config.TARGET_DIR, config.DATA_P5_DIR, "cartografia-secciones", filename)

def get_plot_dir():
    plot_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR, "plots")
    os.makedirs(plot_dir, exist_ok=True)
    return plot_dir

def fmt_k(l):
    def format_num(v):
        if pd.isna(v): return ""
        if v >= 1000: return f"{v/1000:g}k"
        return f"{v:g}"
    return [format_num(v) for v in l]

def cargar_gdf_municipios(año: int, logger=None) -> gpd.GeoDataFrame | None:
    """
    Carga el GeoJSON del año indicado y disuelve secciones → municipios.
    Extrae el nombre del municipio desde la columna 'etiqueta'.
    Devuelve None si el fichero no existe.
    """
    geojson_name = f"secciones_{año}0101_tenerife.json"
    path = get_geojson_path(geojson_name)
    if not os.path.exists(path):
        if logger:
            logger.warning(f"GeoJSON no encontrado: {path}")
        return None
    gdf = gpd.read_file(path).set_crs("EPSG:4326", allow_override=True)
    gdf["municipio"] = gdf["etiqueta"].str.extract(r"- (.+)$")
    return gdf.dissolve(by="municipio", as_index=False)[["municipio", "geometry"]]

@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_actividad_barras(context: AssetExecutionContext) -> None:
    from checks_p5 import inferir_isla
    cfg = get_plot_config()["actividad_barras"]
    ISLA = cfg.get("isla", "Todas")
    
    df = pd.read_csv(get_processed_path(cfg["dataset"])).dropna(subset=["num_casos"])
    df = df[df["Sexo"].isin(["Hombres", "Mujeres"]) & (df["Actividad económica"] != "No consta")]
    
    if ISLA != "Todas":
        df["isla"] = df["municipio"].apply(inferir_isla)
        df = df[df["isla"] == ISLA]

    df["actividad"] = df["Actividad económica"].replace(
        {"Agricultura, ganadería y pesca": "Agricultura/\nGanadería"}
    )

    agg = df.groupby(["Periodo", "actividad", "Sexo"], as_index=False)["num_casos"].sum()

    p = (
        ggplot(agg, aes(x="factor(Periodo)", y="num_casos", fill="Sexo"))
        + geom_col(position="stack", width=0.7, alpha=0.9)
        + facet_wrap("~ actividad", scales="free_y", ncol=2)
        + scale_fill_manual(values={"Hombres": "#4A90D9", "Mujeres": "#D94A8C"})
        + scale_y_continuous(labels=fmt_k)
        + labs(
            title=f"Actividad económica por año y sexo — {ISLA if ISLA != 'Todas' else 'Toda la provincia'}",
            subtitle="Suma de trabajadores por sección censal",
            x=None, y="Nº trabajadores", fill="Sexo",
            caption="Fuente: ISTAC",
        )
        + theme_minimal()
        + theme(
            figure_size=(14, 8),
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            strip_text=element_text(size=9, face="bold"),
            panel_grid_major_y=element_line(color="#dddddd", size=0.5), panel_grid_major_x=element_blank(),
            legend_position="bottom",
        )
    )

    out_path = os.path.join(get_plot_dir(), "actividad_barras.png")
    p.save(out_path, width=14, height=8, dpi=150, verbose=False)
    context.add_output_metadata({"plot": MetadataValue.md(f"![Actividad Barras]({out_path})")})

@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_ocupacion_divergente(context: AssetExecutionContext) -> None:
    cfg = get_plot_config()["ocupacion_divergente"]
    df = pd.read_csv(get_processed_path(cfg["dataset"])).dropna(subset=["num_casos"])
    df = df[df["sexo"].isin(["Hombres", "Mujeres"]) & (df["ocupacion"] != "No consta")]

    agg = df.groupby(["ocupacion", "sexo"], as_index=False)["num_casos"].sum()
    pivot = agg.pivot(index="ocupacion", columns="sexo", values="num_casos").reset_index()
    pivot["brecha"] = pivot["Hombres"] - pivot["Mujeres"]
    pivot["direccion"] = pivot["brecha"].apply(lambda x: "Mayoría Hombres" if x > 0 else "Mayoría Mujeres")
    pivot["ocupacion_wrap"] = pivot["ocupacion"].apply(lambda s: "\n".join([s[i:i+40] for i in range(0, len(s), 40)]))

    p = (
        ggplot(pivot, aes(x="reorder(ocupacion_wrap, brecha)", y="brecha", fill="direccion"))
        + geom_col(width=0.6, alpha=0.9)
        + geom_hline(yintercept=0, linetype="dashed", color="#333333", size=0.5)
        + scale_fill_manual(values={"Mayoría Hombres": "#4A90D9", "Mayoría Mujeres": "#D94A8C"})
        + scale_y_continuous(labels=fmt_k)
        + coord_flip()
        + labs(
            title="Brecha de género por ocupación — Tenerife",
            subtitle="Diferencia acumulada (Hombres − Mujeres)",
            x=None, y=None, fill=None,
            caption="Fuente: ISTAC",
        )
        + theme_minimal()
        + theme(
            figure_size=(12, 6),
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            panel_grid_major_y=element_blank(),
            panel_grid_major_x=element_line(color="#dddddd", size=0.4),
            legend_position="bottom",
        )
    )

    out_path = os.path.join(get_plot_dir(), "ocupacion_divergente.png")
    p.save(out_path, width=12, height=6, dpi=150, verbose=False)
    context.add_output_metadata({"plot": MetadataValue.md(f"![Ocupacion Divergente]({out_path})")})

@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_mapa_distribucion_renta(context: AssetExecutionContext) -> None:
    cfg = get_plot_config()["mapa_distribucion"]
    año = cfg["ano"]
    componente = cfg["componente"]
    geojson_name = f"secciones_{año}0101_tenerife.json"

    LABELS = {
        "OTRAS_PRESTACIONES":     "Otras prestaciones (%)",
        "OTROS_INGRESOS":         "Otros ingresos (%)",
        "PENSIONES":              "Pensiones (%)",
        "PRESTACIONES_DESEMPLEO": "Prestaciones desempleo (%)",
        "SUELDOS_SALARIOS":       "Sueldos y salarios (%)",
    }
    
    CMAPS = {
        "SUELDOS_SALARIOS":       "Blues",
        "PENSIONES":              "Oranges",
        "PRESTACIONES_DESEMPLEO": "Purples",
        "OTRAS_PRESTACIONES":     "Greens",
        "OTROS_INGRESOS":         "YlOrBr",
    }

    df = pd.read_csv(get_processed_path(cfg["dataset"])).dropna(subset=["OBS_VALUE"])
    df_fil = (
        df[(df["año"] == año) & (df["MEDIDAS_CODE"] == componente)]
        .groupby("municipio", as_index=False)["OBS_VALUE"]
        .median()
    )

    gdf_mun = cargar_gdf_municipios(año, context.log)
    if gdf_mun is None:
        context.log.warning(f"GeoJSON no disponible para {año}. Asset omitido.")
        return

    gdf = gdf_mun.merge(df_fil, on="municipio", how="left")

    fig, ax = plt.subplots(figsize=(14, 8))

    gdf.plot(
        column="OBS_VALUE",
        cmap=CMAPS.get(componente, "YlOrRd"),
        linewidth=0.08,
        edgecolor="white",
        legend=True,
        legend_kwds={"label": LABELS.get(componente, componente), "orientation": "vertical", "shrink": 0.55, "pad": 0.01},
        missing_kwds={"color": "#dddddd", "label": "Sin datos"},
        ax=ax,
    )

    ax.set_title(f"{LABELS.get(componente, componente).replace(' (%)', '')} sobre renta total — Tenerife {año}", fontsize=14, fontweight="bold", pad=12)
    ax.annotate("Por municipios · Fuente: ISTAC", xy=(0.01, 0.98), xycoords="axes fraction", fontsize=9, color="#555555", va="top")
    ax.axis("off")
    fig.tight_layout()

    out_path = os.path.join(get_plot_dir(), f"mapa_{componente.lower()}_{año}.png")
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    
    context.add_output_metadata({"plot": MetadataValue.md(f"![Mapa Distribucion]({out_path})")})

@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_brecha_salarial(context: AssetExecutionContext) -> None:
    cfg = get_plot_config()["brecha_salarial"]

    TOP_N   = cfg.get("top_n", 20)
    AÑO_INI = cfg.get("ano_ini", 2021)
    AÑO_FIN = cfg.get("ano_fin", 2023)
    UMBRAL  = cfg.get("umbral", 0.02)

    ocu  = pd.read_csv(get_processed_path(cfg["dataset_ocu"])).dropna(subset=["num_casos"])
    dist = pd.read_csv(get_processed_path(cfg["dataset_dist"])).dropna(subset=["OBS_VALUE"])

    ocu_hm = (
        ocu[ocu["sexo"].isin(["Hombres", "Mujeres"]) & (ocu["ocupacion"] != "No consta")]
        .groupby(["municipio", "año", "sexo"], as_index=False)["num_casos"]
        .sum()
        .pivot(index=["municipio", "año"], columns="sexo", values="num_casos")
        .reset_index()
    )
    ocu_hm.columns.name = None
    ocu_hm["ratio_hm"] = ocu_hm["Hombres"] / (ocu_hm["Hombres"] + ocu_hm["Mujeres"])

    sal = (
        dist[dist["MEDIDAS_CODE"] == "SUELDOS_SALARIOS"]
        .groupby(["municipio", "año"], as_index=False)["OBS_VALUE"]
        .median()
        .rename(columns={"OBS_VALUE": "pct_salarios"})
    )

    merged = ocu_hm.merge(sal, on=["municipio", "año"], how="inner")
    merged["indice_brecha"] = (merged["ratio_hm"] - 0.5) * merged["pct_salarios"]

    ini   = merged[merged["año"] == AÑO_INI][["municipio", "indice_brecha"]].rename(columns={"indice_brecha": "brecha_ini"})
    fin   = merged[merged["año"] == AÑO_FIN][["municipio", "indice_brecha"]].rename(columns={"indice_brecha": "brecha_fin"})
    slope = ini.merge(fin, on="municipio")
    slope["delta"]     = slope["brecha_fin"] - slope["brecha_ini"]
    slope["direccion"] = slope["delta"].apply(
        lambda d: "Brecha aumenta" if d > UMBRAL
        else ("Brecha disminuye" if d < -UMBRAL else "Sin cambio relevante")
    )
    slope["brecha_media"] = (slope["brecha_ini"] + slope["brecha_fin"]) / 2
    top = slope.nlargest(TOP_N, "brecha_media")

    long = pd.concat([
        top.assign(año=AÑO_INI, brecha=top["brecha_ini"]),
        top.assign(año=AÑO_FIN, brecha=top["brecha_fin"]),
    ])
    long["año_cat"] = pd.Categorical(long["año"], categories=[AÑO_INI, AÑO_FIN], ordered=True)

    mediana_global = float(long["brecha"].median())

    # ── Paleta coherente con el proyecto ─────────────────────────────────────
    # e63946 = precariedad/alarma (mismo rojo que Temp. Parcial)
    # 2a9d8f = mejora/estabilidad (mismo verde que Indefinido)
    # AAAAAA = neutro (sin cambio)
    COLORES = {
        "Brecha aumenta":       "#e63946",
        "Brecha disminuye":     "#2a9d8f",
        "Sin cambio relevante": "#AAAAAA",
    }

    long_ini = long[long["año"] == AÑO_INI]
    long_fin = long[long["año"] == AÑO_FIN]

    p = (
        ggplot(long, aes(x="año_cat", y="brecha", group="municipio", color="direccion"))
        + geom_hline(yintercept=mediana_global, linetype="dashed",
                     color="#888888", size=0.5, alpha=0.7)
        + geom_line(size=0.9, alpha=0.8)
        + geom_point(size=2.5, stroke=0.3)
        + geom_text(
            aes(label="municipio"),
            data=long_ini,
            ha="right", nudge_x=-0.05, size=9,
        )
        + geom_text(
            aes(label="municipio"),
            data=long_fin,
            ha="left", nudge_x=0.05, size=9,
        )
        + scale_color_manual(values=COLORES, name=None)
        # expand simétrico: margen igual izquierda y derecha para los nombres
        + scale_x_discrete(expand=(0.45, 0.45))
        + labs(
            title="Evolución de la brecha salarial de género por municipio",
            subtitle=f"Índice = ratio H/(H+M) × % sueldos sobre renta · Top {TOP_N} municipios · {AÑO_INI}→{AÑO_FIN}",
            x=None, y="Índice de brecha salarial ponderado",
            caption="Fuente: ISTAC · ocupacion-sc-3 + distribucion-renta-ingresos",
        )
        + theme_minimal()
        + theme(
            figure_size=(10, 6),
            plot_title=element_text(size=14, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            panel_grid=element_blank(),
            axis_text_x=element_text(size=11, face="bold"),
            axis_text_y=element_text(size=8, color="#888888"),
            legend_position="bottom",
        )
    )

    out_path = os.path.join(get_plot_dir(), "brecha_salarial_slope.png")
    p.save(out_path, width=11, height=9, dpi=150, verbose=False)
    context.add_output_metadata(
        {"plot": MetadataValue.md(f"![Brecha Salarial Slope]({out_path})")}
    )

@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_mapa_brecha_salarial(context: AssetExecutionContext) -> None:
    cfg = get_plot_config()["brecha_salarial"]
    
    AÑO_INI = cfg.get("ano_ini", 2021)
    AÑO_FIN = cfg.get("ano_fin", 2023)
    
    ocu  = pd.read_csv(get_processed_path(cfg["dataset_ocu"])).dropna(subset=["num_casos"])
    dist = pd.read_csv(get_processed_path(cfg["dataset_dist"])).dropna(subset=["OBS_VALUE"])

    ocu_hm = (
        ocu[ocu["sexo"].isin(["Hombres", "Mujeres"]) & (ocu["ocupacion"] != "No consta")]
        .groupby(["municipio", "año", "sexo"], as_index=False)["num_casos"].sum()
        .pivot(index=["municipio", "año"], columns="sexo", values="num_casos")
        .reset_index()
    )
    ocu_hm.columns.name = None
    ocu_hm["ratio_hm"] = ocu_hm["Hombres"] / (ocu_hm["Hombres"] + ocu_hm["Mujeres"])

    sal = (
        dist[dist["MEDIDAS_CODE"] == "SUELDOS_SALARIOS"]
        .groupby(["municipio", "año"], as_index=False)["OBS_VALUE"].median()
        .rename(columns={"OBS_VALUE": "pct_salarios"})
    )

    merged = ocu_hm.merge(sal, on=["municipio", "año"], how="inner")
    merged["indice_brecha"] = (merged["ratio_hm"] - 0.5) * merged["pct_salarios"]

    def cargar_municipios(año):
        geojson_name = f"secciones_{año}0101_tenerife.json"
        try:
            gdf = gpd.read_file(get_geojson_path(geojson_name)).set_crs("EPSG:4326", allow_override=True)
            gdf["municipio"] = gdf["etiqueta"].str.extract(r"- (.+)$")
            gdf_mun = gdf.dissolve(by="municipio", as_index=False)[["municipio", "geometry"]]
            return gdf_mun
        except Exception:
            return None

    AÑO_MAPA = cfg.get("ano_mapa", 2023)
    
    lim = max(abs(merged["indice_brecha"].min()), abs(merged["indice_brecha"].max()))
    vmin, vmax = -lim, lim

    fig, ax = plt.subplots(figsize=(12, 8))
    año = AÑO_MAPA

    gdf_mun = cargar_municipios(año)
    if gdf_mun is None:
        ax.set_title(f"{año} - Sin Datos Espaciales")
        ax.axis("off")
    else:
        datos_año = merged[merged["año"] == año][["municipio", "indice_brecha"]]
        gdf_plot = gdf_mun.merge(datos_año, on="municipio", how="left")

        gdf_plot.plot(
            column="indice_brecha",
            cmap="RdBu_r", vmin=vmin, vmax=vmax,
            linewidth=0.15, edgecolor="white",
            missing_kwds={"color": "#dddddd", "label": "Sin datos"},
            legend=False, ax=ax,
        )

        ax.axis("off")

    norm = mcolors.TwoSlopeNorm(vmin=vmin, vcenter=0, vmax=vmax)
    sm   = ScalarMappable(cmap="RdBu_r", norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, orientation="vertical", shrink=0.55, pad=0.02)
    cbar.set_label("Índice de brecha salarial\n(+ = favorable a hombres  /  − = favorable a mujeres)", fontsize=10)

    fig.suptitle(f"Brecha salarial de género por municipio — Tenerife {AÑO_MAPA}", fontsize=15, fontweight="bold", y=0.95)
    fig.text(0.5, 0.08, "Índice = ratio H/(H+M) × % sueldos sobre renta · Fuente: ISTAC", ha="center", fontsize=9, color="#666666")

    fig.tight_layout()
    out_path = os.path.join(get_plot_dir(), "mapa_brecha_salarial.png")
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    
    context.add_output_metadata({"plot": MetadataValue.md(f"![Mapa Brecha Salarial]({out_path})")})

# ── Constantes y helpers compartidos ─────────────────────────────────────────
ISLAS_ORDEN = [
    "Canarias", "Tenerife", "Gran Canaria", "La Palma",
    "La Gomera", "El Hierro", "Lanzarote", "Fuerteventura",
]

def _load_gini() -> pd.DataFrame:
    return pd.read_csv(get_processed_path("gini.csv")).dropna(subset=["OBS_VALUE"])

def _load_rentas() -> pd.DataFrame:
    return pd.read_csv(get_processed_path("rentas.csv")).dropna(subset=["OBS_VALUE"])


# ══════════════════════════════════════════════════════════════════════════════
# G8 — Líneas: evolución del Índice de Gini por isla (2015-2023)
# ══════════════════════════════════════════════════════════════════════════════
@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_gini_evolucion_islas(context: AssetExecutionContext) -> None:
    """
    IDONEIDAD: 9 años × 8 territorios agregados → la línea es la codificación
    natural del tiempo. Responde "¿en qué isla baja más la desigualdad y a qué
    ritmo?". Un bar chart no permitiría ver la tendencia; un scatter perdería
    la conexión temporal.

    GESTALT:
      Continuidad — la línea implica tendencia entre años consecutivos.
      Similitud   — color constante por isla a lo largo de los 9 años.
      Figura/Fondo — Canarias en gris discontinuo actúa como fondo de
                     referencia; las islas son la figura.

    DISEÑO:
      Canarias como referencia gris (no compite con las islas).
      Anotación vertical en 2020 (COVID) como ancla narrativa clave.
      Eje X con todos los años, grid solo horizontal, escala Y libre.
      Paleta Set2 (9 colores max — 7 islas encajan sin superar el límite).
    """
    gini = _load_gini()
    df   = gini[(gini["MEDIDAS"] == "Índice de Gini") &
                (gini["tipo_territorio"] == "isla")].copy()
    df["TERRITORIO"] = pd.Categorical(
        df["TERRITORIO"], categories=[i for i in ISLAS_ORDEN if i != "Canarias"], ordered=True
    )

    canarias  = df[df["TERRITORIO"] == "Canarias"]
    islas_sin = df[df["TERRITORIO"] != "Canarias"]

    islas_sin["is_tourist"] = islas_sin["TERRITORIO"].isin(["Tenerife", "Gran Canaria", "Lanzarote", "Fuerteventura"])
    islas_sin["is_tourist"] = pd.Categorical(islas_sin["is_tourist"], categories=[True, False], ordered=True)

    p = (
        ggplot(islas_sin,
               aes(x="TIME_PERIOD", y="OBS_VALUE",
                   color="TERRITORIO", group="TERRITORIO", 
                   alpha="is_tourist", size="is_tourist"))
        + geom_vline(xintercept=2020, linetype="dotted", color="#AAAAAA", size=0.6)
        + annotate("text", x=2020.2, y=df["OBS_VALUE"].max() - 0.3,
                   label="2020\nCOVID", size=7, color="#999999", ha="left")
        + geom_line()
        + geom_point(stroke=0.3)
        + scale_x_continuous(breaks=list(range(2015, 2024)))
        + scale_color_manual(
            values={
                "Tenerife":       "#e07b39",
                "Lanzarote":      "#e9c46a",
                "Fuerteventura":  "#f4a261",
                "Gran Canaria":   "#a8c5da",
                "La Palma":       "#b5c8b8",
                "La Gomera":      "#c9b8d0",
                "El Hierro":      "#d4c5b0",
            },
            name="Isla"
        )
        + scale_alpha_manual(values={True: 1.0, False: 0.3}, guide=None)
        + scale_size_manual(values={True: 2.0, False: 0.8}, guide=None)
        + labs(
            title="Evolución del Índice de Gini por isla — Canarias 2015-2023",
            subtitle="Islas turísticas destacadas (Tenerife, Lanzarote, Fuerteventura) · Valores altos = mayor desigualdad",
            x=None, y="Índice de Gini",
            caption="Fuente: ISTAC",
        )
        + theme_minimal()
        + theme(
            figure_size=(13, 6),
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            axis_text_x=element_text(size=9),
            panel_grid_minor=element_blank(),
            panel_grid_major_y=element_line(color="#dddddd", size=0.5), panel_grid_major_x=element_blank(),
            legend_position="right",
        )
    )
    out = os.path.join(get_plot_dir(), "gini_evolucion_islas.png")
    p.save(out, width=13, height=6, dpi=150, verbose=False)
    context.add_output_metadata(
        {"plot": MetadataValue.md(f"![Gini Islas]({out})")}
    )


# ══════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
# G9 — Boxplot: Distribución de la brecha salarial por isla (Toda Canarias)
# ══════════════════════════════════════════════════════════════════════════════
@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_brecha_salarial_islas(context: AssetExecutionContext) -> None:
    from checks_p5 import inferir_isla
    cfg = get_plot_config()["brecha_salarial"]
    AÑO_MAPA = cfg.get("ano_mapa", 2023)
    
    # 1. Cargar Contratos (Proxy de Oportunidad Laboral)
    ocu = pd.read_csv(get_processed_path("contratos_202603.csv")).dropna(subset=["Contratos"])
    ocu = ocu.rename(columns={"Municipio": "municipio"})
    
    ocu_hm = (
        ocu[ocu["sexo"].isin(["Hombres", "Mujeres"])]
        .groupby(["municipio", "sexo"], as_index=False)["Contratos"].sum()
        .pivot(index="municipio", columns="sexo", values="Contratos")
        .reset_index()
        .fillna(0)
    )
    ocu_hm.columns.name = None
    ocu_hm["ratio_hm"] = ocu_hm["Hombres"] / (ocu_hm["Hombres"] + ocu_hm["Mujeres"] + 1e-9)

    # 2. Cargar Dependencia Salarial
    dist = pd.read_csv(get_processed_path(cfg["dataset_dist"])).dropna(subset=["OBS_VALUE"])
    sal = (
        dist[(dist["MEDIDAS_CODE"] == "SUELDOS_SALARIOS") & (dist["año"] == AÑO_MAPA)]
        .groupby("municipio", as_index=False)["OBS_VALUE"].median()
        .rename(columns={"OBS_VALUE": "pct_salarios"})
    )

    # 3. Merge y cálculo de brecha
    merged = ocu_hm.merge(sal, on="municipio", how="inner")
    merged["indice_brecha"] = (merged["ratio_hm"] - 0.5) * merged["pct_salarios"]
    
    df = merged.copy()
    df["isla"] = df["municipio"].apply(inferir_isla)
    df = df[df["isla"] != "Desconocida"]
    
    orden = df.groupby("isla")["indice_brecha"].median().sort_values().index.tolist()
    df["isla"] = pd.Categorical(df["isla"], categories=orden, ordered=True)

    p = (
        ggplot(df, aes(x="isla", y="indice_brecha", fill="isla"))
        + geom_hline(yintercept=0, linetype="dashed", color="#555555", size=0.8)
        + geom_boxplot(alpha=0.6, outlier_alpha=0, width=0.5, color="#333333")
        + geom_jitter(width=0.15, size=2, alpha=0.8, color="#222222")
        + scale_fill_brewer(type="qual", palette="Set2", guide=None)
        + coord_flip()
        + labs(
            title="Distribución de la brecha de género por isla — Toda Canarias",
            subtitle="Índice > 0: Contratación favorable a hombres · Índice < 0: Favorable a mujeres\nPuntos = Municipios · Proxy: Contratos marzo 2026 × Dependencia Salarial 2023",
            x=None, y="Índice de brecha salarial/laboral ponderado",
            caption="Fuente: SEPE / OBECAN · Contratos marzo 2026",
        )
        + theme_minimal()
        + theme(
            figure_size=(12, 6),
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=9, color="#555555"),
            axis_text_y=element_text(size=11, face="bold"),
            panel_grid_minor=element_blank(),
            panel_grid_major_y=element_blank(),
            panel_grid_major_x=element_line(color="#dddddd", size=0.4),
        )
    )
    out = os.path.join(get_plot_dir(), "brecha_salarial_islas.png")
    p.save(out, width=10, height=6, dpi=150, verbose=False)
    context.add_output_metadata(
        {"plot": MetadataValue.md(f"![Brecha Islas]({out})")}
    )



@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_heatmap_segregacion_sectorial(context: AssetExecutionContext) -> None:
    """
    IDONEIDAD: dos variables categóricas (actividad × isla) con una cuantitativa
    continua en la celda (ratio H/M). El heatmap es más eficiente en espacio
    que 84 barras agrupadas y permite detectar patrones de segregación
    consistentes entre islas de un solo vistazo.

    GESTALT:
      Similitud   — gradiente RdBu_r centrado en 0.5 (paridad): rojo = mayoría
                    hombres, azul = mayoría mujeres, blanco = equilibrio.
      Proximidad  — actividades ordenadas de más feminizadas (arriba) a más
                    masculinizadas (abajo): los clusters emergen sin intervención.
      Continuidad — lectura izq→der = islas occidentales → orientales, permite
                    detectar si la segregación es local o estructural.
      Cierre      — bordes blancos entre celdas definen cada unidad sin sobrecargar.

    DISEÑO:
      TwoSlopeNorm centrada en 0.5 (paridad real, no media del dataset).
      Anotación %H / %M dentro de cada celda: elimina la necesidad de leer
      la barra de color para valores concretos.
      Color de texto adaptativo: blanco en celdas extremas, gris en celdas
      próximas a la paridad (ratio tinta/legibilidad óptimo).
      Sin ejes de título (las etiquetas de fila/columna son autoexplicativas).
      Islas ordenadas oeste→este para coherencia geográfica con los mapas.
    """
    import numpy as np
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    from matplotlib.cm import ScalarMappable

    # ── Carga y limpieza ──────────────────────────────────────────────────────
    df = pd.read_csv(get_processed_path("contratos_202603.csv"))
    df.columns = df.columns.str.strip()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # ── Top 12 actividades por volumen total ──────────────────────────────────
    top_act = (
        df.groupby("Actividad económica")["Contratos"]
        .sum().nlargest(12).index.tolist()
    )

    ABREV = {
        "Producción cinematográfica, de vídeo y de programas de televisión, grabación de sonido y edición musical": "Producción audiovisual",
        "Servicios a edificios y actividades de jardinería": "Servicios a edificios",
        "Actividades de construcción especializada":         "Construcción especializada",
        "Administración pública y defensa; seguridad social obligatoria": "Administración pública",
        "Actividades de creación artística y artes escénicas": "Artes escénicas",
        "Actividades sanitarias":      "Actividades sanitarias",
        "Servicios de alojamiento":    "Alojamiento",
        "Servicios de comidas y bebidas": "Hostelería",
        "Comercio al por menor":       "Comercio minorista",
        "Comercio al por mayor":       "Comercio mayorista",
        "Educación":                   "Educación",
        "Construcción de edificios":   "Construcción de edificios",
    }

    # ── Ratio H/(H+M) por actividad e isla ───────────────────────────────────
    pivot = (
        df[df["Actividad económica"].isin(top_act)]
        .groupby(["Actividad económica", "isla", "sexo"])["Contratos"]
        .sum().unstack("sexo").reset_index()
    )
    pivot.columns.name = None
    pivot["ratio_hm"]        = pivot["Hombres"] / (pivot["Hombres"] + pivot["Mujeres"])
    pivot["actividad_short"] = pivot["Actividad económica"].map(ABREV)

    # ── Orden islas oeste→este ────────────────────────────────────────────────
    ISLAS_ORDEN = ["EL HIERRO", "LA GOMERA", "LA PALMA", "TENERIFE",
                   "GRAN CANARIA", "LANZAROTE", "FUERTEVENTURA"]
    ISLAS_LABEL = ["El Hierro", "La Gomera", "La Palma", "Tenerife",
                   "Gran Canaria", "Lanzarote", "Fuerteventura"]

    # ── Orden actividades: más feminizadas arriba ─────────────────────────────
    orden_act = (
        pivot.groupby("actividad_short")["ratio_hm"]
        .mean().sort_values(ascending=True).index.tolist()
    )

    heat = (
        pivot.pivot(index="actividad_short", columns="isla", values="ratio_hm")
        .reindex(index=orden_act, columns=ISLAS_ORDEN)
    )

    # ── Plot ──────────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(13, 7))

    norm = mcolors.TwoSlopeNorm(vmin=0.0, vcenter=0.5, vmax=1.0)
    cmap = plt.cm.RdBu_r
    ax.imshow(heat.values, cmap=cmap, norm=norm, aspect="auto")

    # Ejes
    ax.set_xticks(range(len(ISLAS_ORDEN)))
    ax.set_xticklabels(ISLAS_LABEL, fontsize=11, fontweight="bold")
    ax.set_yticks(range(len(orden_act)))
    ax.set_yticklabels(orden_act, fontsize=10)
    ax.tick_params(left=False, bottom=False)
    ax.set_xlabel(None)
    ax.set_ylabel(None)

    # Anotaciones %H / %M en cada celda
    for i in range(len(orden_act)):
        for j in range(len(ISLAS_ORDEN)):
            val = heat.values[i, j]
            if not np.isnan(val):
                pct_h = int(round(val * 100))
                pct_m = 100 - pct_h
                color_txt = "white" if abs(val - 0.5) > 0.25 else "#333333"
                ax.text(j, i, f"{pct_h}H\n{pct_m}M",
                        ha="center", va="center",
                        fontsize=7.5, fontweight="bold", color=color_txt)

    # Bordes blancos entre celdas
    for i in range(len(orden_act) + 1):
        ax.axhline(i - 0.5, color="white", lw=1.5)
    for j in range(len(ISLAS_ORDEN) + 1):
        ax.axvline(j - 0.5, color="white", lw=1.5)

    ax.set_xlim(-0.5, len(ISLAS_ORDEN) - 0.5)
    ax.set_ylim(-0.5, len(orden_act) - 0.5)

    # Colorbar
    cb = fig.colorbar(
        ScalarMappable(norm=norm, cmap=cmap),
        ax=ax, orientation="vertical", shrink=0.7, pad=0.02,
    )
    cb.set_label("% Hombres contratados", fontsize=9)
    cb.set_ticks([0, 0.25, 0.5, 0.75, 1.0])
    cb.set_ticklabels([
        "0%\n(todo mujeres)", "25%", "50%\n(paridad)", "75%", "100%\n(todo hombres)"
    ])

    # Títulos
    ax.set_title(
        "Segregación de género por sector e isla — Canarias, Marzo 2026",
        fontsize=14, fontweight="bold", pad=14,
    )
    fig.text(
        0.01, -0.02,
        "Azul = mayoría mujeres · Rojo = mayoría hombres · Blanco = paridad  ·  Fuente: SEPE / OBECAN · Contratos marzo 2026",
        fontsize=8, color="#666666",
    )

    plt.tight_layout()
    out = os.path.join(get_plot_dir(), "heatmap_segregacion_sectorial.png")
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    context.add_output_metadata(
        {"plot": MetadataValue.md(f"![Heatmap Segregación]({out})")}
    )


@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_covid_sueldos_islas(context: AssetExecutionContext) -> None:
    """
    IDONEIDAD: variable continua (% sueldos) × tiempo (9 años) × 7 territorios.
    La línea es la codificación natural del tiempo; el área COVID como fondo
    contextualiza el shock sin competir con los datos.
    GESTALT: Figura/Fondo — islas turísticas en color vivo sobre gris neutro.
    Similitud — color constante por isla turística. Continuidad — tendencia temporal.
    DISEÑO: islas resto en gris fino (fondo), turísticas en color grueso (figura).
    Etiquetado directo al final de cada línea elimina la leyenda.
    """
    _plot_covid_lineas(
        context=context,
        medida="Sueldos y salarios",
        ylabel="% renta procedente de sueldos y salarios",
        ylim=(53, 73),
        titulo="Sueldos y salarios sobre renta total por isla — Canarias 2015-2023",
        fname="covid_sueldos_islas.png",
    )


@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_covid_prestaciones_islas(context: AssetExecutionContext) -> None:
    """
    IDONEIDAD: misma estructura que sueldos pero con prestaciones por desempleo.
    El pico de 2020 en islas turísticas (×4 respecto a 2019) es el complemento
    directo de la caída de sueldos: son las dos caras del mismo shock.
    GESTALT: igual que plot_covid_sueldos_islas. La paleta de colores es
    idéntica para facilitar la lectura comparada entre ambos gráficos.
    DISEÑO: escala Y distinta (1.5-17%) para maximizar la legibilidad del pico.
    """
    _plot_covid_lineas(
        context=context,
        medida="Prestaciones por desempleo",
        ylabel="% renta procedente de prestaciones por desempleo",
        ylim=(1.5, 17),
        titulo="Prestaciones por desempleo sobre renta total por isla — Canarias 2015-2023",
        fname="covid_prestaciones_islas.png",
    )


def _plot_covid_lineas(context, medida, ylabel, ylim, titulo, fname):
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches

    rentas = pd.read_csv(get_processed_path("rentas.csv")).dropna(subset=["OBS_VALUE"])

    ISLAS_TURISTICAS = {"Lanzarote", "Fuerteventura", "Tenerife"}
    ISLAS_RESTO      = {"Gran Canaria", "La Palma", "La Gomera", "El Hierro"}
    
    # Islas turísticas: paleta del proyecto. Resto: colores individuales pero tenues.
    COLORES = {
        "Lanzarote":     "#e9c46a",
        "Fuerteventura": "#f4a261",
        "Tenerife":      "#e07b39",
        "Gran Canaria":  "#7fa8c9",   # azul desaturado
        "La Palma":      "#7db89a",   # verde salvia
        "La Gomera":     "#a890b8",   # lila desaturado
        "El Hierro":     "#c8a87a",   # ocre tenue
    }

    fig, ax = plt.subplots(figsize=(13, 6))
    fig.patch.set_facecolor("white")

    ax.axvspan(2019.5, 2021.5, color="#fde8e8", alpha=0.45, zorder=0)
    ax.axvline(2020, color="#c0392b", lw=0.8, ls="--", alpha=0.5, zorder=1)
    ax.set_ylim(ylim)
    ax.text(2020.1, ylim[1] * 0.98,
            "COVID-19", fontsize=8.5, color="#c0392b",
            fontweight="bold", va="top")

    # Dibujar todas las islas, destacando las turísticas
    handles = []
    
    # Primero el resto (fondo): líneas tenues, mismo gris del proyecto
    for isla in sorted(ISLAS_RESTO):
        sub = rentas[(rentas["TERRITORIO"] == isla) &
                     (rentas["MEDIDAS"] == medida)].sort_values("TIME_PERIOD")
        col = COLORES[isla]  # siempre #b0bec5
        ax.plot(sub["TIME_PERIOD"], sub["OBS_VALUE"],
                color=col, lw=1.2, marker="o", markersize=2.5,
                alpha=0.4, zorder=2)
        handles.append(mpatches.Patch(color=col, label=isla, alpha=0.45))

    # Luego turísticas (figura)
    for isla in ISLAS_TURISTICAS:
        sub = rentas[(rentas["TERRITORIO"] == isla) &
                     (rentas["MEDIDAS"] == medida)].sort_values("TIME_PERIOD")
        col = COLORES[isla]
        ax.plot(sub["TIME_PERIOD"], sub["OBS_VALUE"],
                color=col, lw=3.0, marker="o", markersize=6,
                alpha=1.0, zorder=4)
        handles.append(mpatches.Patch(color=col, label=isla, alpha=1.0))

    ax.set_xlim(2014.8, 2023.2)
    ax.set_xticks(range(2015, 2024))
    ax.set_xticklabels(range(2015, 2024), fontsize=8.5)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.yaxis.grid(True, color="#eeeeee", zorder=0)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_xlabel(None)
    ax.set_title(titulo, fontsize=13, fontweight="bold", pad=12)
    
    ax.legend(handles=handles, loc="center left", bbox_to_anchor=(1.02, 0.5), 
              fontsize=9, frameon=False, title="Islas")

    fig.text(0.99, 0.01, "Fuente: ISTAC", ha="right", fontsize=8, color="#888888")

    plt.tight_layout()
    out = os.path.join(get_plot_dir(), fname)
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    context.add_output_metadata({"plot": MetadataValue.md(f"![{titulo}]({out})")})

@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_brecha_temporal_edad(context: AssetExecutionContext) -> None:
    """
    IDONEIDAD: variable nominal (tipo contrato) × nominal (edad) × binario (sexo).
    Barras agrupadas por sexo dentro de cada tipo de contrato, facet por edad.
    Permite comparar tanto entre sexos como entre franjas de edad simultáneamente.
    GESTALT: Proximidad — barras H/M del mismo tipo agrupadas. Similitud — paleta
    azul/rosa constante. Figura/Fondo — barras coloreadas sobre fondo blanco.
    DISEÑO: escala Y compartida (sharey=True) para comparación directa entre
    franjas de edad. Etiquetas solo donde hay masa suficiente (>4%).
    """
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np

    df = pd.read_csv(get_processed_path("contratos_202603.csv"))
    df.columns = df.columns.str.strip()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
    df = df[df["sexo"].isin(["Hombres", "Mujeres"])]
    
    cfg = get_plot_config().get("brecha_temporal_edad", {})
    ISLA = cfg.get("isla", "Todas")
    
    if ISLA != "Todas":
        df = df[df["isla"].str.upper() == ISLA.upper()]

    TC_MAP = {
        "Indefinido":               "Indefinido",
        "Temporal Tiempo Completo": "Temp. Completo",
        "Temporal Tiempo Parcial":  "Temp. Parcial",
        "Conversión a Indefinido":  "Conversión",
    }
    df["tc"] = df["Tipo Contrato"].map(TC_MAP)
    df = df.dropna(subset=["tc"])

    EDAD_ORDER     = ["Menor de 25", "Entre 25 y 44", "45 o más"]
    TC_LABEL_ORDER = ["Temp. Parcial", "Temp. Completo", "Conversión", "Indefinido"]
    COLORS = {"Hombres": "#4A90D9", "Mujeres": "#D94A8C"}

    agg     = df.groupby(["edad", "tc", "sexo"])["Contratos"].sum().reset_index()
    agg     = agg[agg["tc"].isin(TC_LABEL_ORDER)]
    totales = agg.groupby(["edad", "sexo"])["Contratos"].sum().reset_index(name="total")
    agg     = agg.merge(totales, on=["edad", "sexo"])
    agg["pct"] = agg["Contratos"] / agg["total"] * 100

    fig, axes = plt.subplots(1, 3, figsize=(15, 6), sharey=True)
    fig.patch.set_facecolor("white")
    x = np.arange(len(TC_LABEL_ORDER))
    w = 0.35

    for ax, edad in zip(axes, EDAD_ORDER):
        ax.set_facecolor("white")
        for sexo, offset in [("Hombres", -w/2), ("Mujeres", w/2)]:
            vals = [
                float(agg[(agg["edad"]==edad) & (agg["sexo"]==sexo) &
                          (agg["tc"]==tc)]["pct"].values[0])
                if len(agg[(agg["edad"]==edad) & (agg["sexo"]==sexo) &
                           (agg["tc"]==tc)]) > 0 else 0.0
                for tc in TC_LABEL_ORDER
            ]
            ax.bar(x + offset, vals, w, color=COLORS[sexo], alpha=0.85, zorder=3)
            for xi, v in zip(x + offset, vals):
                if v > 4:
                    ax.text(xi, v + 0.4, f"{v:.0f}%",
                            ha="center", va="bottom", fontsize=7.5,
                            color=COLORS[sexo], fontweight="bold")

        ax.set_xticks(x)
        ax.set_xticklabels(TC_LABEL_ORDER, fontsize=9, rotation=20, ha="right")
        ax.set_title(edad, fontsize=11, fontweight="bold", pad=8)
        ax.yaxis.grid(True, color="#eeeeee", zorder=0)
        ax.spines[["top", "right", "left"]].set_visible(False)
        ax.set_ylim(0, 62)
        if ax == axes[0]:
            ax.set_ylabel("% sobre contratos del grupo edad-sexo", fontsize=10)

    titulo_loc = "Canarias" if ISLA == "Todas" else ISLA
    fig.suptitle(
        f"Distribución del tipo de contrato por edad y género — {titulo_loc}, Marzo 2026",
        fontsize=13, fontweight="bold",
    )
    # Leyenda integrada dentro del área del figura
    handles = [mpatches.Patch(color=c, label=s, alpha=0.85) for s, c in COLORS.items()]
    fig.legend(handles=handles, loc="lower center", ncol=2, fontsize=9,
               frameon=False, bbox_to_anchor=(0.5, -0.04))
    fig.text(0.99, -0.07, "Fuente: SEPE / OBECAN · Contratos marzo 2026",
             ha="right", fontsize=8, color="#888888")

    plt.tight_layout(rect=[0, 0.0, 1, 0.96])
    out = os.path.join(get_plot_dir(), "brecha_temporal_parcial_edad.png")
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    context.add_output_metadata({"plot": MetadataValue.md(f"![Brecha Edad]({out})")})

# ==============================================================================
# G12 — Histórico de Tipos de Contrato por Edad
# ==============================================================================
@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_historico_tipos_contrato_por_edad(context: AssetExecutionContext) -> None:
    """
    Genera 4 figuras (una por tipo de contrato) con la evolución histórica
    del % de cada tipo sobre el total de contratos, por franja de edad y género.

    IDONEIDAD: variable continua (%) × temporal (2019-2026) × nominal (edad) ×
    binario (sexo). Las líneas temporales son la codificación natural del tiempo;
    el facet por edad permite comparar grupos sin solapamiento.

    GESTALT:
      Continuidad — líneas como codificación de tendencia temporal.
      Similitud   — azul = hombres, rosa = mujeres, coherente con el proyecto.
      Figura/Fondo — banda gris suave marca el cambio estructural de 2022
                     sin competir con las líneas.
      Proximidad  — los tres paneles de edad permiten comparación inmediata.

    DISEÑO:
      Grid mínimo (solo horizontal). Sin etiquetas de contexto en el gráfico.
      Puntos solo en extremos (2019 y 2025) + etiqueta del valor final.
      2026 como diamante semitransparente (dato parcial — solo marzo).
      Área de brecha rosa solo en Temporal Parcial (única con brecha estructural).
      sharey=True para comparación directa entre edades dentro de cada figura.
    """
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import glob, warnings
    warnings.filterwarnings("ignore")

    def detect_sep(p):
        with open(p, "r", encoding="utf-8", errors="ignore") as f:
            l = f.readline()
        return ";" if l.count(";") > l.count(",") else ","

    # ── Carga histórico completo ───────────────────────────────────────────────
    data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)

    FUENTES = [
        (2019, [os.path.join(data_dir, "contratos2019.csv")],                               "contratos"),
        (2020, [os.path.join(data_dir, "contratos2020.csv")],                               "contratos"),
        (2021, [os.path.join(data_dir, "contratos2021.csv")],                               "contratos"),
        (2022, [os.path.join(data_dir, "contratos2022.csv")],                               "contratos"),
        (2023, sorted(glob.glob(os.path.join(data_dir, "2023", "contratos_registrados_*.csv"))), "Contratos"),
        (2024, sorted(glob.glob(os.path.join(data_dir, "2024", "contratos_registrados_*.csv"))), "Contratos"),
        (2025, sorted(glob.glob(os.path.join(data_dir, "2025", "contratos_202*.csv"))),         "Contratos"),
    ]

    # 2026: solo marzo
    df26 = pd.read_csv(get_processed_path("contratos_202603.csv"),
                       sep=detect_sep(get_processed_path("contratos_202603.csv")),
                       dtype={"Contratos": float})
    df26.columns = df26.columns.str.strip()
    df26 = df26.rename(columns={"Contratos": "c"})
    for col in df26.select_dtypes(include="object").columns:
        df26[col] = df26[col].str.strip()
    df26 = df26[df26["sexo"].isin(["Hombres", "Mujeres"])]
    df26["año"] = 2026

    all_dfs = []
    for año, paths, col_c in FUENTES:
        dfs = []
        for p in paths:
            if not os.path.exists(p):
                continue
            df = pd.read_csv(p, sep=detect_sep(p), dtype={col_c: float})
            df.columns = df.columns.str.strip()
            df = df.rename(columns={col_c: "c"})
            for col in df.select_dtypes(include="object").columns:
                df[col] = df[col].str.strip()
            dfs.append(df[df["sexo"].isin(["Hombres", "Mujeres"])])
        if dfs:
            df_y = pd.concat(dfs, ignore_index=True)
            df_y["año"] = año
            all_dfs.append(df_y)

    all_dfs.append(df26)
    df_hist = pd.concat(all_dfs, ignore_index=True)

    TC_MAP = {
        "Indefinido":               "Indefinido",
        "Temporal Tiempo Completo": "Temp. Completo",
        "Temporal Tiempo Parcial":  "Temp. Parcial",
        "Conversión a Indefinido":  "Conversión",
    }
    EDADES  = ["Menor de 25", "Entre 25 y 44", "45 o más"]
    AÑOS    = [2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    COLORS  = {"Hombres": "#4A90D9", "Mujeres": "#D94A8C"}
    TC_TITLE = {
        "Temp. Parcial":  "Contrato temporal a tiempo parcial",
        "Temp. Completo": "Contrato temporal a tiempo completo",
        "Conversión":     "Conversión a indefinido",
        "Indefinido":     "Contrato indefinido",
    }
    # Solo Temp. Parcial tiene brecha estructural visible → área de relleno
    TC_FILL = {"Temp. Parcial": True, "Temp. Completo": False,
               "Conversión": False, "Indefinido": False}

    df_hist["tc"]   = df_hist["Tipo Contrato"].str.strip().map(TC_MAP)
    df_hist = df_hist.dropna(subset=["tc", "edad"])
    df_hist = df_hist[df_hist["edad"].isin(EDADES)]

    total = (df_hist.groupby(["año", "edad", "sexo"])["c"]
             .sum().reset_index(name="total"))
    agg   = (df_hist.groupby(["año", "edad", "sexo", "tc"])["c"]
             .sum().reset_index())
    agg   = agg.merge(total, on=["año", "edad", "sexo"])
    agg["pct"] = agg["c"] / agg["total"] * 100

    # ── Una figura por tipo de contrato ───────────────────────────────────────
    output_paths = []
    for tc_name in TC_MAP.values():
        fig, axes = plt.subplots(1, len(EDADES), figsize=(14, 5),
                                 sharey=True, sharex=True)
        fig.patch.set_facecolor("white")

        for col, edad in enumerate(EDADES):
            ax = axes[col]
            ax.set_facecolor("white")

            # Banda cambio 2022 sutil
            ax.axvspan(1, 2.5, color="#f5f5f5", alpha=0.8, zorder=0)
            ax.axvline(2.5, color="#dddddd", lw=0.8, zorder=1)

            for sexo in ["Hombres", "Mujeres"]:
                sub  = agg[(agg["sexo"] == sexo) &
                           (agg["edad"] == edad) &
                           (agg["tc"]   == tc_name)].set_index("año")
                vals = [sub.loc[a, "pct"] if a in sub.index else np.nan
                        for a in AÑOS]

                # Línea 2019-2025
                xs = [i for i, a in enumerate(AÑOS)
                      if a <= 2025 and not np.isnan(vals[i])]
                ys = [vals[i] for i in xs]
                ax.plot(xs, ys, color=COLORS[sexo], lw=2.2,
                        alpha=0.9, zorder=4, solid_capstyle="round")

                # Puntos en extremos + etiqueta en 2025
                for i_pt in [0, AÑOS.index(2025)]:
                    if not np.isnan(vals[i_pt]):
                        ax.scatter(i_pt, vals[i_pt], s=55,
                                   color=COLORS[sexo], zorder=5,
                                   edgecolors="white", linewidths=0.8)

                # 2026 — diamante semitransparente
                i26 = AÑOS.index(2026)
                v26 = vals[i26]
                if not np.isnan(v26):
                    ax.scatter(i26, v26, s=45, color=COLORS[sexo],
                               marker="D", zorder=5, alpha=0.6,
                               edgecolors="white", linewidths=0.8)

            # Área brecha solo en Temp. Parcial
            if TC_FILL[tc_name]:
                sh = agg[(agg["sexo"]=="Hombres") & (agg["edad"]==edad) &
                         (agg["tc"]==tc_name)].set_index("año")
                sm = agg[(agg["sexo"]=="Mujeres") & (agg["edad"]==edad) &
                         (agg["tc"]==tc_name)].set_index("año")
                ac = [a for a in AÑOS if a<=2025
                      and a in sh.index and a in sm.index]
                ax.fill_between([AÑOS.index(a) for a in ac],
                                [sh.loc[a,"pct"] for a in ac],
                                [sm.loc[a,"pct"] for a in ac],
                                color="#D94A8C", alpha=0.07, zorder=1)

            ax.set_title(edad, fontsize=11, fontweight="bold",
                         pad=8, color="#333333")
            ax.set_xticks(range(len(AÑOS)))
            ax.set_xticklabels(
                [str(a) if a != 2026 else "Mar\n2026" for a in AÑOS],
                fontsize=8.5, rotation=30, ha="right")

            ax.yaxis.grid(True, color="#eeeeee", lw=0.8, zorder=0)
            ax.spines[["top", "right", "bottom"]].set_visible(False)
            ax.spines["left"].set_color("#eeeeee")
            ax.tick_params(axis="y", labelsize=8.5, colors="#888888")
            ax.tick_params(axis="x", length=0)

            if col == 0:
                ax.set_ylabel("% sobre total contratos del grupo",
                              fontsize=9.5, color="#444444")

        handles = [mpatches.Patch(color=COLORS[s], label=s)
                   for s in ["Hombres", "Mujeres"]]
        handles += [plt.scatter([], [], marker="D", color="#aaaaaa",
                                s=40, alpha=0.6,
                                label="Mar 2026 (dato parcial)")]
        fig.legend(handles=handles, loc="lower center", ncol=3,
                   fontsize=10, frameon=False, bbox_to_anchor=(0.5, -0.04))

        fig.suptitle(f"{TC_TITLE[tc_name]} — Canarias 2019-2026",
                     fontsize=13, fontweight="bold", y=1.01)
        fig.text(0.99, -0.06, "Fuente: OBECAN / SEPE",
                 ha="right", fontsize=8, color="#888888")

        plt.tight_layout(rect=[0, 0.08, 1, 1])

        safe = tc_name.lower().replace(". ", "_").replace(" ", "_")
        out  = os.path.join(get_plot_dir(), f"historico_{safe}.png")
        fig.savefig(out, dpi=150, bbox_inches="tight")
        plt.close(fig)
        output_paths.append(out)
        context.log.info(f"✓ {out}")

    context.add_output_metadata({
        "plots": MetadataValue.md(
            "\n".join(f"- `{os.path.basename(p)}`" for p in output_paths)
        )
    })

