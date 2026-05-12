import os
import glob
import re
import unicodedata
import warnings
import numpy as np
import yaml
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
from matplotlib.cm import ScalarMappable
from plotnine import *
from dagster import asset, AssetExecutionContext, MetadataValue
from assets import preprocesar_datos_p5
import config

warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_plot_config():
    config_path = os.path.join(os.path.dirname(__file__), "plot_config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_paleta() -> dict:
    """
    Lee la sección 'paleta' del YAML.
    Claves:
      H, M        — identidad de género (Uso 1: barras, líneas)
      BH, BM      — dirección de la brecha (Uso 2: mapa, slope)
      cmap_brecha — colormap para mapas coropléticos
    """
    cfg = get_plot_config().get("paleta", {})
    return {
        "H":           cfg.get("color_hombres",                  "#4A90D9"),
        "M":           cfg.get("color_mujeres",                  "#D94A8C"),
        "BH":          cfg.get("color_brecha_favorable_hombres", "#4A90D9"),
        "BM":          cfg.get("color_brecha_favorable_mujeres", "#D94A8C"),
        "cmap_brecha": cfg.get("cmap_brecha",                    "RdBu_r"),
    }


def _aplicar_eje_y(ax, y_min_data: float, y_max_data: float,
                   empezar_en_cero: bool, margen_sup: float = 0.08) -> None:
    """
    Configura el eje Y.
    empezar_en_cero=True  → desde 0, sin marca.
    empezar_en_cero=False → truncado al rango de datos con símbolo //.
    """
    span  = y_max_data - y_min_data
    y_top = y_max_data + span * margen_sup

    if empezar_en_cero:
        ax.set_ylim(0, y_top)
        return

    y_bot = y_min_data - span * 0.05
    ax.set_ylim(y_bot, y_top)

    d  = 0.012
    kw = dict(transform=ax.transAxes, color="#aaaaaa",
              clip_on=False, lw=1.4, zorder=10)
    ax.plot((-d, +d), (-2.2*d,        +2.2*d),        **kw)
    ax.plot((-d, +d), (-2.2*d + 0.02, +2.2*d + 0.02), **kw)
    ax.text(-0.045, -0.005,
            "↑ no empieza en 0",
            transform=ax.transAxes,
            fontsize=6, color="#aaaaaa", va="top", ha="center",
            style="italic", rotation=90)


def get_processed_path(filename):
    return os.path.join(config.TARGET_DIR, config.DATA_P5_DIR, "processed", filename)


def get_geojson_path(filename):
    return os.path.join(config.TARGET_DIR, config.DATA_P5_DIR,
                        "cartografia-secciones", filename)


def get_plot_dir():
    plot_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR, "plots")
    os.makedirs(plot_dir, exist_ok=True)
    return plot_dir


def fmt_k(l):
    """Formatea números como 1k, -5k, etc. Soporta negativos."""
    def _f(v):
        if pd.isna(v):
            return ""
        abs_v = abs(v)
        return f"{v/1000:g}k" if abs_v >= 1000 else f"{v:g}"
    return [_f(v) for v in l]


def cargar_gdf_municipios(año: int, logger=None) -> gpd.GeoDataFrame | None:
    geojson_name = f"secciones_{año}0101_tenerife.json"
    path = get_geojson_path(geojson_name)
    if not os.path.exists(path):
        if logger:
            logger.warning(f"GeoJSON no encontrado: {path}")
        return None
    gdf = gpd.read_file(path).set_crs("EPSG:4326", allow_override=True)
    gdf["municipio"] = gdf["etiqueta"].str.extract(r"- (.+)$")
    return gdf.dissolve(by="municipio", as_index=False)[["municipio", "geometry"]]


# ── Constantes compartidas ────────────────────────────────────────────────────
ISLAS_ORDEN = [
    "Canarias", "Tenerife", "Gran Canaria", "La Palma",
    "La Gomera", "El Hierro", "Lanzarote", "Fuerteventura",
]

COLORES_ISLA = {
    "Tenerife":      "#e07b39",
    "Lanzarote":     "#e9c46a",
    "Fuerteventura": "#f4a261",
    "Gran Canaria":  "#a8c5da",
    "La Palma":      "#b5c8b8",
    "La Gomera":     "#c9b8d0",
    "El Hierro":     "#d4c5b0",
}

COLOR_RESTO_ISLAS = "#b0bec5"   # gris uniforme para islas no destacadas


def _load_gini() -> pd.DataFrame:
    return pd.read_csv(get_processed_path("gini.csv")).dropna(subset=["OBS_VALUE"])


def _load_rentas() -> pd.DataFrame:
    return pd.read_csv(get_processed_path("rentas.csv")).dropna(subset=["OBS_VALUE"])


# ══════════════════════════════════════════════════════════════════════════════
# ASSETS
# ══════════════════════════════════════════════════════════════════════════════

@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_actividad_barras(context: AssetExecutionContext) -> None:
    from checks_p5 import inferir_isla
    cfg  = get_plot_config()["actividad_barras"]
    pal  = get_paleta()
    ISLA = cfg.get("isla", "Todas")

    df = pd.read_csv(get_processed_path(cfg["dataset"])).dropna(subset=["num_casos"])
    df = df[df["Sexo"].isin(["Hombres", "Mujeres"]) &
            (df["Actividad económica"] != "No consta")]

    if ISLA != "Todas":
        df["isla"] = df["municipio"].apply(inferir_isla)
        df = df[df["isla"] == ISLA]

    df["actividad"] = df["Actividad económica"].replace(
        {"Agricultura, ganadería y pesca": "Agricultura/\nGanadería"})

    agg = df.groupby(["Periodo", "actividad", "Sexo"], as_index=False)["num_casos"].sum()

    p = (
        ggplot(agg, aes(x="factor(Periodo)", y="num_casos", fill="Sexo"))
        + geom_col(position="stack", width=0.7, alpha=0.9)
        + facet_wrap("~ actividad", scales="free_y", ncol=2)
        + scale_fill_manual(values={"Hombres": pal["H"], "Mujeres": pal["M"]})
        + scale_y_continuous(labels=fmt_k)
        + labs(
            title=f"Actividad económica por año y sexo — "
                  f"{ISLA if ISLA != 'Todas' else 'Toda la provincia'}",
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
            panel_grid_major_y=element_line(color="#dddddd", size=0.5),
            panel_grid_major_x=element_blank(),
            legend_position="bottom",
        )
    )
    out = os.path.join(get_plot_dir(), "actividad_barras.png")
    p.save(out, width=14, height=8, dpi=150, verbose=False)
    context.add_output_metadata({"plot": MetadataValue.md(f"![Actividad Barras]({out})")})


@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_ocupacion_divergente(context: AssetExecutionContext) -> None:
    cfg = get_plot_config()["ocupacion_divergente"]
    pal = get_paleta()

    df = pd.read_csv(get_processed_path(cfg["dataset"])).dropna(subset=["num_casos"])
    df = df[df["sexo"].isin(["Hombres", "Mujeres"]) & (df["ocupacion"] != "No consta")]

    agg   = df.groupby(["ocupacion", "sexo"], as_index=False)["num_casos"].sum()
    pivot = agg.pivot(index="ocupacion", columns="sexo", values="num_casos").reset_index()
    pivot["brecha"]    = pivot["Hombres"] - pivot["Mujeres"]
    pivot["direccion"] = pivot["brecha"].apply(
        lambda x: "Mayoría Hombres" if x > 0 else "Mayoría Mujeres")
    pivot["ocupacion_wrap"] = pivot["ocupacion"].apply(
        lambda s: "\n".join([s[i:i+40] for i in range(0, len(s), 40)]))

    p = (
        ggplot(pivot, aes(x="reorder(ocupacion_wrap, brecha)",
                          y="brecha", fill="direccion"))
        + geom_col(width=0.6, alpha=0.9)
        + geom_hline(yintercept=0, linetype="dashed", color="#333333", size=0.5)
        + scale_fill_manual(values={"Mayoría Hombres": pal["BH"],
                                    "Mayoría Mujeres": pal["BM"]})
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
    out = os.path.join(get_plot_dir(), "ocupacion_divergente.png")
    p.save(out, width=12, height=6, dpi=150, verbose=False)
    context.add_output_metadata({"plot": MetadataValue.md(f"![Ocupacion Divergente]({out})")})


@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_mapa_distribucion_renta(context: AssetExecutionContext) -> None:
    cfg        = get_plot_config()["mapa_distribucion"]
    año        = cfg["ano"]
    componente = cfg["componente"]

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

    df     = pd.read_csv(get_processed_path(cfg["dataset"])).dropna(subset=["OBS_VALUE"])
    df_fil = (df[(df["año"] == año) & (df["MEDIDAS_CODE"] == componente)]
              .groupby("municipio", as_index=False)["OBS_VALUE"].median())

    gdf_mun = cargar_gdf_municipios(año, context.log)
    if gdf_mun is None:
        context.log.warning(f"GeoJSON no disponible para {año}. Asset omitido.")
        return

    gdf = gdf_mun.merge(df_fil, on="municipio", how="left")

    fig, ax = plt.subplots(figsize=(14, 8))
    gdf.plot(
        column="OBS_VALUE", cmap=CMAPS.get(componente, "YlOrRd"),
        linewidth=0.08, edgecolor="white", legend=True,
        legend_kwds={"label": LABELS.get(componente, componente),
                     "orientation": "vertical", "shrink": 0.55, "pad": 0.01},
        missing_kwds={"color": "#dddddd", "label": "Sin datos"},
        ax=ax,
    )
    ax.set_title(
        f"{LABELS.get(componente,'').replace(' (%)','')}"
        f" sobre renta total — Tenerife {año}",
        fontsize=14, fontweight="bold", pad=12)
    ax.annotate("Por municipios · Fuente: ISTAC",
                xy=(0.01, 0.98), xycoords="axes fraction",
                fontsize=9, color="#555555", va="top")
    ax.axis("off")
    fig.tight_layout()

    out = os.path.join(get_plot_dir(), f"mapa_{componente.lower()}_{año}.png")
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    context.add_output_metadata({"plot": MetadataValue.md(f"![Mapa Distribución]({out})")})


@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_brecha_salarial(context: AssetExecutionContext) -> None:
    """
    Slope chart: top N municipios con mayor variación absoluta del índice
    de brecha salarial entre ano_ini y ano_fin.
    """
    cfg     = get_plot_config()["brecha_salarial"]
    pal     = get_paleta()
    TOP_N   = cfg.get("top_n", 5)
    AÑO_INI = cfg.get("ano_ini", 2021)
    AÑO_FIN = cfg.get("ano_fin", 2023)
    UMBRAL  = cfg.get("umbral", 0.02)

    ocu  = pd.read_csv(get_processed_path(cfg["dataset_ocu"])).dropna(subset=["num_casos"])
    dist = pd.read_csv(get_processed_path(cfg["dataset_dist"])).dropna(subset=["OBS_VALUE"])

    ocu_hm = (
        ocu[ocu["sexo"].isin(["Hombres","Mujeres"]) & (ocu["ocupacion"] != "No consta")]
        .groupby(["municipio","año","sexo"], as_index=False)["num_casos"].sum()
        .pivot(index=["municipio","año"], columns="sexo", values="num_casos")
        .reset_index()
    )
    ocu_hm.columns.name = None
    ocu_hm["ratio_hm"] = ocu_hm["Hombres"] / (ocu_hm["Hombres"] + ocu_hm["Mujeres"])

    sal = (
        dist[dist["MEDIDAS_CODE"] == "SUELDOS_SALARIOS"]
        .groupby(["municipio","año"], as_index=False)["OBS_VALUE"].median()
        .rename(columns={"OBS_VALUE": "pct_salarios"})
    )

    merged = ocu_hm.merge(sal, on=["municipio","año"], how="inner")
    merged["indice_brecha"] = (merged["ratio_hm"] - 0.5) * merged["pct_salarios"]

    ini   = merged[merged["año"] == AÑO_INI][["municipio","indice_brecha"]].rename(
        columns={"indice_brecha": "brecha_ini"})
    fin   = merged[merged["año"] == AÑO_FIN][["municipio","indice_brecha"]].rename(
        columns={"indice_brecha": "brecha_fin"})
    slope = ini.merge(fin, on="municipio")
    slope["delta"]     = slope["brecha_fin"] - slope["brecha_ini"]
    slope["direccion"] = slope["delta"].apply(
        lambda d: "Brecha aumenta" if d > UMBRAL
        else ("Brecha disminuye" if d < -UMBRAL else "Sin cambio relevante"))

    # Top N por variación absoluta (más dinámico que por brecha_media)
    top = slope.reindex(slope["delta"].abs().nlargest(TOP_N).index)

    long = pd.concat([
        top.assign(año=AÑO_INI, brecha=top["brecha_ini"]),
        top.assign(año=AÑO_FIN, brecha=top["brecha_fin"]),
    ])
    long["año_cat"] = pd.Categorical(
        long["año"], categories=[AÑO_INI, AÑO_FIN], ordered=True)

    mediana_global = float(long["brecha"].median())

    COLORES = {
        "Brecha aumenta":       "#e63946",
        "Brecha disminuye":     "#2a9d8f",
        "Sin cambio relevante": "#AAAAAA",
    }

    p = (
        ggplot(long, aes(x="año_cat", y="brecha",
                         group="municipio", color="direccion"))
        + geom_hline(yintercept=mediana_global, linetype="dashed",
                     color="#888888", size=0.5, alpha=0.7)
        + geom_line(size=0.9, alpha=0.8)
        + geom_point(size=2.5, stroke=0.3)
        + geom_text(aes(label="municipio"),
                    data=long[long["año"] == AÑO_FIN],
                    ha="left", nudge_x=0.05, size=9)
        + scale_color_manual(values=COLORES, name=None)
        + scale_x_discrete(expand=(0.45, 0.45))
        + labs(
            title="Evolución de la brecha salarial de género por municipio",
            subtitle=(f"Índice = ratio H/(H+M) × % sueldos sobre renta · "
                      f"Top {TOP_N} municipios por mayor variación · {AÑO_INI}→{AÑO_FIN}"),
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
    out = os.path.join(get_plot_dir(), "brecha_salarial_slope.png")
    p.save(out, width=11, height=9, dpi=150, verbose=False)
    context.add_output_metadata(
        {"plot": MetadataValue.md(f"![Brecha Salarial Slope]({out})")})


@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_mapa_brecha_salarial(context: AssetExecutionContext) -> None:
    """Mapa coroplético de la brecha salarial — provincia SC Tenerife."""
    cfg     = get_plot_config()["brecha_salarial"]
    pal     = get_paleta()
    AÑO_MAPA = cfg.get("ano_mapa", 2023)

    ocu  = pd.read_csv(get_processed_path(cfg["dataset_ocu"])).dropna(subset=["num_casos"])
    dist = pd.read_csv(get_processed_path(cfg["dataset_dist"])).dropna(subset=["OBS_VALUE"])

    ocu_hm = (
        ocu[ocu["sexo"].isin(["Hombres","Mujeres"]) & (ocu["ocupacion"] != "No consta")]
        .groupby(["municipio","año","sexo"], as_index=False)["num_casos"].sum()
        .pivot(index=["municipio","año"], columns="sexo", values="num_casos")
        .reset_index()
    )
    ocu_hm.columns.name = None
    ocu_hm["ratio_hm"] = ocu_hm["Hombres"] / (ocu_hm["Hombres"] + ocu_hm["Mujeres"])

    sal = (
        dist[dist["MEDIDAS_CODE"] == "SUELDOS_SALARIOS"]
        .groupby(["municipio","año"], as_index=False)["OBS_VALUE"].median()
        .rename(columns={"OBS_VALUE": "pct_salarios"})
    )

    merged = ocu_hm.merge(sal, on=["municipio","año"], how="inner")
    merged["indice_brecha"] = (merged["ratio_hm"] - 0.5) * merged["pct_salarios"]

    lim  = max(abs(merged["indice_brecha"].min()), abs(merged["indice_brecha"].max()))
    norm = mcolors.TwoSlopeNorm(vmin=-lim, vcenter=0, vmax=lim)

    gdf_mun = cargar_gdf_municipios(AÑO_MAPA, context.log)
    fig, ax = plt.subplots(figsize=(12, 8))

    if gdf_mun is None:
        ax.set_title(f"{AÑO_MAPA} - Sin Datos Espaciales")
        ax.axis("off")
    else:
        datos = merged[merged["año"] == AÑO_MAPA][["municipio","indice_brecha"]]
        gdf_p = gdf_mun.merge(datos, on="municipio", how="left")
        gdf_p.plot(
            column="indice_brecha", cmap=pal["cmap_brecha"],
            norm=norm, linewidth=0.15, edgecolor="white",
            missing_kwds={"color": "#dddddd", "label": "Sin datos"},
            legend=False, ax=ax)
        ax.axis("off")

    sm   = ScalarMappable(cmap=pal["cmap_brecha"], norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, orientation="vertical", shrink=0.55, pad=0.02)
    cbar.set_label(
        "Índice de brecha salarial\n(+ = favorable a hombres  /  − = favorable a mujeres)",
        fontsize=10)

    fig.suptitle(f"Brecha salarial de género por municipio — Tenerife {AÑO_MAPA}",
                 fontsize=15, fontweight="bold", y=0.95)
    fig.text(0.5, 0.08,
             "Índice = ratio H/(H+M) × % sueldos sobre renta · Fuente: ISTAC",
             ha="center", fontsize=9, color="#666666")
    fig.tight_layout()

    out = os.path.join(get_plot_dir(), "mapa_brecha_salarial.png")
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    context.add_output_metadata(
        {"plot": MetadataValue.md(f"![Mapa Brecha Salarial]({out})")})


@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_gini_evolucion_islas(context: AssetExecutionContext) -> None:
    """
    Líneas temporales del Índice de Gini por isla (2015-2023).
    Sin Canarias. Top 3 en 2023 resaltadas. Franja COVID. Ruptura eje Y.
    """
    cfg_plot        = get_plot_config().get("gini_evolucion_islas", {})
    empezar_en_cero = cfg_plot.get("empezar_en_cero", False)

    gini = _load_gini()
    ISLAS_SET = {"Tenerife","Gran Canaria","La Palma","La Gomera",
                 "El Hierro","Lanzarote","Fuerteventura"}
    df = gini[
        (gini["MEDIDAS"] == "Índice de Gini") &
        (gini["TERRITORIO"].isin(ISLAS_SET))
    ].copy()

    TOP3 = ["La Palma", "El Hierro", "Tenerife"]   # top 3 Gini 2023
    AÑOS = sorted(df["TIME_PERIOD"].unique())

    fig, ax = plt.subplots(figsize=(13, 6))
    fig.patch.set_facecolor("white")

    # Franja COVID
    ax.axvspan(2019.5, 2021.5, color="#fde8e8", alpha=0.45, zorder=0)
    ax.axvline(2020, color="#c0392b", lw=0.8, ls="--", alpha=0.5, zorder=1)
    ax.text(2020.15, df["OBS_VALUE"].max() - 0.15,
            "COVID-19", fontsize=8, color="#c0392b",
            fontweight="bold", va="top")

    # Líneas
    for isla in df["TERRITORIO"].unique():
        sub    = df[df["TERRITORIO"] == isla].sort_values("TIME_PERIOD")
        col    = COLORES_ISLA.get(isla, "#aaaaaa")
        is_top = isla in TOP3
        ax.plot(sub["TIME_PERIOD"], sub["OBS_VALUE"],
                color=col if is_top else COLOR_RESTO_ISLAS,
                lw=2.5 if is_top else 0.8,
                marker="o" if is_top else None,
                markersize=5 if is_top else 0,
                alpha=1.0 if is_top else 0.5,
                zorder=4 if is_top else 2)
        if is_top:
            ultimo = sub[sub["TIME_PERIOD"] == sub["TIME_PERIOD"].max()]
            ax.text(ultimo["TIME_PERIOD"].values[0] + 0.1,
                    ultimo["OBS_VALUE"].values[0],
                    isla, fontsize=8.5, color=col,
                    fontweight="bold", va="center")

    # Eje Y
    _aplicar_eje_y(ax,
                   y_min_data=float(df["OBS_VALUE"].min()),
                   y_max_data=float(df["OBS_VALUE"].max()),
                   empezar_en_cero=empezar_en_cero)

    ax.set_xlim(AÑOS[0] - 0.2, AÑOS[-1] + 1.5)
    ax.set_xticks(AÑOS)
    ax.set_xticklabels(AÑOS, fontsize=9)
    ax.set_ylabel("Índice de Gini", fontsize=10)
    ax.yaxis.grid(True, color="#eeeeee", zorder=0)
    ax.spines[["top","right"]].set_visible(False)

    handles = [mpatches.Patch(color=COLORES_ISLA[i], label=i) for i in TOP3]
    handles += [mpatches.Patch(color=COLOR_RESTO_ISLAS, alpha=0.6,
                               label="Resto de islas")]
    ax.legend(handles=handles, loc="lower left", fontsize=9, frameon=False)

    ax.set_title("Evolución del Índice de Gini por isla — Canarias 2015-2023",
                 fontsize=13, fontweight="bold", pad=12)
    ax.annotate(
        "Valores altos = mayor desigualdad  ·  "
        "Destacadas: islas con mayor desigualdad en 2023",
        xy=(0.01, 0.98), xycoords="axes fraction",
        fontsize=8.5, color="#555555", va="top")
    fig.text(0.99, 0.01, "Fuente: ISTAC",
             ha="right", fontsize=8, color="#888888")

    plt.tight_layout()
    out = os.path.join(get_plot_dir(), "gini_evolucion_islas.png")
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    context.add_output_metadata(
        {"plot": MetadataValue.md(f"![Gini Islas]({out})")})


@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_heatmap_segregacion_sectorial(context: AssetExecutionContext) -> None:
    """
    Heatmap ratio H/(H+M) por sector e isla — Canarias, Marzo 2026.
    Sin anotaciones en celda: el gradiente de color codifica la información,
    los números dentro eran redundantes y añadían ruido visual.

    GESTALT:
      Similitud   — RdBu_r centrado en 0.5: rojo = masculinizado,
                    azul = feminizado, blanco = paridad.
      Proximidad  — actividades ordenadas de más feminizadas (arriba) a más
                    masculinizadas (abajo): clusters emergen sin intervención.
      Continuidad — lectura oeste→este permite detectar si la segregación
                    es local o estructural en todo el archipiélago.
      Cierre      — bordes blancos definen cada celda sin sobrecargar.
    """
    pal = get_paleta()

    df = pd.read_csv(get_processed_path("contratos_202603.csv"))
    df.columns = df.columns.str.strip()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    top_act = (df.groupby("Actividad económica")["Contratos"]
               .sum().nlargest(12).index.tolist())

    ABREV = {
        "Producción cinematográfica, de vídeo y de programas de televisión, "
        "grabación de sonido y edición musical":                 "Prod. audiovisual",
        "Servicios a edificios y actividades de jardinería":     "Servicios a edificios",
        "Actividades de construcción especializada":             "Construcción esp.",
        "Administración pública y defensa; seguridad social obligatoria": "Adm. pública",
        "Actividades de creación artística y artes escénicas":   "Artes escénicas",
        "Actividades sanitarias":       "Sanidad",
        "Servicios de alojamiento":     "Alojamiento",
        "Servicios de comidas y bebidas": "Hostelería",
        "Comercio al por menor":        "Comercio minorista",
        "Comercio al por mayor":        "Comercio mayorista",
        "Educación":                    "Educación",
        "Construcción de edificios":    "Construcción",
    }

    pivot = (
        df[df["Actividad económica"].isin(top_act)]
        .groupby(["Actividad económica","isla","sexo"])["Contratos"]
        .sum().unstack("sexo").reset_index()
    )
    pivot.columns.name = None
    pivot["ratio_hm"]        = pivot["Hombres"] / (pivot["Hombres"] + pivot["Mujeres"])
    pivot["actividad_short"] = pivot["Actividad económica"].map(ABREV)

    ISLAS_ORD = ["EL HIERRO","LA GOMERA","LA PALMA","TENERIFE",
                 "GRAN CANARIA","LANZAROTE","FUERTEVENTURA"]
    ISLAS_LBL = ["El\nHierro","La\nGomera","La\nPalma","Tenerife",
                 "Gran\nCanaria","Lanzarote","Fuerte-\nventura"]

    orden_act = (pivot.groupby("actividad_short")["ratio_hm"]
                 .mean().sort_values(ascending=True).index.tolist())

    heat = (pivot.pivot(index="actividad_short", columns="isla", values="ratio_hm")
            .reindex(index=orden_act, columns=ISLAS_ORD))

    fig, ax = plt.subplots(figsize=(12, 6))
    fig.patch.set_facecolor("white")

    norm_c = mcolors.TwoSlopeNorm(vmin=0.0, vcenter=0.5, vmax=1.0)
    cmap_c = plt.get_cmap(pal["cmap_brecha"])
    ax.imshow(heat.values, cmap=cmap_c, norm=norm_c, aspect="auto")

    ax.set_xticks(range(len(ISLAS_ORD)))
    ax.set_xticklabels(ISLAS_LBL, fontsize=10, fontweight="bold")
    ax.set_yticks(range(len(orden_act)))
    ax.set_yticklabels(orden_act, fontsize=10)
    ax.tick_params(left=False, bottom=False)

    for i in range(len(orden_act) + 1):
        ax.axhline(i - 0.5, color="white", lw=1.2)
    for j in range(len(ISLAS_ORD) + 1):
        ax.axvline(j - 0.5, color="white", lw=1.2)

    ax.set_xlim(-0.5, len(ISLAS_ORD) - 0.5)
    ax.set_ylim(-0.5, len(orden_act) - 0.5)

    cb = fig.colorbar(ScalarMappable(norm=norm_c, cmap=cmap_c),
                      ax=ax, orientation="vertical", shrink=0.8, pad=0.02)
    cb.set_label("% hombres contratados", fontsize=9)
    cb.set_ticks([0, 0.25, 0.5, 0.75, 1.0])
    cb.set_ticklabels(["0%\n(solo mujeres)","25%","50%\n(paridad)",
                       "75%","100%\n(solo hombres)"])

    ax.set_title("Segregación de género por sector e isla — Canarias, Marzo 2026",
                 fontsize=13, fontweight="bold", pad=12)
    fig.text(0.01, -0.02,
             "Azul = mayoría mujeres · Rojo = mayoría hombres · "
             "Blanco = paridad  ·  Fuente: SEPE / OBECAN · Contratos marzo 2026",
             fontsize=8, color="#666666")

    plt.tight_layout()
    out = os.path.join(get_plot_dir(), "heatmap_segregacion_sectorial.png")
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    context.add_output_metadata(
        {"plot": MetadataValue.md(f"![Heatmap Segregación]({out})")})

# ── COVID helpers ─────────────────────────────────────────────────────────────

@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_covid_sueldos_islas(context: AssetExecutionContext) -> None:
    _plot_covid_lineas(
        context=context,
        medida="Sueldos y salarios",
        ylabel="% renta procedente de sueldos y salarios",
        titulo="Sueldos y salarios sobre renta total por isla — Canarias 2015-2023",
        fname="covid_sueldos_islas.png",
        cfg_key="covid_sueldos_islas",
    )


@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_covid_prestaciones_islas(context: AssetExecutionContext) -> None:
    _plot_covid_lineas(
        context=context,
        medida="Prestaciones por desempleo",
        ylabel="% renta procedente de prestaciones por desempleo",
        titulo="Prestaciones por desempleo sobre renta total por isla — Canarias 2015-2023",
        fname="covid_prestaciones_islas.png",
        cfg_key="covid_prestaciones_islas",
    )


def _plot_covid_lineas(context, medida: str, ylabel: str,
                       titulo: str, fname: str, cfg_key: str) -> None:
    cfg_plot        = get_plot_config().get(cfg_key, {})
    empezar_en_cero = cfg_plot.get("empezar_en_cero", False)

    rentas = _load_rentas()
    ISLAS_TURISTICAS = {"Lanzarote","Fuerteventura","Tenerife"}
    ISLAS_RESTO      = {"Gran Canaria","La Palma","La Gomera","El Hierro"}

    sub_all = rentas[rentas["MEDIDAS"] == medida]
    y_min_d = float(sub_all["OBS_VALUE"].min())
    y_max_d = float(sub_all["OBS_VALUE"].max())

    fig, ax = plt.subplots(figsize=(13, 6))
    fig.patch.set_facecolor("white")

    ax.axvspan(2019.5, 2021.5, color="#fde8e8", alpha=0.45, zorder=0)
    ax.axvline(2020, color="#c0392b", lw=0.8, ls="--", alpha=0.5, zorder=1)

    handles = []

    # Resto: gris uniforme (fondo)
    for isla in sorted(ISLAS_RESTO):
        sub = rentas[(rentas["TERRITORIO"] == isla) &
                     (rentas["MEDIDAS"] == medida)].sort_values("TIME_PERIOD")
        ax.plot(sub["TIME_PERIOD"], sub["OBS_VALUE"],
                color=COLOR_RESTO_ISLAS, lw=1.0, alpha=0.5, zorder=2)
    handles.append(mpatches.Patch(color=COLOR_RESTO_ISLAS, alpha=0.6,
                                  label="Resto de islas"))

    # Turísticas: colores del proyecto (figura)
    for isla in ["Tenerife","Fuerteventura","Lanzarote"]:
        sub = rentas[(rentas["TERRITORIO"] == isla) &
                     (rentas["MEDIDAS"] == medida)].sort_values("TIME_PERIOD")
        col = COLORES_ISLA[isla]
        ax.plot(sub["TIME_PERIOD"], sub["OBS_VALUE"],
                color=col, lw=3.0, marker="o", markersize=6,
                alpha=1.0, zorder=4)
        handles.append(mpatches.Patch(color=col, label=isla))

    # Anotación COVID posición dinámica
    ax.text(2020.15, y_max_d * 0.99,
            "COVID-19", fontsize=8.5, color="#c0392b",
            fontweight="bold", va="top")

    _aplicar_eje_y(ax, y_min_d, y_max_d, empezar_en_cero)

    ax.set_xlim(2014.8, 2023.2)
    ax.set_xticks(range(2015, 2024))
    ax.set_xticklabels(range(2015, 2024), fontsize=8.5)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.yaxis.grid(True, color="#eeeeee", zorder=0)
    ax.spines[["top","right"]].set_visible(False)
    ax.set_title(titulo, fontsize=13, fontweight="bold", pad=12)
    ax.legend(handles=handles, loc="center left",
              bbox_to_anchor=(1.02, 0.5), fontsize=9,
              frameon=False, title="Islas")

    fig.text(0.99, 0.01, "Fuente: ISTAC", ha="right", fontsize=8, color="#888888")
    plt.tight_layout()

    out = os.path.join(get_plot_dir(), fname)
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    context.add_output_metadata({"plot": MetadataValue.md(f"![{titulo}]({out})")})


@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_brecha_temporal_edad(context: AssetExecutionContext) -> None:
    """Barras agrupadas H/M por tipo de contrato, facet por edad — Marzo 2026."""
    pal = get_paleta()

    df = pd.read_csv(get_processed_path("contratos_202603.csv"))
    df.columns = df.columns.str.strip()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
    df = df[df["sexo"].isin(["Hombres","Mujeres"])]

    cfg  = get_plot_config().get("brecha_temporal_edad", {})
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

    EDAD_ORDER     = ["Menor de 25","Entre 25 y 44","45 o más"]
    TC_LABEL_ORDER = ["Temp. Parcial","Temp. Completo","Conversión","Indefinido"]
    COLORS = {"Hombres": pal["H"], "Mujeres": pal["M"]}

    agg     = df.groupby(["edad","tc","sexo"])["Contratos"].sum().reset_index()
    agg     = agg[agg["tc"].isin(TC_LABEL_ORDER)]
    totales = agg.groupby(["edad","sexo"])["Contratos"].sum().reset_index(name="total")
    agg     = agg.merge(totales, on=["edad","sexo"])
    agg["pct"] = agg["Contratos"] / agg["total"] * 100

    fig, axes = plt.subplots(1, 3, figsize=(15, 6), sharey=True)
    fig.patch.set_facecolor("white")
    x = np.arange(len(TC_LABEL_ORDER))
    w = 0.35

    for ax, edad in zip(axes, EDAD_ORDER):
        ax.set_facecolor("white")
        for sexo, offset in [("Hombres",-w/2),("Mujeres",w/2)]:
            vals = [
                float(agg[(agg["edad"]==edad)&(agg["sexo"]==sexo)&
                          (agg["tc"]==tc)]["pct"].values[0])
                if len(agg[(agg["edad"]==edad)&(agg["sexo"]==sexo)&
                           (agg["tc"]==tc)]) > 0 else 0.0
                for tc in TC_LABEL_ORDER
            ]
            ax.bar(x + offset, vals, w,
                   color=COLORS[sexo], alpha=0.85, zorder=3)
            for xi, v in zip(x + offset, vals):
                if v > 4:
                    ax.text(xi, v + 0.4, f"{v:.0f}%",
                            ha="center", va="bottom", fontsize=7.5,
                            color=COLORS[sexo], fontweight="bold")

        ax.set_xticks(x)
        ax.set_xticklabels(TC_LABEL_ORDER, fontsize=9, rotation=20, ha="right")
        ax.set_title(edad, fontsize=11, fontweight="bold", pad=8)
        ax.yaxis.grid(True, color="#eeeeee", zorder=0)
        ax.spines[["top","right","left"]].set_visible(False)
        ax.set_ylim(0, 62)
        if ax == axes[0]:
            ax.set_ylabel("% sobre contratos del grupo edad-sexo", fontsize=10)

    titulo_loc = "Canarias" if ISLA == "Todas" else ISLA
    fig.suptitle(
        f"Distribución del tipo de contrato por edad y género — "
        f"{titulo_loc}, Marzo 2026",
        fontsize=13, fontweight="bold")

    handles = [mpatches.Patch(color=c, label=s, alpha=0.85)
               for s, c in COLORS.items()]
    fig.legend(handles=handles, loc="lower center", ncol=2,
               fontsize=9, frameon=False, bbox_to_anchor=(0.5, -0.04))
    fig.text(0.99, -0.07,
             "Fuente: SEPE / OBECAN · Contratos marzo 2026",
             ha="right", fontsize=8, color="#888888")

    plt.tight_layout(rect=[0, 0.0, 1, 0.96])
    out = os.path.join(get_plot_dir(), "brecha_temporal_parcial_edad.png")
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    context.add_output_metadata(
        {"plot": MetadataValue.md(f"![Brecha Edad]({out})")})


@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_historico_tipos_contrato_por_edad(context: AssetExecutionContext) -> None:
    """
    4 figuras (una por tipo de contrato) con evolución histórica 2019-2026
    del % sobre total, por franja de edad y género.
    Sin fill_between.
    """
    pal = get_paleta()

    def detect_sep(p):
        with open(p, "r", encoding="utf-8", errors="ignore") as f:
            l = f.readline()
        return ";" if l.count(";") > l.count(",") else ","

    data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)

    FUENTES = [
        (2019, [os.path.join(data_dir, "contratos2019.csv")],                                   "contratos"),
        (2020, [os.path.join(data_dir, "contratos2020.csv")],                                   "contratos"),
        (2021, [os.path.join(data_dir, "contratos2021.csv")],                                   "contratos"),
        (2022, [os.path.join(data_dir, "contratos2022.csv")],                                   "contratos"),
        (2023, sorted(glob.glob(os.path.join(data_dir,"2023","contratos_registrados_*.csv"))),  "Contratos"),
        (2024, sorted(glob.glob(os.path.join(data_dir,"2024","contratos_registrados_*.csv"))),  "Contratos"),
        (2025, sorted(glob.glob(os.path.join(data_dir,"2025","contratos_202*.csv"))),           "Contratos"),
    ]

    df26 = pd.read_csv(
        get_processed_path("contratos_202603.csv"),
        sep=detect_sep(get_processed_path("contratos_202603.csv")),
        dtype={"Contratos": float})
    df26.columns = df26.columns.str.strip()
    df26 = df26.rename(columns={"Contratos": "c"})
    for col in df26.select_dtypes(include="object").columns:
        df26[col] = df26[col].str.strip()
    df26 = df26[df26["sexo"].isin(["Hombres","Mujeres"])]
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
            dfs.append(df[df["sexo"].isin(["Hombres","Mujeres"])])
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
    EDADES   = ["Menor de 25","Entre 25 y 44","45 o más"]
    AÑOS     = [2019,2020,2021,2022,2023,2024,2025,2026]
    COLORS   = {"Hombres": pal["H"], "Mujeres": pal["M"]}
    TC_TITLE = {
        "Temp. Parcial":  "Contrato temporal a tiempo parcial",
        "Temp. Completo": "Contrato temporal a tiempo completo",
        "Conversión":     "Conversión a indefinido",
        "Indefinido":     "Contrato indefinido",
    }

    df_hist["tc"] = df_hist["Tipo Contrato"].str.strip().map(TC_MAP)
    df_hist = df_hist.dropna(subset=["tc","edad"])
    df_hist = df_hist[df_hist["edad"].isin(EDADES)]

    total = (df_hist.groupby(["año","edad","sexo"])["c"]
             .sum().reset_index(name="total"))
    agg   = df_hist.groupby(["año","edad","sexo","tc"])["c"].sum().reset_index()
    agg   = agg.merge(total, on=["año","edad","sexo"])
    agg["pct"] = agg["c"] / agg["total"] * 100

    output_paths = []
    for tc_name in TC_MAP.values():
        fig, axes = plt.subplots(1, len(EDADES), figsize=(14, 5),
                                 sharey=True, sharex=True)
        fig.patch.set_facecolor("white")

        for col, edad in enumerate(EDADES):
            ax = axes[col]
            ax.set_facecolor("white")

            ax.axvspan(1, 2.5, color="#f5f5f5", alpha=0.8, zorder=0)
            ax.axvline(2.5, color="#dddddd", lw=0.8, zorder=1)

            for sexo in ["Hombres","Mujeres"]:
                sub  = agg[(agg["sexo"]==sexo) &
                           (agg["edad"]==edad) &
                           (agg["tc"]==tc_name)].set_index("año")
                vals = [sub.loc[a,"pct"] if a in sub.index else np.nan
                        for a in AÑOS]

                xs = [i for i,a in enumerate(AÑOS)
                      if a <= 2025 and not np.isnan(vals[i])]
                ys = [vals[i] for i in xs]
                ax.plot(xs, ys, color=COLORS[sexo], lw=2.2,
                        alpha=0.9, zorder=4, solid_capstyle="round")

                for i_pt in [0, AÑOS.index(2025)]:
                    if not np.isnan(vals[i_pt]):
                        ax.scatter(i_pt, vals[i_pt], s=55,
                                   color=COLORS[sexo], zorder=5,
                                   edgecolors="white", linewidths=0.8)

                i26 = AÑOS.index(2026)
                v26 = vals[i26]
                if not np.isnan(v26):
                    ax.scatter(i26, v26, s=45, color=COLORS[sexo],
                               marker="D", zorder=5, alpha=0.6,
                               edgecolors="white", linewidths=0.8)

            # Sin fill_between

            ax.set_title(edad, fontsize=11, fontweight="bold",
                         pad=8, color="#333333")
            ax.set_xticks(range(len(AÑOS)))
            ax.set_xticklabels(
                [str(a) if a != 2026 else "Mar\n2026" for a in AÑOS],
                fontsize=8.5, rotation=30, ha="right")

            ax.yaxis.grid(True, color="#eeeeee", lw=0.8, zorder=0)
            ax.spines[["top","right","bottom"]].set_visible(False)
            ax.spines["left"].set_color("#eeeeee")
            ax.tick_params(axis="y", labelsize=8.5, colors="#888888")
            ax.tick_params(axis="x", length=0)

            if col == 0:
                ax.set_ylabel("% sobre total contratos del grupo",
                              fontsize=9.5, color="#444444")

        handles = [mpatches.Patch(color=COLORS[s], label=s)
                   for s in ["Hombres","Mujeres"]]
        handles += [plt.scatter([], [], marker="D", color="#aaaaaa",
                                s=40, alpha=0.6, label="Mar 2026 (dato parcial)")]
        fig.legend(handles=handles, loc="lower center", ncol=3,
                   fontsize=10, frameon=False, bbox_to_anchor=(0.5, -0.04))

        fig.suptitle(f"{TC_TITLE[tc_name]} — Canarias 2019-2026",
                     fontsize=13, fontweight="bold", y=1.01)
        fig.text(0.99, -0.06, "Fuente: OBECAN / SEPE",
                 ha="right", fontsize=8, color="#888888")

        plt.tight_layout(rect=[0, 0.08, 1, 1])

        safe = tc_name.lower().replace(". ","_").replace(" ","_")
        out  = os.path.join(get_plot_dir(), f"historico_{safe}.png")
        fig.savefig(out, dpi=150, bbox_inches="tight")
        plt.close(fig)
        output_paths.append(out)
        context.log.info(f"✓ {out}")

    context.add_output_metadata({
        "plots": MetadataValue.md(
            "\n".join(f"- `{os.path.basename(p)}`" for p in output_paths))
    })


@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_mapa_brecha_salarial_canarias(context: AssetExecutionContext) -> None:
    """
    Mapa coroplético del índice de brecha salarial por municipio — Toda Canarias.
    Usa contratos 2023 (flujo anual completo) × rentas 2023 del mismo año,
    evitando el cruce temporal imperfecto del boxplot (contratos mar 2026 × rentas 2023).
    GeoJSON: canarias2026.geojson (88 municipios).

    GESTALT:
      Similitud   — RdBu_r divergente centrado en 0: rojo = favorable a hombres,
                    azul = favorable a mujeres, blanco = paridad.
      Proximidad  — municipios vecinos se comparan sin esfuerzo cognitivo.
      Cierre      — polígonos municipales como unidades perceptivas completas.
      Figura/Fondo — grises para municipios sin dato (sin datos suficientes).

    DISEÑO:
      TwoSlopeNorm centrada en 0. Límite de escala en percentil 95 para que
      outliers extremos no aplanen el gradiente del resto del territorio.
      Misma paleta que plot_mapa_brecha_salarial (Tenerife) para coherencia.
      Fuente explícita: contratos SEPE 2023 × rentas ISTAC 2023 (mismo año).
    """
    import re
    import glob
    import unicodedata

    pal  = get_paleta()
    cmap = pal["cmap_brecha"]

    data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
    geojson  = os.path.join(data_dir, "municipios2023.json")

    if not os.path.exists(geojson):
        context.log.warning(f"GeoJSON no encontrado: {geojson}")
        return

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _detect_sep(p):
        with open(p, "r", encoding="utf-8", errors="ignore") as f:
            l = f.readline()
        return ";" if l.count(";") > l.count(",") else ","

    def _fix_articulo(s):
        m = re.match(r"^(.+),\s*(La|El|Los|Las)$", str(s).strip(), re.IGNORECASE)
        return f"{m.group(2)} {m.group(1)}" if m else s.strip()

    def _norm(s):
        return (unicodedata.normalize("NFD", str(s).strip().upper())
                .encode("ascii", "ignore").decode())

    MANUAL = {}   # municipios2023.json tiene etiquetas limpias, sin artículos invertidos

    # ── Cargar contratos 2023 ─────────────────────────────────────────────────
    paths_2023 = sorted(glob.glob(
        os.path.join(data_dir, "2023", "contratos_registrados_*.csv")))
    if not paths_2023:
        context.log.warning("No hay ficheros de contratos 2023.")
        return

    dfs = []
    for p in paths_2023:
        df = pd.read_csv(p, sep=_detect_sep(p), dtype={"Contratos": float})
        df.columns = df.columns.str.strip()
        df = df.rename(columns={"Contratos": "c"})
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].str.strip()
        if "Municipio" in df.columns:
            df["Municipio"] = df["Municipio"].apply(_fix_articulo)
        dfs.append(df[df["sexo"].isin(["Hombres", "Mujeres"])])

    df23 = pd.concat(dfs, ignore_index=True)

    ratio = (df23.groupby(["Municipio", "sexo"])["c"]
             .sum().unstack("sexo").fillna(0).reset_index())
    ratio.columns.name = None
    ratio["ratio_hm"] = ratio["Hombres"] / (ratio["Hombres"] + ratio["Mujeres"])

    # ── Cargar rentas 2023 ────────────────────────────────────────────────────
    rentas = _load_rentas()
    sal = (rentas[(rentas["MEDIDAS"] == "Sueldos y salarios") &
                  (rentas["TIME_PERIOD"] == 2023)]
           [["TERRITORIO", "OBS_VALUE"]]
           .rename(columns={"TERRITORIO": "Municipio",
                             "OBS_VALUE":  "pct_salarios"}))

    merged = ratio.merge(sal, on="Municipio", how="inner")
    merged["indice_brecha"] = (merged["ratio_hm"] - 0.5) * merged["pct_salarios"]
    merged["mun_norm"] = merged["Municipio"].apply(_norm)

    context.log.info(
        f"Municipios con dato: {len(merged)} · "
        f"rango [{merged['indice_brecha'].min():.2f}, "
        f"{merged['indice_brecha'].max():.2f}]")

    # ── GeoJSON ───────────────────────────────────────────────────────────────
    # municipios2023.json usa columna 'etiqueta' con nombres limpios (sin artículos invertidos)
    gdf = gpd.read_file(geojson)
    gdf["nombre_norm"] = gdf["etiqueta"].apply(_norm)
    gdf = gdf.merge(merged[["mun_norm", "indice_brecha"]],
                    left_on="nombre_norm", right_on="mun_norm", how="left")

    n_ok = int(gdf["indice_brecha"].notna().sum())
    context.log.info(f"Join GeoJSON: {n_ok}/88 municipios")

    # ── Mapa ──────────────────────────────────────────────────────────────────
    lim      = float(np.percentile(merged["indice_brecha"].abs(), 95))
    norm_col = mcolors.TwoSlopeNorm(vmin=-lim, vcenter=0, vmax=lim)

    fig, ax = plt.subplots(figsize=(14, 9))
    fig.patch.set_facecolor("white")

    gdf.plot(
        column="indice_brecha", cmap=cmap, norm=norm_col,
        linewidth=0.3, edgecolor="white",
        missing_kwds={"color": "#dddddd", "label": "Sin datos"},
        legend=False, ax=ax)

    sm = ScalarMappable(cmap=cmap, norm=norm_col)
    sm.set_array([])
    cb = fig.colorbar(sm, ax=ax, orientation="vertical", shrink=0.55, pad=0.02)
    cb.set_label(
        "Índice de brecha salarial\n"
        "(+ = favorable a hombres  /  − = favorable a mujeres)",
        fontsize=9)

    ax.set_title(
        "Brecha salarial de género por municipio — Canarias 2023",
        fontsize=14, fontweight="bold", pad=12)
    ax.annotate(
        "Índice = ratio H/(H+M) contratos × % sueldos sobre renta · "
        "Rojo = favorable hombres · Azul = favorable mujeres · Gris = sin datos",
        xy=(0.01, 0.98), xycoords="axes fraction",
        fontsize=8.5, color="#555555", va="top")
    ax.axis("off")
    fig.text(0.99, 0.01,
             "Fuente: SEPE / ISTAC — Contratos 2023 × Rentas 2023",
             ha="right", fontsize=8, color="#888888")

    plt.tight_layout()
    out = os.path.join(get_plot_dir(), "mapa_brecha_salarial_canarias.png")
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)

    context.add_output_metadata({
        "municipios_con_dato": MetadataValue.int(n_ok),
        "plot": MetadataValue.md(f"![Mapa Brecha Canarias]({out})"),
    })