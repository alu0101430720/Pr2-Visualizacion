import os
import glob
import re
import textwrap
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
from matplotlib.colors import LinearSegmentedColormap
from plotnine import *
from dagster import (
    asset, AssetExecutionContext, MetadataValue,
)
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


def get_paleta():
    cfg = get_plot_config().get("paleta", {})

    # Colormap personalizado: azul → blanco → rosa.
    # Si el YAML define cmap_brecha como string, se usa ese nombre de matplotlib.
    cmap_custom = LinearSegmentedColormap.from_list(
        "azul_rosa",
        ["#4A90D9", "#ffffff", "#D94A8C"],
    )
    cmap_val = cfg.get("cmap_brecha", None)
    cmap_brecha = plt.get_cmap(cmap_val) if isinstance(cmap_val, str) else cmap_custom

    return {
        "H":           cfg.get("color_hombres",                  "#4A90D9"),
        "M":           cfg.get("color_mujeres",                  "#D94A8C"),
        "BH":          cfg.get("color_brecha_favorable_hombres", "#4A90D9"),
        "BM":          cfg.get("color_brecha_favorable_mujeres", "#D94A8C"),
        "cmap_brecha": cmap_brecha,
    }


def _aplicar_eje_y(ax, y_min_data: float, y_max_data: float,
                   empezar_en_cero: bool, margen_sup: float = 0.08) -> None:
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


def preprocess_file_if_needed(source_path: str, target_path: str) -> None:
    if not os.path.exists(source_path):
        return
    # If target already exists and is newer than source, skip preprocessing
    if os.path.exists(target_path) and os.path.getmtime(target_path) >= os.path.getmtime(source_path):
        return
        
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    
    # Detect separator
    with open(source_path, "r", encoding="utf-8", errors="ignore") as f:
        l = f.readline()
    sep = ";" if l.count(";") > l.count(",") else ","
    
    # Load and preprocess
    df = pd.read_csv(source_path, sep=sep)
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
    df.to_csv(target_path, index=False)


def get_processed_contratos(año: int, mes: int = None) -> list[str]:
    source_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
    processed_dir = os.path.join(source_dir, "processed")
    os.makedirs(processed_dir, exist_ok=True)
    
    source_paths = []
    
    if año in (2019, 2020, 2021, 2022):
        source_paths = [os.path.join(source_dir, f"contratos{año}.csv")]
    elif mes is not None:
        mes_str = f"{mes:02d}"
        if año == 2023:
            source_paths = [os.path.join(source_dir, "2023", f"contratos_registrados_2023{mes_str}.csv")]
        elif año == 2024:
            source_paths = [os.path.join(source_dir, "2024", f"contratos_registrados_2024{mes_str}.csv")]
        elif año == 2025:
            source_paths = [os.path.join(source_dir, "2025", f"contratos_2025{mes_str}.csv")]
        elif año == 2026:
            path_root = os.path.join(source_dir, f"contratos_2026{mes_str}.csv")
            if os.path.exists(path_root):
                source_paths = [path_root]
            else:
                source_paths = [os.path.join(source_dir, "2026", f"contratos_2026{mes_str}.csv")]
    else:
        # Load whole year
        if año == 2023:
            source_paths = sorted(glob.glob(os.path.join(source_dir, "2023", "contratos_registrados_*.csv")))
        elif año == 2024:
            source_paths = sorted(glob.glob(os.path.join(source_dir, "2024", "contratos_registrados_*.csv")))
        elif año == 2025:
            source_paths = sorted(glob.glob(os.path.join(source_dir, "2025", "contratos_202*.csv")))
        elif año == 2026:
            source_paths = sorted(glob.glob(os.path.join(source_dir, "contratos_202*.csv")))
            
    processed_paths = []
    for sp in source_paths:
        if os.path.exists(sp):
            tp = os.path.join(processed_dir, os.path.basename(sp))
            preprocess_file_if_needed(sp, tp)
            processed_paths.append(tp)
            
    return processed_paths


def get_geojson_path(filename):
    return os.path.join(config.TARGET_DIR, config.DATA_P5_DIR,
                        "cartografia-secciones", filename)


def get_plot_dir():
    plot_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR, "plots")
    os.makedirs(plot_dir, exist_ok=True)
    return plot_dir


def fmt_k(l):
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


def _calcular_indice_brecha(ocu: pd.DataFrame,
                             dist: pd.DataFrame) -> pd.DataFrame:
    """Calcula indice_brecha por municipio y año. Reutilizable entre assets."""
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
    return merged


# ── Constantes compartidas ────────────────────────────────────────────────────
ISLAS_ORDEN = [
    "Canarias", "Tenerife", "Gran Canaria", "La Palma",
    "La Gomera", "El Hierro", "Lanzarote", "Fuerteventura",
]

COLORES_ISLA = {
    "Tenerife":      "#1B9E77",
    "Gran Canaria":  "#D95F02",
    "Lanzarote":     "#7570B3",
    "Fuerteventura": "#E7298A",
    "La Palma":      "#66A61E",
    "La Gomera":     "#E6AB02",
    "El Hierro":     "#A6761D",
}

COLOR_RESTO_ISLAS = "#b0bec5"


def get_colores_isla():
    try:
        cfg = get_plot_config().get("colores_islas", {})
        if cfg:
            merged = COLORES_ISLA.copy()
            for k, v in cfg.items():
                if k in merged:
                    merged[k] = v
            return merged
    except Exception:
        pass
    return COLORES_ISLA


def _load_gini() -> pd.DataFrame:
    return pd.read_csv(get_processed_path("gini.csv")).dropna(subset=["OBS_VALUE"])


def _load_rentas() -> pd.DataFrame:
    return pd.read_csv(get_processed_path("rentas.csv")).dropna(subset=["OBS_VALUE"])


# ══════════════════════════════════════════════════════════════════════════════
# ASSETS
# ══════════════════════════════════════════════════════════════════════════════

@asset(deps=[preprocesar_datos_p5], group_name="viz_estructura_laboral")
def plot_actividad_barras(context: AssetExecutionContext) -> None:
    """
    Barras por actividad económica y sexo.
    Modo configurable en plot_config.yaml:
      'fill'  → 100% apilado: muestra solo proporción H/M  (recomendado)
      'stack' → apilado absoluto: volumen + composición
      'dodge' → agrupado: comparar valores absolutos H vs M
    """
    from checks_p5 import inferir_isla  # import local documentado: depende de checks_p5

    cfg  = get_plot_config()["actividad_barras"]
    pal  = get_paleta()
    ISLA = cfg.get("isla", "Todas")
    MODO = cfg.get("modo", "fill")  # fill | stack | dodge

    df = pd.read_csv(get_processed_path(cfg["dataset"])).dropna(subset=["num_casos"])
    df = df[df["Sexo"].isin(["Hombres", "Mujeres"]) &
            (df["Actividad económica"] != "No consta")]

    if ISLA != "Todas":
        df["isla"] = df["municipio"].apply(inferir_isla)
        df = df[df["isla"] == ISLA]

    df["actividad"] = df["Actividad económica"].replace(
        {"Agricultura, ganadería y pesca": "Agricultura/\nGanadería"})

    agg = df.groupby(["Periodo", "actividad", "Sexo"], as_index=False)["num_casos"].sum()

    # Ordenar facets por volumen total descendente
    orden_act = (
        agg.groupby("actividad")["num_casos"].sum()
        .sort_values(ascending=False).index.tolist()
    )
    agg["actividad"] = pd.Categorical(agg["actividad"], categories=orden_act, ordered=True)

    if MODO == "fill":
        pos      = position_fill()
        y_label  = "Proporción H/M"
        subtitle = "Proporción de trabajadores por sexo en cada actividad"
        y_fmt    = lambda l: [f"{v:.0%}" for v in l]
    elif MODO == "dodge":
        pos      = position_dodge(width=0.7)
        y_label  = "Nº trabajadores"
        subtitle = "Número de trabajadores por sexo en cada actividad"
        y_fmt    = fmt_k
    else:  # stack
        pos      = "stack"
        y_label  = "Nº trabajadores"
        subtitle = "Suma de trabajadores por sección censal (apilado H+M)"
        y_fmt    = fmt_k

    p = (
        ggplot(agg, aes(x="factor(Periodo)", y="num_casos", fill="Sexo"))
        + geom_col(position=pos, width=0.7, alpha=0.9)
        + facet_wrap("~ actividad", scales="free_y" if MODO != "fill" else "fixed", ncol=2)
        + scale_fill_manual(values={"Hombres": pal["H"], "Mujeres": pal["M"]})
        + scale_y_continuous(labels=y_fmt)
        + labs(
            title=f"Actividad económica por año y sexo — "
                  f"{ISLA if ISLA != 'Todas' else 'Toda la provincia'}",
            subtitle=subtitle,
            x="", y=y_label, fill="Sexo",
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


@asset(deps=[preprocesar_datos_p5], group_name="viz_estructura_laboral")
def plot_brecha_salarial(context: AssetExecutionContext) -> None:
    """
    Bar chart horizontal: top N municipios con mayor variación absoluta
    del índice de brecha salarial entre ano_ini y ano_fin.
    Ordenado por |delta| descendente. Color = dirección del cambio.
    """
    cfg     = get_plot_config()["brecha_salarial"]
    pal     = get_paleta()
    TOP_N   = cfg.get("top_n", 5)
    AÑO_INI = cfg.get("ano_ini", 2021)
    AÑO_FIN = cfg.get("ano_fin", 2023)
    UMBRAL  = cfg.get("umbral", 0.02)

    ocu  = pd.read_csv(get_processed_path(cfg["dataset_ocu"])).dropna(subset=["num_casos"])
    dist = pd.read_csv(get_processed_path(cfg["dataset_dist"])).dropna(subset=["OBS_VALUE"])

    merged = _calcular_indice_brecha(ocu, dist)

    ini  = merged[merged["año"] == AÑO_INI][["municipio", "indice_brecha"]].rename(
        columns={"indice_brecha": "brecha_ini"})
    fin  = merged[merged["año"] == AÑO_FIN][["municipio", "indice_brecha"]].rename(
        columns={"indice_brecha": "brecha_fin"})
    slope = ini.merge(fin, on="municipio")
    slope["delta"]     = slope["brecha_fin"] - slope["brecha_ini"]
    slope["direccion"] = slope["delta"].apply(
        lambda d: "Brecha aumenta" if d > UMBRAL
        else ("Brecha disminuye" if d < -UMBRAL else "Sin cambio relevante"))

    # BUG FIX: usar .loc con los índices correctos, no .reindex()
    top_idx = slope["delta"].abs().nlargest(TOP_N).index
    top = slope.loc[top_idx].sort_values("delta", key=abs, ascending=True)

    # Mediana sobre TODOS los municipios (no solo el top N)
    mediana_todos = float(merged[merged["año"].isin([AÑO_INI, AÑO_FIN])]["indice_brecha"].median())

    COLORES = {
        "Brecha aumenta":       "#D13111",
        "Brecha disminuye":     "#10C710",
        "Sin cambio relevante": "#AAAAAA",
    }

    p = (
        ggplot(top, aes(x="reorder(municipio, delta)", y="delta", fill="direccion"))
        + geom_col(width=0.65, alpha=0.9)
        + geom_hline(yintercept=0, linetype="dashed", color="#333333", size=0.4)
        + scale_fill_manual(values=COLORES, name=None)
        + coord_flip()
        + labs(
            title="Municipios con mayor cambio en brecha salarial de género",
            subtitle=(f"Δ índice entre {AÑO_INI} y {AÑO_FIN} · "
                      f"Top {TOP_N} por variación absoluta · "
                      f"+ = brecha aumenta  /  − = brecha disminuye"),
            x="", y="Cambio en índice de brecha salarial",
            caption="Fuente: ISTAC · ocupacion-sc-3 + distribucion-renta-ingresos",
        )
        + theme_minimal()
        + theme(
            figure_size=(10, 5),
            plot_title=element_text(size=14, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            panel_grid_major_y=element_blank(),
            panel_grid_minor=element_blank(),
            legend_position="bottom",
        )
    )
    out = os.path.join(get_plot_dir(), "brecha_salarial_divergente.png")
    p.save(out, width=10, height=5, dpi=150, verbose=False)
    context.add_output_metadata(
        {"plot": MetadataValue.md(f"![Brecha Salarial Divergente]({out})")})


@asset(deps=[preprocesar_datos_p5], group_name="viz_estructura_laboral")
def plot_mapa_brecha_salarial(context: AssetExecutionContext) -> None:
    """
    Mapa coroplético de la brecha salarial — provincia SC Tenerife.
    Cmap invertido: rosa = favorable a hombres, azul = favorable a mujeres.
    Sin anotaciones de texto sobre el mapa.
    """
    cfg      = get_plot_config()["brecha_salarial"]
    AÑO_MAPA = cfg.get("ano_mapa", 2023)

    # Rosa → positivo (hombres), blanco → paridad, azul → negativo (mujeres)
    cmap_mapa = LinearSegmentedColormap.from_list(
        "rosa_blanco_azul", ["#D94A8C", "#ffffff", "#4A90D9"]
    )

    ocu  = pd.read_csv(get_processed_path(cfg["dataset_ocu"])).dropna(subset=["num_casos"])
    dist = pd.read_csv(get_processed_path(cfg["dataset_dist"])).dropna(subset=["OBS_VALUE"])

    merged = _calcular_indice_brecha(ocu, dist)

    lim = max(abs(merged["indice_brecha"].min()), abs(merged["indice_brecha"].max()))
    if lim == 0:
        norm = plt.Normalize(vmin=-0.01, vmax=0.01)
    else:
        norm = mcolors.TwoSlopeNorm(vmin=-lim, vcenter=0, vmax=lim)

    gdf_mun = cargar_gdf_municipios(AÑO_MAPA, context.log)
    fig, ax = plt.subplots(figsize=(12, 8))

    if gdf_mun is None:
        ax.set_title(f"{AÑO_MAPA} - Sin Datos Espaciales")
        ax.axis("off")
    else:
        datos = merged[merged["año"] == AÑO_MAPA][["municipio", "indice_brecha"]]
        gdf_p = gdf_mun.merge(datos, on="municipio", how="left")
        gdf_p.plot(
            column="indice_brecha", cmap=cmap_mapa,
            norm=norm, linewidth=0.15, edgecolor="white",
            missing_kwds={"color": "#dddddd", "label": "Sin datos"},
            legend=False, ax=ax)

        ax.axis("off")

    sm = ScalarMappable(cmap=cmap_mapa, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, orientation="vertical", shrink=0.55, pad=0.02)
    cbar.set_label("Índice de brecha salarial", fontsize=10)
    cbar.ax.text(0.5, -0.02, "mujeres", transform=cbar.ax.transAxes,
                 ha="center", va="top", fontsize=8, color="#D94A8C")   # rosa
    cbar.ax.text(0.5, 1.02, "hombres", transform=cbar.ax.transAxes,
                 ha="center", va="bottom", fontsize=8, color="#4A90D9")  # azul

    fig.suptitle(f"Brecha salarial de género por municipio — Tenerife {AÑO_MAPA}",
                 fontsize=15, fontweight="bold", y=0.95)
    # BUG FIX: annotate en lugar de fig.text para que tight_layout lo tenga en cuenta
    fig.text(0.5, 0.01,
             "Índice = ratio H/(H+M) × % sueldos sobre renta · Fuente: ISTAC",
             ha="center", fontsize=9, color="#666666",
             transform=fig.transFigure)
    fig.tight_layout(rect=[0, 0.04, 1, 1])

    out = os.path.join(get_plot_dir(), "mapa_brecha_salarial.png")
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    context.add_output_metadata(
        {"plot": MetadataValue.md(f"![Mapa Brecha Salarial]({out})")})


@asset(deps=[preprocesar_datos_p5], group_name="viz_desigualdad_y_renta")
def plot_gini_evolucion_islas(context: AssetExecutionContext) -> None:
    """
    Líneas temporales del Índice de Gini por isla.
    Top N islas calculado dinámicamente desde el último año disponible.
    Valor numérico anotado en el punto final de cada isla destacada.
    """
    cfg_plot        = get_plot_config().get("gini_evolucion_islas", {})
    empezar_en_cero = cfg_plot.get("empezar_en_cero", False)
    top_n_islas     = cfg_plot.get("top_n_islas", 3)

    gini = _load_gini()
    ISLAS_SET = {"Tenerife", "Gran Canaria", "La Palma", "La Gomera",
                 "El Hierro", "Lanzarote", "Fuerteventura"}
    df = gini[
        (gini["MEDIDAS"] == "Índice de Gini") &
        (gini["TERRITORIO"].isin(ISLAS_SET))
    ].copy()

    # BUG FIX: verificar que el filtro devuelve datos
    if df.empty:
        context.log.warning(
            "plot_gini_evolucion_islas: sin datos tras filtrar MEDIDAS=='Índice de Gini'. "
            "Verifica tildes o espacios en el CSV."
        )
        context.add_output_metadata({
            "aviso": MetadataValue.md("⚠️ Sin datos de Gini — plot no generado.")
        })
        return

    # TOP N calculado dinámicamente desde el último año disponible
    ultimo_año = df["TIME_PERIOD"].max()
    TOP_N_ISLAS = (
        df[df["TIME_PERIOD"] == ultimo_año]
        .nlargest(top_n_islas, "OBS_VALUE")["TERRITORIO"]
        .tolist()
    )
    context.log.info(f"Top {top_n_islas} Gini en {ultimo_año}: {TOP_N_ISLAS}")

    AÑOS = sorted(df["TIME_PERIOD"].unique())
    colores = get_colores_isla()

    fig, ax = plt.subplots(figsize=(13, 6))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    ax.axvspan(2019.5, 2021.5, color="#fde8e8", alpha=0.45, zorder=0)
    ax.axvline(2020, color="#c0392b", lw=0.8, ls="--", alpha=0.5, zorder=1)
    ax.text(2020.15, df["OBS_VALUE"].max() - 0.15,
            "COVID-19", fontsize=8, color="#c0392b",
            fontweight="bold", va="top")

    for isla in df["TERRITORIO"].unique():
        sub    = df[df["TERRITORIO"] == isla].sort_values("TIME_PERIOD")
        col    = colores.get(isla, "#aaaaaa")
        is_top = isla in TOP_N_ISLAS
        ax.plot(sub["TIME_PERIOD"], sub["OBS_VALUE"],
                color=col if is_top else COLOR_RESTO_ISLAS,
                lw=2.5 if is_top else 0.8,
                marker="o" if is_top else None,
                markersize=5 if is_top else 0,
                alpha=1.0 if is_top else 0.5,
                zorder=4 if is_top else 2)

        # sin etiqueta inline — el dato va en la leyenda lateral

    _aplicar_eje_y(ax,
                   y_min_data=float(df["OBS_VALUE"].min()),
                   y_max_data=float(df["OBS_VALUE"].max() + 5),
                   empezar_en_cero=empezar_en_cero)

    ax.set_xlim(AÑOS[0] - 0.2, AÑOS[-1] + 0.5)
    ax.set_xticks(AÑOS)
    ax.set_xticklabels(AÑOS, fontsize=9)
    ax.set_ylabel("Índice de Gini", fontsize=10)
    ax.yaxis.grid(True, color="#eeeeee", zorder=0)
    ax.spines[["top", "right"]].set_visible(False)

    handles = [
        mpatches.Patch(
            color=colores.get(i, "#aaaaaa"),
            label=f"{i}  ({df[df['TERRITORIO']==i].sort_values('TIME_PERIOD')['OBS_VALUE'].iloc[-1]:.1f})"
        )
        for i in TOP_N_ISLAS
    ]
    handles += [mpatches.Patch(color=COLOR_RESTO_ISLAS, alpha=0.6,
                               label="Resto de islas")]
    ax.legend(handles=handles, loc="center left",
              bbox_to_anchor=(1.02, 0.5), fontsize=9,
              frameon=False, title="Islas")

    ax.set_title("Evolución del Índice de Gini por isla — Canarias 2015-2023",
                 fontsize=13, fontweight="bold", pad=12)
    ax.annotate(
        f"Valores altos = mayor desigualdad  ·  "
        f"Destacadas: top {top_n_islas} islas con mayor desigualdad en {ultimo_año}",
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


@asset(deps=[preprocesar_datos_p5], group_name="viz_estructura_laboral")
def plot_segregacion_sectorial(context: AssetExecutionContext) -> None:
    """
    Dot plot de Cleveland: ratio H/(H+M) por actividad e isla.
    Historia: ¿cómo varía la segregación de cada actividad entre islas?

    Gramática de gráficos:
      - Canal principal: posición en eje X común (el más preciso, Cleveland 1984)
      - Canal secundario: color por isla (identidad, no magnitud)
      - Facet por actividad: comparación entre islas dentro de cada panel
      - Línea de referencia en 0.5 (paridad) como figura; puntos como fondo

    Gestalt:
      - Proximidad: puntos de la misma actividad en el mismo panel
      - Continuidad: línea de paridad guía la lectura horizontal
      - Similitud: color por isla coherente con el resto del proyecto
      - Figura/Fondo: línea gris clara de fondo, puntos de color en primer plano
    """
    cfg   = get_plot_config().get("segregacion_sectorial", {})
    TOP_N = cfg.get("top_n", 15)
    AÑO   = cfg.get("ano", 2026)
    MES   = cfg.get("mes", 3)

    df = _cargar_contratos(os.path.join(config.TARGET_DIR, config.DATA_P5_DIR), AÑO, MES)
    if df is None:
        context.log.warning(f"No hay datos de contratos para {AÑO}-{MES:02d}.")
        return

    df.columns = df.columns.str.strip()
    df["Contratos"] = df["c"]
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    top_act = (df.groupby("Actividad económica")["Contratos"]
               .sum().nlargest(TOP_N).index.tolist())

    ABREV = {
        "Producción cinematográfica, de vídeo y de programas de televisión, "
        "grabación de sonido y edición musical":                 "Prod. audiovisual",
        "Servicios a edificios y actividades de jardinería":     "Servicios a edificios",
        "Actividades de construcción especializada":             "Construcción esp.",
        "Administración pública y defensa; seguridad social obligatoria": "Adm. pública",
        "Actividades de creación artística y artes escénicas":   "Artes escénicas",
        "Actividades sanitarias":                                "Sanidad",
        "Servicios de alojamiento":                              "Alojamiento",
        "Servicios de comidas y bebidas":                        "Hostelería",
        "Comercio al por menor":                                 "Comercio minorista",
        "Comercio al por mayor":                                 "Comercio mayorista",
        "Educación":                                             "Educación",
        "Construcción de edificios":                             "Construcción",
        "Agricultura, ganadería, caza y servicios relacionados": "Agricultura",
        "Pesca y acuicultura":                                   "Pesca",
        "Transporte terrestre y por tubería":                    "Transporte",
        "Almacenamiento y actividades anexas al transporte":     "Almacenamiento",
        "Actividades deportivas, recreativas y de entretenimiento": "Deporte/recreación",
        "Asistencia en establecimientos residenciales":          "Asist. residencial",
        "Actividades de servicios sociales sin alojamiento":     "Servicios sociales",
        "Actividades de empleo":                                 "Activ. de empleo",
    }

    # Colores por isla coherentes con el proyecto
    COLORES_ISLAS = get_colores_isla()

    # ── Preparar datos ────────────────────────────────────────────────────────
    pivot = (
        df[df["Actividad económica"].isin(top_act)]
        .groupby(["Actividad económica", "isla", "sexo"])["Contratos"]
        .sum().unstack("sexo").reset_index()
    )
    pivot.columns.name = None
    for col in ["Hombres", "Mujeres"]:
        if col not in pivot.columns:
            pivot[col] = 0
    pivot[["Hombres", "Mujeres"]] = pivot[["Hombres", "Mujeres"]].fillna(0)

    # Filtrar combinaciones con masa insuficiente (ratio inestable)
    MIN_CONTRATOS = cfg.get("min_contratos", 15)
    pivot = pivot[(pivot["Hombres"] + pivot["Mujeres"]) >= MIN_CONTRATOS].copy()

    pivot["ratio_hm"]        = pivot["Hombres"] / (pivot["Hombres"] + pivot["Mujeres"])
    pivot["actividad_short"] = pivot["Actividad económica"].map(ABREV).fillna(
        pivot["Actividad económica"].str[:30])
    pivot["isla"] = pivot["isla"].str.strip().str.title()

    # Ordenar actividades por ratio medio (más feminizadas arriba,
    # más masculinizadas abajo) — Gestalt: orden emergente sin leyenda
    orden_act = (pivot.groupby("actividad_short")["ratio_hm"]
                 .mean().sort_values(ascending=False).index.tolist())
    pivot["actividad_short"] = pd.Categorical(
        pivot["actividad_short"], categories=orden_act, ordered=True)

    # Ratio medio por actividad (para la barra de referencia de fondo)
    media_act = (pivot.groupby("actividad_short")["ratio_hm"]
                 .mean().reset_index(name="media"))

    # ── Plot ──────────────────────────────────────────────────────────────────
    n_act    = len(orden_act)
    fig_h    = max(6, n_act * 0.55)
    fig, ax  = plt.subplots(figsize=(10, fig_h))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    # Franjas alternadas muy suaves para guiar el ojo entre filas
    for i, act in enumerate(orden_act):
        if i % 2 == 0:
            ax.axhspan(i - 0.45, i + 0.45, color="#f7f7f7", zorder=0)

    # Línea de paridad (figura principal)
    ax.axvline(0.5, color="#bbbbbb", lw=1.8, ls="--", zorder=1)
    ax.text(0.5, -0.7, "paridad", ha="center", va="top",
            fontsize=7.5, color="#aaaaaa", style="italic")

    # Segmento horizontal del rango inter-isla por actividad (dispersión visual)
    for act in orden_act:
        sub = pivot[pivot["actividad_short"] == act]["ratio_hm"]
        if len(sub) >= 2:
            y = orden_act.index(act)
            ax.plot([sub.min(), sub.max()], [y, y],
                    color="#dddddd", lw=3, solid_capstyle="round", zorder=2)

    # Jitter vertical determinista por isla para evitar solapamientos en Y
    ISLAS_LIST = list(COLORES_ISLAS.keys())
    def get_island_jitter(isla_name):
        try:
            idx = ISLAS_LIST.index(isla_name)
            # Distribuye uniformemente de -0.1 a 0.1
            return -0.1 + idx * 0.033
        except ValueError:
            return 0.0

    # Puntos por isla
    for isla, color in COLORES_ISLAS.items():
        sub = pivot[pivot["isla"] == isla].copy()
        if sub.empty:
            continue
        jitter = get_island_jitter(isla)
        ys = [orden_act.index(a) + jitter for a in sub["actividad_short"]]
        ax.scatter(sub["ratio_hm"], ys,
                   color=color, s=70, zorder=4,
                   edgecolors="white", linewidths=0.6,
                   alpha=0.85,  # Transparencia leve para discernir solapamientos
                   label=isla)

    # Punto de media por actividad (triángulo negro) como referencia agregada
    for act in orden_act:
        row = media_act[media_act["actividad_short"] == act]
        if not row.empty:
            y = orden_act.index(act)
            ax.scatter(row["media"].values[0], y,
                       marker="D", s=30, color="#333333",
                       zorder=5, edgecolors="white", linewidths=0.5)

    # Ejes
    ax.set_yticks(range(n_act))
    ax.set_yticklabels(orden_act, fontsize=9.5)
    ax.set_xlim(0.0, 1.0)
    ax.set_xticks([0, 0.25, 0.5, 0.75, 1.0])
    ax.set_xticklabels(["0%", "25%", "50%\n(paridad)", "75%", "100%"],
                       fontsize=8.5)
    ax.set_xlabel("% hombres contratados", fontsize=10)
    ax.xaxis.grid(True, color="#eeeeee", zorder=0)
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.spines["bottom"].set_color("#dddddd")
    ax.tick_params(axis="y", length=0)

    # Leyenda islas + símbolo de media
    handles, labels = ax.get_legend_handles_labels()
    media_handle = plt.scatter([], [], marker="D", s=30, color="#333333",
                               label="Media Canarias")
    handles.append(media_handle)
    ax.legend(handles=handles, loc="upper left", bbox_to_anchor=(1.02, 1.0),
              fontsize=8.5, frameon=False, title="Isla", title_fontsize=8.5)

    mes_names = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
        7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
    }
    title_date = f"{mes_names.get(MES, f'{MES:02d}')} {AÑO}"

    ax.set_title(
        f"Segregación de género por actividad e isla — Canarias, {title_date}",
        fontsize=12, fontweight="bold", pad=12)
    fig.text(
        0.01, 0.0,
        "◆ = media Canarias  ·  barra gris = rango entre islas  "
        "·  Fuente: SEPE / OBECAN",
        fontsize=8, color="#666666")

    plt.tight_layout(rect=[0, 0.02, 1, 1])
    out = os.path.join(get_plot_dir(), "segregacion_sectorial_dotplot.png")
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    context.add_output_metadata(
        {"plot": MetadataValue.md(f"![Segregación dot plot]({out})")})

# ── COVID helpers ─────────────────────────────────────────────────────────────

@asset(deps=[preprocesar_datos_p5], group_name="viz_desigualdad_y_renta")
def plot_covid_sueldos_islas(context: AssetExecutionContext) -> None:
    _plot_covid_lineas(
        context=context,
        medida="Sueldos y salarios",
        ylabel="% renta procedente de sueldos y salarios",
        titulo="Sueldos y salarios sobre renta total por isla — Canarias 2015-2023",
        fname="covid_sueldos_islas.png",
        cfg_key="covid_sueldos_islas",
    )


@asset(deps=[preprocesar_datos_p5], group_name="viz_desigualdad_y_renta")
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
    ISLAS_TURISTICAS = {"Lanzarote", "Fuerteventura", "Tenerife"}
    ISLAS_RESTO      = {"Gran Canaria", "La Palma", "La Gomera", "El Hierro"}

    sub_all = rentas[rentas["MEDIDAS"] == medida]
    y_min_d = float(sub_all["OBS_VALUE"].min())
    y_max_d = float(sub_all["OBS_VALUE"].max())

    fig, ax = plt.subplots(figsize=(13, 6))
    fig.patch.set_facecolor("white")

    ax.axvspan(2019.5, 2021.5, color="#fde8e8", alpha=0.45, zorder=0)
    ax.axvline(2020, color="#c0392b", lw=0.8, ls="--", alpha=0.5, zorder=1)

    handles = []

    for isla in sorted(ISLAS_RESTO):
        sub = rentas[(rentas["TERRITORIO"] == isla) &
                     (rentas["MEDIDAS"] == medida)].sort_values("TIME_PERIOD")
        ax.plot(sub["TIME_PERIOD"], sub["OBS_VALUE"],
                color=COLOR_RESTO_ISLAS, lw=1.0, alpha=0.5, zorder=2)
    handles.append(mpatches.Patch(color=COLOR_RESTO_ISLAS, alpha=0.6,
                                  label="Resto de islas"))


    colores = get_colores_isla()
    for isla in ["Tenerife", "Fuerteventura", "Lanzarote"]:
        sub = rentas[(rentas["TERRITORIO"] == isla) &
                     (rentas["MEDIDAS"] == medida)].sort_values("TIME_PERIOD")
        col = colores[isla]
        ax.plot(sub["TIME_PERIOD"], sub["OBS_VALUE"],
                color=col, lw=3.0, marker="o", markersize=6,
                alpha=1.0, zorder=4)
        val_ultimo = float(sub["OBS_VALUE"].iloc[-1]) if not sub.empty else 0
        handles.append(mpatches.Patch(color=col,
                                      label=f"{isla}  ({val_ultimo:.1f})"))

    ax.text(2020.15, y_max_d * 0.99,
            "COVID-19", fontsize=8.5, color="#c0392b",
            fontweight="bold", va="top")

    _aplicar_eje_y(ax, y_min_d, y_max_d, empezar_en_cero)

    ax.set_xlim(2014.8, 2023.2)
    ax.set_xticks(range(2015, 2024))
    ax.set_xticklabels(range(2015, 2024), fontsize=8.5)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.yaxis.grid(True, color="#eeeeee", zorder=0)
    ax.spines[["top", "right"]].set_visible(False)
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


@asset(deps=[preprocesar_datos_p5], group_name="viz_estructura_laboral")
def plot_brecha_temporal_edad(context: AssetExecutionContext) -> None:
    """
    Barras agrupadas H/M por tipo de contrato, facet por edad — Marzo 2026.
    Anotaciones reducidas: solo se etiqueta la barra más alta por panel
    (o todas si anotar_solo_mayor=false en config).
    """
    pal = get_paleta()

    cfg  = get_plot_config().get("brecha_temporal_edad", {})
    ISLA = cfg.get("isla", "Todas")
    SOLO_MAYOR = cfg.get("anotar_solo_mayor", True)
    AÑO = cfg.get("ano", 2026)
    MES = cfg.get("mes", 3)

    df = _cargar_contratos(os.path.join(config.TARGET_DIR, config.DATA_P5_DIR), AÑO, MES)
    if df is None:
        context.log.warning(f"No hay datos de contratos para {AÑO}-{MES:02d}.")
        return

    df.columns = df.columns.str.strip()
    df["Contratos"] = df["c"]
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
    df = df[df["sexo"].isin(["Hombres", "Mujeres"])]

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
    
    # Ordenar tipos de contrato por volumen total (magnitud)
    orden_tc = (
        df.groupby("tc")["Contratos"].sum()
        .sort_values(ascending=False).index.tolist()
    )
    TC_LABEL_ORDER = [t for t in orden_tc if t in ["Temp. Parcial", "Temp. Completo", "Conversión", "Indefinido"]]
    
    COLORS = {"Hombres": pal["H"], "Mujeres": pal["M"]}

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
        all_vals = {}
        for sexo, offset in [("Hombres", -w/2), ("Mujeres", w/2)]:
            vals = [
                float(agg[(agg["edad"] == edad) & (agg["sexo"] == sexo) &
                           (agg["tc"] == tc)]["pct"].values[0])
                if len(agg[(agg["edad"] == edad) & (agg["sexo"] == sexo) &
                            (agg["tc"] == tc)]) > 0 else 0.0
                for tc in TC_LABEL_ORDER
            ]
            ax.bar(x + offset, vals, w, color=COLORS[sexo], alpha=0.85, zorder=3)
            all_vals[sexo] = vals

        # Anotaciones: solo la barra más alta por panel, o todas si SOLO_MAYOR=False
        for sexo, offset in [("Hombres", -w/2), ("Mujeres", w/2)]:
            vals = all_vals[sexo]
            max_v = max(vals)
            for xi, (tc, v) in enumerate(zip(TC_LABEL_ORDER, vals)):
                if v < 4:
                    continue
                if SOLO_MAYOR and v < max_v:
                    continue
                ax.text(xi + offset, v + 0.4, f"{v:.0f}%",
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

    mes_names = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
        7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
    }
    title_date = f"{mes_names.get(MES, f'{MES:02d}')} {AÑO}"

    titulo_loc = "Canarias" if ISLA == "Todas" else ISLA
    fig.suptitle(
        f"Distribución del tipo de contrato por edad y género — "
        f"{titulo_loc}, {title_date}",
        fontsize=13, fontweight="bold")

    handles = [mpatches.Patch(color=c, label=s, alpha=0.85)
               for s, c in COLORS.items()]
    fig.legend(handles=handles, loc="lower center", ncol=2,
               fontsize=9, frameon=False, bbox_to_anchor=(0.5, -0.04))
    fig.text(0.99, -0.07,
             f"Fuente: SEPE / OBECAN · Contratos {title_date.lower()}",
             ha="right", fontsize=8, color="#888888")

    plt.tight_layout(rect=[0, 0.0, 1, 0.96])
    out = os.path.join(get_plot_dir(), "brecha_temporal_parcial_edad.png")
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    context.add_output_metadata(
        {"plot": MetadataValue.md(f"![Brecha Edad]({out})")})


@asset(deps=[preprocesar_datos_p5], group_name="viz_estructura_laboral")
def plot_historico_tipos_contrato_por_edad(context: AssetExecutionContext) -> None:
    """
    4 figuras individuales (una por tipo de contrato) con evolución histórica
    2019-2026 del % sobre total, por franja de edad y género.
    Anotación de la reforma laboral (dic 2021).
    """
    pal = get_paleta()

    def detect_sep(p):
        import csv as _csv
        with open(p, "r", encoding="utf-8", errors="ignore") as f:
            sample = f.read(4096)
        try:
            dialect = _csv.Sniffer().sniff(sample, delimiters=",;")
            return dialect.delimiter
        except Exception:
            return "," if sample.count(",") >= sample.count(";") else ";"

    data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)

    cfg = get_plot_config().get("historico_tipos_contrato", {})
    AÑO_FIN = cfg.get("ano_fin", 2026)
    MES_FIN = cfg.get("mes_fin", 3)

    FUENTES = []
    for año in range(2019, AÑO_FIN):
        if año in (2019, 2020, 2021, 2022):
            paths = [os.path.join(data_dir, f"contratos{año}.csv")]
            col_c = "contratos"
        else:
            paths = sorted(glob.glob(os.path.join(data_dir, str(año), "contratos_*.csv")))
            if año == 2025:
                paths = sorted(glob.glob(os.path.join(data_dir, "2025", "contratos_202*.csv")))
            col_c = "Contratos"
        FUENTES.append((año, paths, col_c))

    df_latest = _cargar_contratos(data_dir, AÑO_FIN, MES_FIN)
    if df_latest is None:
        context.log.warning(f"No hay datos de contratos para el último periodo {AÑO_FIN}-{MES_FIN:02d}.")
        return

    df_latest = df_latest.rename(columns={"Contratos": "c"})
    df_latest["año"] = AÑO_FIN

    all_dfs = []
    for año, paths, col_c in FUENTES:
        if not paths:
            context.log.warning(f"Sin archivos para el año {año}")
            continue
        dfs = []
        for p in paths:
            if not os.path.exists(p):
                context.log.warning(f"Archivo no encontrado: {p}")
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
        else:
            context.log.warning(f"Ningún archivo válido para el año {año}")

    all_dfs.append(df_latest)
    df_hist = pd.concat(all_dfs, ignore_index=True)

    TC_MAP = {
        "Indefinido":               "Indefinido",
        "Temporal Tiempo Completo": "Temp. Completo",
        "Temporal Tiempo Parcial":  "Temp. Parcial",
        "Conversión a Indefinido":  "Conversión",
    }
    TC_TITLE = {
        "Temp. Parcial":  "Temporal tiempo parcial",
        "Temp. Completo": "Temporal tiempo completo",
        "Conversión":     "Conversión a indefinido",
        "Indefinido":     "Contrato indefinido",
    }
    TC_ORDER = ["Temp. Parcial", "Temp. Completo", "Conversión", "Indefinido"]

    EDADES   = ["Menor de 25", "Entre 25 y 44", "45 o más"]
    COLORS   = {"Hombres": pal["H"], "Mujeres": pal["M"]}

    df_hist["tc"] = df_hist["Tipo Contrato"].str.strip().map(TC_MAP)
    df_hist = df_hist.dropna(subset=["tc", "edad"])
    df_hist = df_hist[df_hist["edad"].isin(EDADES)]

    # Derivar AÑOS desde los datos (no hardcodeado)
    AÑOS = sorted(df_hist["año"].unique())

    total = (df_hist.groupby(["año", "edad", "sexo"])["c"]
             .sum().reset_index(name="total"))
    agg   = df_hist.groupby(["año", "edad", "sexo", "tc"])["c"].sum().reset_index()
    agg   = agg.merge(total, on=["año", "edad", "sexo"])
    agg["pct"] = agg["c"] / agg["total"] * 100

    output_paths = []

    for tc_name in TC_MAP.values():
        fig, axes = plt.subplots(1, len(EDADES), figsize=(14, 6.2),
                                 sharey=True, sharex=True)
        fig.patch.set_facecolor("white")

        for col, edad in enumerate(EDADES):
            ax = axes[col]
            ax.set_facecolor("white")

            for sexo in ["Hombres", "Mujeres"]:
                sub  = agg[(agg["sexo"] == sexo) &
                           (agg["edad"] == edad) &
                           (agg["tc"] == tc_name)].set_index("año")
                vals = [sub.loc[a, "pct"] if a in sub.index else np.nan for a in AÑOS]

                # Trazar línea continua hasta el penúltimo año
                xs = [i for i, a in enumerate(AÑOS)
                      if a <= (AÑO_FIN - 1) and not np.isnan(vals[i])]
                ys = [vals[i] for i in xs]
                ax.plot(xs, ys, color=COLORS[sexo], lw=2.2)

                # Proyección al último año (dato parcial) con línea punteada y diamante
                if AÑO_FIN in AÑOS and (AÑO_FIN - 1) in AÑOS:
                    x_prev = AÑOS.index(AÑO_FIN - 1)
                    x_curr = AÑOS.index(AÑO_FIN)
                    y_prev = vals[x_prev]
                    y_curr = vals[x_curr]
                    if not np.isnan(y_prev) and not np.isnan(y_curr):
                        # Línea discontinua de proyección
                        ax.plot([x_prev, x_curr], [y_prev, y_curr], color=COLORS[sexo], lw=2.0, linestyle=":")
                        # Diamante
                        ax.scatter(x_curr, y_curr, marker="D", s=45, color=COLORS[sexo],
                                   edgecolors="white", linewidths=0.6, zorder=5)

            # Línea discontinua vertical para la Reforma Laboral en 2022
            if 2022 in AÑOS:
                idx_2022 = AÑOS.index(2022)
                ax.axvline(x=idx_2022, color="#7f8c8d", linestyle="--", alpha=0.7, lw=1.2, zorder=1)
                ax.text(idx_2022 - 0.08, 0.95, "Reforma Laboral",
                        transform=ax.get_xaxis_transform(),
                        rotation=90, ha="right", va="top", color="#7f8c8d", fontsize=7.5, style="italic")

            mes_abbr = {
                1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
                7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
            }
            ax.set_title(edad, fontsize=11, fontweight="bold", pad=8, color="#333333")
            ax.set_xticks(range(len(AÑOS)))
            label_fin = f"{mes_abbr.get(MES_FIN, f'{MES_FIN:02d}')}\n{AÑO_FIN}"
            ax.set_xticklabels(
                [str(a) if a != AÑO_FIN else label_fin for a in AÑOS],
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
        legend_label_fin = f"{mes_abbr.get(MES_FIN, f'{MES_FIN:02d}')} {AÑO_FIN} (dato parcial)"
        handles += [plt.scatter([], [], marker="D", color="#aaaaaa",
                                s=40, alpha=0.6, label=legend_label_fin)]
        fig.legend(handles=handles, loc="lower center", ncol=3,
                   fontsize=10, frameon=False, bbox_to_anchor=(0.5, 0.02))

        fig.suptitle(f"{TC_TITLE[tc_name]} — Canarias 2019-{AÑO_FIN}",
                     fontsize=13, fontweight="bold", y=0.965)
        fig.text(0.99, 0.015, "Fuente: OBECAN / SEPE",
                 ha="right", fontsize=8, color="#888888")

        plt.tight_layout(rect=[0, 0.12, 1, 0.93])

        safe = tc_name.lower().replace(". ", "_").replace(" ", "_")
        safe = "".join(c for c in unicodedata.normalize("NFD", safe) if unicodedata.category(c) != "Mn")
        out  = os.path.join(get_plot_dir(), f"historico_{safe}.png")
        fig.savefig(out, dpi=150)
        plt.close(fig)
        output_paths.append(out)
        context.log.info(f"✓ {out}")

    context.add_output_metadata({
        "plots": MetadataValue.md(
            "\n".join(f"- `{os.path.basename(p)}`" for p in output_paths))
    })


# ══════════════════════════════════════════════════════════════════════════════
# ASSETS CONTRATOS HISTÓRICOS (TODA CANARIAS)
# ══════════════════════════════════════════════════════════════════════════════

# ── Constantes Canarias ───────────────────────────────────────────────────────

ISLAS_ORDEN_CANARIAS = ["El Hierro", "La Gomera", "La Palma", "Tenerife",
                        "Gran Canaria", "Lanzarote", "Fuerteventura"]
ISLAS_VALIDAS = set(ISLAS_ORDEN_CANARIAS)

PROVINCIAS = {
    "SC Tenerife": {"Tenerife", "La Palma", "La Gomera", "El Hierro"},
    "Las Palmas":  {"Gran Canaria", "Lanzarote", "Fuerteventura"},
}

CNO_GRUPOS = {
    1: "Directores y gerentes",
    2: "Técnicos y científicos",
    3: "Técnicos de apoyo",
    4: "Administrativos",
    5: "Servicios y comercio",
    6: "Trabajadores agrarios",
    7: "Artesanos e industria",
    8: "Operadores de maquinaria",
    9: "Ocupaciones elementales",
}

# ── Helpers Canarias ──────────────────────────────────────────────────────────

def _detect_sep(p: str) -> str:
    with open(p, "r", encoding="utf-8", errors="ignore") as f:
        l = f.readline()
    return ";" if l.count(";") > l.count(",") else ","


def _fix_articulo(s: str) -> str:
    """'Orotava, La' → 'La Orotava'. Coherente con preprocesar_datos_p5."""
    m = re.match(r"^(.+),\s*(La|El|Los|Las)$", str(s).strip(), re.IGNORECASE)
    return f"{m.group(2)} {m.group(1)}" if m else s.strip()


def _cno_grupo(val) -> int | None:
    """
    Extrae grupo CNO (primer dígito) independientemente del formato:
      2019-2022: '5.5', '9.432'  →  5, 9
      2023+:     '5120', '9310'  →  5, 9
    """
    s = str(val).strip().replace(".", "")
    return int(s[0]) if s and s[0].isdigit() else None


def _filtrar_ambito(df: pd.DataFrame, ambito: str) -> pd.DataFrame:
    """
    Filtra según el ámbito configurado en el YAML.
    Valores válidos:
      "Canarias"    → todas las islas
      "SC Tenerife" → Tenerife, La Palma, La Gomera, El Hierro
      "Las Palmas"  → Gran Canaria, Lanzarote, Fuerteventura
      nombre isla   → solo esa isla
    """
    if ambito == "Canarias":
        return df[df["isla"].isin(ISLAS_VALIDAS)].copy()
    elif ambito in PROVINCIAS:
        return df[df["isla"].isin(PROVINCIAS[ambito])].copy()
    elif ambito in ISLAS_VALIDAS:
        return df[df["isla"] == ambito].copy()
    else:
        opts = ["Canarias", "SC Tenerife", "Las Palmas"] + sorted(ISLAS_VALIDAS)
        raise ValueError(
            f"Ámbito no reconocido: {ambito!r}. "
            f"Opciones válidas en plot_config.yaml: {opts}"
        )


def _titulo_ambito(ambito: str) -> str:
    return {"SC Tenerife": "Prov. SC Tenerife",
            "Las Palmas":  "Prov. Las Palmas"}.get(ambito, ambito)


def _islas_en_ambito(ambito: str) -> list:
    if ambito == "Canarias":
        return ISLAS_ORDEN_CANARIAS
    elif ambito in PROVINCIAS:
        return [i for i in ISLAS_ORDEN_CANARIAS if i in PROVINCIAS[ambito]]
    else:
        return [ambito]


def _cargar_contratos(data_dir: str, año: int, mes: int = None) -> pd.DataFrame | None:
    """
    Carga contratos de un año (y opcionalmente de un mes), normaliza columnas,
    infiere isla para ficheros 2019-2022 que no la traen, y calcula grupo CNO-1.
    """
    from checks_p5 import inferir_isla

    processed_paths = get_processed_contratos(año, mes)
    if not processed_paths:
        return None

    dfs = []
    for p in processed_paths:
        with open(p, "r", encoding="utf-8", errors="ignore") as f:
            first_line = f.readline()
        sep = ";" if first_line.count(";") > first_line.count(",") else ","
        df = pd.read_csv(p, sep=sep)
        df.columns = df.columns.str.strip()
        
        # Determinar columna de contratos y renombrar a 'c' cleanly
        col_c = "contratos" if año in (2019, 2020, 2021, 2022) else "Contratos"
        if col_c in df.columns:
            df = df.rename(columns={col_c: "c"})
            df["c"] = df["c"].astype(float)
            
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].str.strip()
            
        # Corregir artículos antes de inferir isla
        if "Municipio" in df.columns:
            df["Municipio"] = df["Municipio"].apply(_fix_articulo)
        dfs.append(df[df["sexo"].isin(["Hombres", "Mujeres"])])

    if not dfs:
        return None

    df_año = pd.concat(dfs, ignore_index=True)
    df_año["año"] = año

    # Inferir isla si no existe (ficheros 2019-2022)
    if "isla" not in df_año.columns:
        df_año["isla"] = df_año["Municipio"].apply(inferir_isla)
    else:
        df_año["isla"] = df_año["isla"].str.strip().str.title()

    # Grupo CNO-1
    if "CNO11" in df_año.columns:
        df_año["grupo_cno"] = (
            df_año["CNO11"].apply(_cno_grupo).map(CNO_GRUPOS)
        )

    return df_año


def _tema_base(fig_size):
    return theme_minimal() + theme(
        figure_size=fig_size,
        plot_title=element_text(size=13, face="bold"),
        plot_subtitle=element_text(size=10, color="#555555"),
        axis_text_y=element_text(size=9),
        panel_grid_major_x=element_line(color="#dddddd", size=0.4),
        panel_grid_major_y=element_blank(),
        legend_position="bottom",
    )


# ── Assets ────────────────────────────────────────────────────────────────────

@asset(deps=[preprocesar_datos_p5], group_name="viz_estructura_laboral")
def plot_ocupacion_divergente_canarias(context: AssetExecutionContext) -> None:
    """
    Barras divergentes H-M por grupo CNO-1, filtradas según el ámbito
    configurado en plot_config.yaml.
    """
    cfg    = get_plot_config()["ocupacion_divergente_canarias"]
    AÑO    = cfg.get("ano", 2025)
    AMBITO = cfg.get("ambito", "Canarias")
    MIN_C  = cfg.get("min_contratos", 100)
    AGREGAR = cfg.get("agregar_islas", False)

    data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
    df = _cargar_contratos(data_dir, AÑO)
    if df is None or "grupo_cno" not in df.columns:
        context.log.warning(f"No hay datos CNO para {AÑO}.")
        return

    df = _filtrar_ambito(df, AMBITO)
    df = df.dropna(subset=["grupo_cno"])
    context.log.info(
        f"Ámbito: {AMBITO} → {df['isla'].nunique()} isla(s), "
        f"{df['c'].sum():,.0f} contratos"
    )

    titulo = (f"Brecha de género por ocupación — "
              f"{_titulo_ambito(AMBITO)}, {AÑO}")

    es_isla_unica = AMBITO in ISLAS_VALIDAS

    # ── Modo agregado: toda Canarias en un único gráfico sin facet ────────────
    if AGREGAR or es_isla_unica:
        pivot = (
            df.groupby(["grupo_cno","sexo"])["c"]
            .sum().unstack("sexo").fillna(0).reset_index()
        )
        pivot.columns.name = None
        total_can          = df["c"].sum()
        pivot["brecha"]    = (
            (pivot.get("Hombres",0) - pivot.get("Mujeres",0))
            / total_can * 100
        )
        pivot["total"]     = pivot.get("Hombres",0) + pivot.get("Mujeres",0)
        pivot              = pivot[pivot["total"] >= MIN_C]
        pivot["direccion"] = pivot["brecha"].apply(
            lambda x: "Mayoría Hombres" if x > 0 else "Mayoría Mujeres")

        subtitulo = (
            "Diferencia (H − M) como % del total de contratos · grupos CNO-1"
            if es_isla_unica else
            "Diferencia (H − M) como % del total de contratos de Canarias · grupos CNO-1"
        )

        p = (
            ggplot(pivot,
                   aes(x="reorder(grupo_cno, brecha)", y="brecha",
                       fill="direccion"))
            + geom_col(width=0.65, alpha=0.9)
            + geom_hline(yintercept=0, linetype="dashed",
                         color="#333333", size=0.5)
            + scale_fill_manual(
                values={"Mayoría Hombres":"#4A90D9",
                        "Mayoría Mujeres":"#D94A8C"}, name=None)
            + scale_y_continuous(labels=lambda l: [f"{v:+.1f}%" for v in l])
            + coord_flip()
            + labs(title=titulo,
                   subtitle=subtitulo,
                   x="", y="% sobre total contratos (Hombres − Mujeres)",
                   caption="Fuente: OBECAN / SEPE")
            + _tema_base((11, 6))
        )
        fig_w, fig_h = 11, 6

    else:
        # ── Modo facet: una columna por isla ──────────────────────────────────
        islas_ambito = _islas_en_ambito(AMBITO)
        # Total por isla para normalizar
        total_isla = df[df["isla"].isin(ISLAS_VALIDAS)].groupby("isla")["c"].sum()

        pivot = (
            df.groupby(["isla","grupo_cno","sexo"])["c"]
            .sum().unstack("sexo").fillna(0).reset_index()
        )
        pivot.columns.name = None
        pivot["total_isla"] = pivot["isla"].map(total_isla)
        pivot["brecha"]     = (
            (pivot.get("Hombres",0) - pivot.get("Mujeres",0))
            / pivot["total_isla"] * 100
        )
        pivot["total"]      = pivot.get("Hombres",0) + pivot.get("Mujeres",0)
        pivot               = pivot[pivot["total"] >= MIN_C]
        pivot["direccion"]  = pivot["brecha"].apply(
            lambda x: "Mayoría Hombres" if x > 0 else "Mayoría Mujeres")

        # Orden global: coherencia visual entre islas
        orden = (pivot.groupby("grupo_cno")["brecha"].sum()
                 .sort_values().index.tolist())
        pivot["grupo_cno"] = pd.Categorical(
            pivot["grupo_cno"], categories=orden, ordered=True)
        pivot["isla"] = pd.Categorical(
            pivot["isla"], categories=islas_ambito, ordered=True)

        ncol = min(len(islas_ambito), 4)
        p = (
            ggplot(pivot,
                   aes(x="grupo_cno", y="brecha", fill="direccion"))
            + geom_col(width=0.65, alpha=0.9)
            + geom_hline(yintercept=0, linetype="dashed",
                         color="#333333", size=0.5)
            + facet_wrap("~ isla", scales="fixed", ncol=ncol)
            + scale_fill_manual(
                values={"Mayoría Hombres":"#4A90D9",
                        "Mayoría Mujeres":"#D94A8C"}, name=None)
            + scale_y_continuous(labels=lambda l: [f"{v:+.1f}%" for v in l])
            + coord_flip()
            + labs(title=titulo,
                   subtitle="Diferencia (H − M) como % del total de contratos de cada isla · grupos CNO-1 · escala compartida",
                   x="", y="% sobre total contratos (Hombres − Mujeres)",
                   caption="Fuente: OBECAN / SEPE")
            + _tema_base((16, 10))
            + theme(strip_text=element_text(size=9, face="bold"))
        )
        fig_w, fig_h = 16, 10

    out = os.path.join(get_plot_dir(), "ocupacion_divergente_canarias.png")
    p.save(out, width=fig_w, height=fig_h, dpi=150, verbose=False)
    context.add_output_metadata({
        "ambito": MetadataValue.text(AMBITO),
        "año":    MetadataValue.int(AÑO),
        "plot":   MetadataValue.md(f"![Ocupación {AMBITO}]({out})"),
    })