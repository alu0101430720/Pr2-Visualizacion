import os
import yaml
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
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

@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_distribucion_lineas(context: AssetExecutionContext) -> None:
    cfg = get_plot_config()["distribucion_lineas"]
    df = pd.read_csv(get_processed_path(cfg["dataset"])).dropna(subset=["OBS_VALUE"])
    
    LABELS = {
        "OTRAS_PRESTACIONES":     "Otras prestaciones",
        "OTROS_INGRESOS":         "Otros ingresos",
        "PENSIONES":              "Pensiones",
        "PRESTACIONES_DESEMPLEO": "Prest. desempleo",
        "SUELDOS_SALARIOS":       "Sueldos y salarios",
    }
    df["componente"] = df["MEDIDAS_CODE"].map(LABELS)

    agg = (
        df.groupby(["año", "componente"])["OBS_VALUE"]
        .agg(
            mediana="median",
            q25=lambda x: x.quantile(0.25),
            q75=lambda x: x.quantile(0.75),
        )
        .reset_index()
    )

    p = (
        ggplot(agg, aes(x="año", y="mediana", color="componente", fill="componente", group="componente"))
        + geom_ribbon(aes(ymin="q25", ymax="q75"), alpha=0.12, color=None)
        + geom_line(size=1.2)
        + geom_point(size=3, stroke=0.4)
        + scale_x_continuous(breaks=cfg["x_breaks"])
        + scale_color_brewer(type="qual", palette="Set2", name="Componente")
        + scale_fill_brewer(type="qual", palette="Set2", guide=None)
        + labs(
            title="Evolución de la distribución de ingresos — Tenerife",
            subtitle="Mediana por sección censal · banda IQR (Q25–Q75)",
            x="Año",
            y="% sobre renta total",
            caption="Fuente: ISTAC",
        )
        + theme_minimal()
        + theme(
            figure_size=(12, 6),
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            panel_grid_minor=element_blank(),
            panel_grid_major_x=element_blank(),
            legend_position="bottom",
            legend_title=element_blank(),
        )
    )

    out_path = os.path.join(get_plot_dir(), "distribucion_lineas.png")
    p.save(out_path, width=12, height=6, dpi=150, verbose=False)
    
    context.add_output_metadata({"plot": MetadataValue.md(f"![Lineas Distribucion]({out_path})")})

@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_actividad_barras(context: AssetExecutionContext) -> None:
    cfg = get_plot_config()["actividad_barras"]
    df = pd.read_csv(get_processed_path(cfg["dataset"])).dropna(subset=["num_casos"])
    df = df[df["Sexo"].isin(["Hombres", "Mujeres"])]

    df["actividad"] = df["Actividad económica"].replace(
        {"Agricultura, ganadería y pesca": "Agricultura/\nGanadería"}
    )

    agg = df.groupby(["Periodo", "actividad", "Sexo"], as_index=False)["num_casos"].sum()

    p = (
        ggplot(agg, aes(x="factor(Periodo)", y="num_casos", fill="Sexo"))
        + geom_col(position="stack", width=0.7, alpha=0.9)
        + facet_wrap("~ actividad", scales="free_y", ncol=3)
        + scale_fill_manual(values={"Hombres": "#2B6CB0", "Mujeres": "#D53F8C"})
        + scale_y_continuous(labels=fmt_k)
        + labs(
            title="Actividad económica por año y sexo — Tenerife",
            subtitle="Suma de trabajadores por sección censal",
            x="Año", y="Nº trabajadores", fill="Sexo",
            caption="Fuente: ISTAC",
        )
        + theme_minimal()
        + theme(
            figure_size=(14, 8),
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            strip_text=element_text(size=9, face="bold"),
            panel_grid_major_x=element_blank(),
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
        + scale_fill_manual(values={"Mayoría Hombres": "#2B6CB0", "Mayoría Mujeres": "#D53F8C"})
        + scale_y_continuous(labels=fmt_k)
        + coord_flip()
        + labs(
            title="Brecha de género por ocupación — Tenerife",
            subtitle="Diferencia acumulada (Hombres − Mujeres)",
            x=None, y="Diferencia (personas)", fill=None,
            caption="Fuente: ISTAC",
        )
        + theme_minimal()
        + theme(
            figure_size=(12, 6),
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            panel_grid_major_y=element_blank(),
            legend_position="bottom",
        )
    )

    out_path = os.path.join(get_plot_dir(), "ocupacion_divergente.png")
    p.save(out_path, width=12, height=6, dpi=150, verbose=False)
    context.add_output_metadata({"plot": MetadataValue.md(f"![Ocupacion Divergente]({out_path})")})

@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_renta_cajas(context: AssetExecutionContext) -> None:
    cfg = get_plot_config()["renta_cajas"]
    df = pd.read_csv(get_processed_path(cfg["dataset"])).dropna(subset=["OBS_VALUE"])
    medida = cfg["medida"]

    rent = df[df["MEDIDAS_CODE"] == medida].copy()
    top15 = rent.groupby("municipio")["OBS_VALUE"].median().nlargest(15).index.tolist()
    rent15 = rent[rent["municipio"].isin(top15)].copy()
    orden = rent15.groupby("municipio")["OBS_VALUE"].median().sort_values(ascending=False).index.tolist()
    rent15["municipio"] = pd.Categorical(rent15["municipio"], categories=orden, ordered=True)

    PALETTE_YEAR = {2021: "#a8dadc", 2022: "#457b9d", 2023: "#1d3557"}

    p = (
        ggplot(rent15, aes(x="municipio", y="OBS_VALUE"))
        + geom_boxplot(fill="#457b9d", color="#1d3557", alpha=0.7, outlier_alpha=0)
        + geom_jitter(aes(color="factor(año)"), width=0.15, size=1.5, alpha=0.6)
        + scale_color_manual(values=list(PALETTE_YEAR.values()), name="Año")
        + scale_y_continuous(labels=fmt_k)
        + coord_flip()
        + labs(
            title=f"{medida.replace('_', ' ').title()} — Top 15 municipios Tenerife",
            subtitle="Distribución por sección censal · puntos = valores anuales individuales",
            x=None, y="€ / año",
            caption="Fuente: ISTAC",
        )
        + theme_minimal()
        + theme(
            figure_size=(12, 7),
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            axis_text_y=element_text(size=9),
            legend_position="right",
        )
    )

    out_path = os.path.join(get_plot_dir(), f"renta_{medida.lower()}.png")
    p.save(out_path, width=12, height=7, dpi=150, verbose=False)
    context.add_output_metadata({"plot": MetadataValue.md(f"![Renta Cajas]({out_path})")})

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
    df_fil = df[(df["año"] == año) & (df["MEDIDAS_CODE"] == componente)][["TERRITORIO_CODE", "OBS_VALUE"]]

    try:
        gdf = gpd.read_file(get_geojson_path(geojson_name)).set_crs("EPSG:4326", allow_override=True)
    except Exception as e:
        context.log.warning(f"GeoJSON not found: {geojson_name}")
        return
        
    # Arreglo de cruce para sortear desajustes de prefijos de año (ej: 2024 vs 2023 en geocode)
    gdf["sec_code"] = gdf["geocode"].apply(lambda x: "_".join(x.split("_")[1:]) if pd.notna(x) else x)
    df_fil["sec_code"] = df_fil["TERRITORIO_CODE"].apply(lambda x: "_".join(x.split("_")[1:]) if pd.notna(x) else x)
    
    gdf = gdf.merge(df_fil, on="sec_code", how="left")

    fig, ax = plt.subplots(figsize=(14, 10))

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
    ax.annotate("Por sección censal · Fuente: ISTAC", xy=(0.01, 0.98), xycoords="axes fraction", fontsize=9, color="#555555", va="top")
    ax.axis("off")
    fig.tight_layout()

    out_path = os.path.join(get_plot_dir(), f"mapa_{componente.lower()}_{año}.png")
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    
    context.add_output_metadata({"plot": MetadataValue.md(f"![Mapa Distribucion]({out_path})")})

@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_mapa_generico(context: AssetExecutionContext) -> None:
    cfg = get_plot_config()["mapa_generico"]
    dataset_name = cfg["dataset"]
    año = cfg["ano"]
    geojson_name = f"secciones_{año}0101_tenerife.json"
    
    filtro_medida = cfg.get("filtro_medida")
    filtro_actividad = cfg.get("filtro_actividad")
    filtro_ocupacion = cfg.get("filtro_ocupacion")
    filtro_sexo = cfg.get("filtro_sexo")
    cmap = cfg.get("cmap", "YlOrRd")

    df = pd.read_csv(get_processed_path(dataset_name))
    cols = df.columns.tolist()

    if "MEDIDAS_CODE" in cols and "OBS_VALUE" in cols and "TERRITORIO_CODE" in cols:
        col_año, col_geo, col_val = "año", "TERRITORIO_CODE", "OBS_VALUE"
        mask = df[col_año] == año
        if filtro_medida:
            mask &= df["MEDIDAS_CODE"] == filtro_medida
        label_val = filtro_medida or "OBS_VALUE"
        df_datos = df[mask][[col_geo, col_val]].copy()

    elif "Actividad económica" in cols:
        col_año, col_geo, col_val = "Periodo", "geocode", "num_casos"
        mask = df[col_año] == año
        if filtro_actividad:
            mask &= df["Actividad económica"] == filtro_actividad
        if filtro_sexo:
            mask &= df["Sexo"] == filtro_sexo
        label_val = f"{filtro_actividad or 'Todas'} · {filtro_sexo or 'Ambos sexos'}"
        df_datos = df[mask].groupby(col_geo)[col_val].sum().reset_index()

    elif "ocupacion" in cols:
        col_año, col_geo, col_val = "año", "geocode", "num_casos"
        mask = df[col_año] == año
        if filtro_ocupacion:
            mask &= df["ocupacion"] == filtro_ocupacion
        if filtro_sexo:
            mask &= df["sexo"] == filtro_sexo
        label_val = f"{filtro_ocupacion or 'Todas'} · {filtro_sexo or 'Ambos sexos'}"
        df_datos = df[mask].groupby(col_geo)[col_val].sum().reset_index()
    else:
        raise ValueError(f"Estructura de dataset no reconocida para el mapa genérico.")

    try:
        gdf = gpd.read_file(get_geojson_path(geojson_name)).set_crs("EPSG:4326", allow_override=True)
    except Exception as e:
        context.log.warning(f"No se detectó un GeoJSON ({geojson_name}). Asegurese de que reside en data-P5/cartografia-secciones/")
        return
        
    gdf["sec_code"] = gdf["geocode"].apply(lambda x: "_".join(str(x).split("_")[1:]) if pd.notna(x) else x)
    df_datos["sec_code"] = df_datos[col_geo].apply(lambda x: "_".join(str(x).split("_")[1:]) if pd.notna(x) else x)
    
    gdf = gdf.merge(df_datos, on="sec_code", how="left")

    fig, ax = plt.subplots(figsize=(14, 10))
    # Para geopandas plots muy finos
    gdf.plot(
        column=col_val,
        cmap=cmap,
        linewidth=0.08,
        edgecolor="white",
        legend=True,
        legend_kwds={"label": label_val, "orientation": "vertical", "shrink": 0.55, "pad": 0.01},
        missing_kwds={"color": "#dddddd", "label": "Sin datos"},
        ax=ax,
    )

    titulo = f"{label_val} — Tenerife {año}"
    ax.set_title(titulo, fontsize=14, fontweight="bold", pad=12)
    ax.annotate(
        f"Por sección censal · {dataset_name} · Fuente: ISTAC",
        xy=(0.01, 0.98), xycoords="axes fraction",
        fontsize=9, color="#555555", va="top",
    )
    ax.axis("off")
    fig.tight_layout()

    out_path = os.path.join(get_plot_dir(), f"mapa_{dataset_name.replace('.csv','').replace('-','_')}_{año}.png")
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    
    context.add_output_metadata({"plot": MetadataValue.md(f"![Mapa Geoespacial]({out_path})")})

@asset(deps=[preprocesar_datos_p5], group_name="visualizaciones")
def plot_brecha_salarial(context: AssetExecutionContext) -> None:
    cfg = get_plot_config()["brecha_salarial"]
    
    TOP_N     = cfg.get("top_n", 20)
    AÑO_INI   = cfg.get("ano_ini", 2021)
    AÑO_FIN   = cfg.get("ano_fin", 2023)
    UMBRAL    = cfg.get("umbral", 0.02)
    
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

    ini = merged[merged["año"] == AÑO_INI][["municipio", "indice_brecha"]].rename(columns={"indice_brecha": "brecha_ini"})
    fin = merged[merged["año"] == AÑO_FIN][["municipio", "indice_brecha"]].rename(columns={"indice_brecha": "brecha_fin"})
    
    slope = ini.merge(fin, on="municipio")
    slope["delta"] = slope["brecha_fin"] - slope["brecha_ini"]
    slope["direccion"] = slope["delta"].apply(
        lambda d: "Brecha aumenta" if d > UMBRAL else ("Brecha disminuye" if d < -UMBRAL else "Sin cambio relevante")
    )

    slope["brecha_media"] = (slope["brecha_ini"] + slope["brecha_fin"]) / 2
    top = slope.nlargest(TOP_N, "brecha_media")

    long = pd.concat([
        top.assign(año=AÑO_INI, brecha=top["brecha_ini"]),
        top.assign(año=AÑO_FIN, brecha=top["brecha_fin"]),
    ])

    mediana_global = long["brecha"].median()
    COLORES = {
        "Brecha aumenta":       "#C0392B",
        "Brecha disminuye":     "#2B6CB0",
        "Sin cambio relevante": "#AAAAAA",
    }

    p = (
        ggplot(long, aes(x="factor(año)", y="brecha", group="municipio", color="direccion"))
        + geom_hline(yintercept=mediana_global, linetype="dashed", color="#888888", size=0.5, alpha=0.7)
        + geom_line(size=0.9, alpha=0.8)
        + geom_point(size=2.5, stroke=0.3)
        + geom_text(
            data=long[long["año"] == AÑO_FIN],
            mapping=aes(label="municipio"),
            ha="left", size=7, nudge_x=0.05, color="#333333",
        )
        + scale_color_manual(values=COLORES, name=None)
        + scale_x_discrete(expand=(0, 0.4))
        + labs(
            title="Evolución de la brecha salarial de género por municipio",
            subtitle=f"Índice = ratio H/(H+M) × % sueldos sobre renta · Top {TOP_N} municipios · {AÑO_INI}→{AÑO_FIN}",
            x=None, y="Índice de brecha salarial ponderado",
            caption="Fuente: ISTAC · ocupacion-sc-3 + distribucion-renta-ingresos",
        )
        + theme_minimal()
        + theme(
            figure_size=(11, 9),
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=9, color="#555555"),
            panel_grid=element_blank(),
            axis_text_x=element_text(size=11, face="bold"),
            axis_text_y=element_text(size=8, color="#888888"),
            legend_position="bottom",
        )
    )

    out_path = os.path.join(get_plot_dir(), "brecha_salarial_slope.png")
    p.save(out_path, width=11, height=9, dpi=150, verbose=False)
    context.add_output_metadata({"plot": MetadataValue.md(f"![Brecha Salarial Slope]({out_path})")})
