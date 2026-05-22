import os
import glob
import re
import time
import pandas as pd
import geopandas as gpd
import matplotlib
import matplotlib.pyplot as plt
import config
from dagster import asset_check, AssetCheckResult, MetadataValue, AssetCheckSeverity
from assets import preprocesar_datos_p5, commitear_plots_a_github
from plots_assets import (
    plot_actividad_barras,
    plot_ocupacion_divergente,
    plot_brecha_salarial,
    plot_mapa_brecha_salarial,
    get_processed_path,
    get_geojson_path,
    get_plot_config,
    get_paleta,
    get_plot_dir,
    plot_gini_evolucion_islas,
    plot_heatmap_segregacion_sectorial,
    plot_covid_sueldos_islas,
    plot_covid_prestaciones_islas,
    plot_brecha_temporal_edad,
    plot_historico_tipos_contrato_por_edad,
    ISLAS_ORDEN,
    COLORES_ISLA,
    _load_rentas,
    _load_gini,
)

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTES COMPARTIDAS
# ══════════════════════════════════════════════════════════════════════════════

MUNICIPIOS_POR_ISLA = {
    "Tenerife": {
        "Adeje", "Arafo", "Arico", "Arona", "Buenavista del Norte", "Candelaria",
        "Fasnia", "Garachico", "Granadilla de Abona", "La Guancha", "Guía de Isora",
        "Güímar", "Icod de los Vinos", "La Matanza de Acentejo", "La Orotava",
        "Puerto de la Cruz", "Puerto de La Cruz", "Los Realejos", "El Rosario",
        "San Cristóbal de La Laguna", "San Juan de la Rambla", "San Miguel de Abona",
        "Santa Cruz de Tenerife", "Santa Úrsula", "Santiago del Teide", "El Sauzal",
        "Los Silos", "Tacoronte", "El Tanque", "Tegueste", "La Victoria de Acentejo",
        "Vilaflor", "Vilaflor de Chasna",
    },
    "Gran Canaria": {
        "Agaete", "Agüimes", "Artenara", "Arucas", "Firgas", "Gáldar", "Ingenio",
        "Mogán", "Moya", "Las Palmas de Gran Canaria", "San Bartolomé de Tirajana",
        "La Aldea de San Nicolás", "Santa Brígida", "Santa Lucía de Tirajana",
        "Santa María de Guía de Gran Canaria", "Tejeda", "Telde", "Teror", "Valleseco",
        "Valsequillo de Gran Canaria", "Vega de San Mateo",
    },
    "La Palma": {
        "Barlovento", "Breña Alta", "Breña Baja", "Fuencaliente de la Palma",
        "Fuencaliente de La Palma", "Garafía", "Los Llanos de Aridane", "El Paso",
        "Puntagorda", "Puntallana", "San Andrés y Sauces", "Santa Cruz de la Palma",
        "Santa Cruz de La Palma", "Tazacorte", "Tijarafe", "Villa de Mazo",
    },
    "Lanzarote":     {"Arrecife", "Haría", "San Bartolomé", "Teguise", "Tías", "Tinajo", "Yaiza"},
    "Fuerteventura": {"Antigua", "Betancuria", "La Oliva", "Pájara", "Puerto del Rosario", "Tuineje"},
    "La Gomera": {
        "Agulo", "Alajeró", "Hermigua", "San Sebastián de la Gomera",
        "San Sebastián de La Gomera", "Valle Gran Rey", "Vallehermoso",
    },
    "El Hierro": {
        "La Frontera", "Frontera", "El Pinar de El Hierro",
        "Pinar de El Hierro, El", "Valverde",
    },
}

CANONICOS_ISLA = {
    "Tenerife": {
        "Adeje", "Arafo", "Arico", "Arona", "Buenavista del Norte", "Candelaria",
        "Fasnia", "Garachico", "Granadilla de Abona", "La Guancha", "Guía de Isora",
        "Güímar", "Icod de los Vinos", "La Matanza de Acentejo", "La Orotava",
        "Puerto de la Cruz", "Los Realejos", "El Rosario", "San Cristóbal de La Laguna",
        "San Juan de la Rambla", "San Miguel de Abona", "Santa Cruz de Tenerife",
        "Santa Úrsula", "Santiago del Teide", "El Sauzal", "Los Silos", "Tacoronte",
        "El Tanque", "Tegueste", "La Victoria de Acentejo", "Vilaflor de Chasna",
    },
    "Gran Canaria": {
        "Agaete", "Agüimes", "Artenara", "Arucas", "Firgas", "Gáldar", "Ingenio",
        "Mogán", "Moya", "Las Palmas de Gran Canaria", "San Bartolomé de Tirajana",
        "La Aldea de San Nicolás", "Santa Brígida", "Santa Lucía de Tirajana",
        "Santa María de Guía de Gran Canaria", "Tejeda", "Telde", "Teror", "Valleseco",
        "Valsequillo de Gran Canaria", "Vega de San Mateo",
    },
    "La Palma": {
        "Barlovento", "Breña Alta", "Breña Baja", "Fuencaliente de la Palma", "Garafía",
        "Los Llanos de Aridane", "El Paso", "Puntagorda", "Puntallana",
        "San Andrés y Sauces", "Santa Cruz de la Palma", "Tazacorte", "Tijarafe",
        "Villa de Mazo",
    },
    "Lanzarote":     {"Arrecife", "Haría", "San Bartolomé", "Teguise", "Tías", "Tinajo", "Yaiza"},
    "Fuerteventura": {"Antigua", "Betancuria", "La Oliva", "Pájara", "Puerto del Rosario", "Tuineje"},
    "La Gomera": {
        "Agulo", "Alajeró", "Hermigua", "San Sebastián de la Gomera",
        "Valle Gran Rey", "Vallehermoso",
    },
    "El Hierro": {"La Frontera", "El Pinar de El Hierro", "Valverde"},
}

ESPERADOS_ISLAS = {
    "Tenerife": 31, "Gran Canaria": 21, "La Palma": 14,
    "Lanzarote": 7, "Fuerteventura": 6, "La Gomera": 6, "El Hierro": 3,
}

ISLAS_SC          = {"Tenerife", "La Palma", "La Gomera", "El Hierro"}
MUNICIPIOS_TENERIFE = CANONICOS_ISLA["Tenerife"]
AÑOS_ESPERADOS    = {2021, 2022, 2023}
SEXOS_ESPERADOS   = {"Hombres", "Mujeres"}
COMPONENTES_DIST  = {
    "OTRAS_PRESTACIONES", "OTROS_INGRESOS", "PENSIONES",
    "PRESTACIONES_DESEMPLEO", "SUELDOS_SALARIOS",
}
MEDIDAS_RENTA = {
    "RENTA_BRUTA_MEDIA_HOGAR", "RENTA_BRUTA_MEDIA_PERSONA",
    "RENTA_NETA_MEDIA_HOGAR",  "RENTA_NETA_MEDIA_PERSONA",
    "RENTA_NETA_UNIDAD_CONSUMO_MEDIA", "RENTA_NETA_UNIDAD_CONSUMO_MEDIANA",
}
MAX_CATEGORIAS = 9
MAX_LABEL      = 35
DOMINANCE_MAX  = 0.80
RATIO_MAX      = 5.0
MIN_KB_PLOT    = 50
MAX_AGE_S      = 1800   # 30 min

# Límites de longitud por contexto de uso
MAX_CHARS_TITULO        = 80   # título de figura (suptitle / set_title)
MAX_CHARS_EJE_Y_FLIP    = 40   # etiquetas en eje Y de coord_flip (ocupación, municipio)
MAX_CHARS_FACET_LABEL   = 30   # etiqueta de panel en facet_wrap
MAX_CHARS_ANOTACION_MAPA = 20  # nombre de municipio anotado sobre el mapa

# Nombres canónicos de los PNG que deben existir tras la ejecución
PNG_ESPERADOS = [
    "actividad_barras.png",
    "ocupacion_divergente.png",
    "brecha_salarial_lollipop.png",       # era slope, ahora lollipop
    "mapa_brecha_salarial.png",
    "gini_evolucion_islas.png",
    "heatmap_segregacion_sectorial.png",
    "covid_sueldos_islas.png",
    "covid_prestaciones_islas.png",
    "brecha_temporal_parcial_edad.png",
    "historico_tipos_contrato_2x2.png",   # era 4 ficheros, ahora small multiple único
]

# Añadir el mapa de distribución de renta dinámicamente (depende de cfg)
def _png_esperados_dinamicos() -> list[str]:
    try:
        cfg = get_plot_config()["mapa_distribucion"]
        return PNG_ESPERADOS + [f"mapa_{cfg['componente'].lower()}_{cfg['ano']}.png"]
    except Exception:
        return PNG_ESPERADOS


def inferir_isla(municipio: str) -> str:
    for isla, munis in MUNICIPIOS_POR_ISLA.items():
        if municipio in munis:
            return isla
    return "Desconocida"


def _indice_brecha(cfg: dict) -> pd.DataFrame:
    """Calcula el índice de brecha H/M ponderado por sueldos. Reutilizado en dos checks."""
    ocu  = pd.read_csv(get_processed_path(cfg["dataset_ocu"])).dropna(subset=["num_casos"])
    dist = pd.read_csv(get_processed_path(cfg["dataset_dist"])).dropna(subset=["OBS_VALUE"])
    ocu_hm = (
        ocu[ocu["sexo"].isin(SEXOS_ESPERADOS)]
        .groupby(["municipio", "año", "sexo"])["num_casos"].sum()
        .unstack("sexo").reset_index()
    )
    ocu_hm["ratio_hm"] = ocu_hm["Hombres"] / (ocu_hm["Hombres"] + ocu_hm["Mujeres"])
    sal = (
        dist[dist["MEDIDAS_CODE"] == "SUELDOS_SALARIOS"]
        .groupby(["municipio", "año"])["OBS_VALUE"].median()
        .reset_index()
    )
    merged = ocu_hm.merge(sal, on=["municipio", "año"])
    merged["indice"] = (merged["ratio_hm"] - 0.5) * merged["OBS_VALUE"]
    return merged


# ══════════════════════════════════════════════════════════════════════════════
# BLOQUE 1 — CHECKS DE PREPROCESAMIENTO
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset=preprocesar_datos_p5,
    description="Detecta nulos en columnas críticas de cada CSV procesado.",
)
def check_ausencia_nulos(context, preprocesar_datos_p5: str):
    """
    Gestalt — Figura/Fondo: los huecos inesperados rompen la forma de la
    visualización. Un nulo en OBS_VALUE o num_casos se propaga a sumas,
    medianas y escalas de color sin ningún aviso visible.
    """
    csv_files = glob.glob(os.path.join(preprocesar_datos_p5, "*.csv"))
    total_nulos = 0
    report_md = (
        "### Nulos por dataset\n\n"
        "| Dataset | Total nulos | Columnas afectadas |\n"
        "|---------|-------------|--------------------|\n"
    )
    for file in csv_files:
        df = pd.read_csv(file)
        n = int(df.isna().sum().sum())
        total_nulos += n
        if n > 0:
            cols    = df.columns[df.isna().any()].tolist()
            detalle = ", ".join([f"`{c}` ({int(df[c].isna().sum())})" for c in cols])
            status  = "🔴"
        else:
            detalle, status = "-", "🟢"
        report_md += f"| `{os.path.basename(file)}` | {status} {n} | {detalle} |\n"

    return AssetCheckResult(
        passed=bool(total_nulos == 0),
        severity=AssetCheckSeverity.WARN,
        metadata={"Nulos": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Verifica filas duplicadas en todos los CSVs procesados.",
)
def check_duplicados(context, preprocesar_datos_p5: str):
    """
    Gestalt — Similitud: un duplicado infla la barra o celda de un municipio
    haciéndola parecer dominante sin serlo. En el lollipop distorsiona el
    índice de brecha y los palos pierden significado de magnitud real.
    """
    csv_files = glob.glob(os.path.join(preprocesar_datos_p5, "*.csv"))
    passed = True
    report_md = "### Duplicados\n\n| Dataset | Filas duplicadas |\n|---------|------------------|\n"

    for file in csv_files:
        df    = pd.read_csv(file)
        n_dup = int(df.astype(str).duplicated().sum())
        passed = passed and (n_dup == 0)
        report_md += f"| `{os.path.basename(file)}` | {'🟢' if n_dup == 0 else '🔴'} {n_dup} |\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Duplicados": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Verifica continuidad temporal y valores válidos en columnas de sexo.",
)
def check_temporal_y_sexo(context, preprocesar_datos_p5: str):
    """
    Gestalt — Continuidad: un salto de año (2021→2023 sin 2022) hace que la
    línea una puntos lejanos creando una pendiente falsa.
    Gestalt — Similitud: una categoría de sexo inesperada rompe la paleta
    manual azul/rosa que codifica el sexo por color.
    """
    csv_files = glob.glob(os.path.join(preprocesar_datos_p5, "*.csv"))
    passed = True
    report_md = "### Continuidad temporal y valores de sexo\n"

    for file in csv_files:
        df    = pd.read_csv(file)
        fname = os.path.basename(file)
        report_md += f"\n#### `{fname}`\n"

        for col in ("Periodo", "año"):
            if col not in df.columns:
                continue
            periodos = sorted(df[col].dropna().unique())
            if len(periodos) > 1:
                saltos = [periodos[i+1] - periodos[i] for i in range(len(periodos)-1)]
                if any(s > 1 for s in saltos):
                    passed = False
                    report_md += f"- **{col}**: 🔴 Salto: {periodos}\n"
                else:
                    report_md += f"- **{col}**: 🟢 Continuo: {periodos}\n"

        for col in ("Sexo", "sexo"):
            if col not in df.columns:
                continue
            extra = set(df[col].dropna().unique()) - SEXOS_ESPERADOS - {"No consta"}
            if extra:
                passed = False
                report_md += f"- **{col}**: 🔴 Categorías extra: {extra}\n"
            else:
                report_md += f"- **{col}**: 🟢 Correcto\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Temporal_Sexo": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Verifica conteo de municipios por isla respecto a los esperados.",
)
def check_conteo_municipios(context, preprocesar_datos_p5: str):
    """
    Gestalt — Cierre: un municipio faltante rompe la percepción de territorio
    completo. Un alias duplicado infla una barra o celda silenciosamente.
    """
    csv_files = glob.glob(os.path.join(preprocesar_datos_p5, "*.csv"))
    if not csv_files:
        return AssetCheckResult(passed=False, metadata={"Error": MetadataValue.md("No se encontraron CSVs.")})

    passed = True
    report_md = "### Balance geográfico por isla\n\n"

    for file in csv_files:
        fname = os.path.basename(file)
        report_md += f"\n#### `{fname}`\n"
        df = pd.read_csv(file)
        if "municipio" not in df.columns:
            passed = False
            report_md += "🔴 Columna `municipio` faltante.\n"
            continue

        conteo      = {isla: set() for isla in ESPERADOS_ISLAS}
        desconocidos = set()
        for muni in df["municipio"].dropna().unique():
            isla = inferir_isla(muni)
            conteo[isla].add(muni) if isla != "Desconocida" else desconocidos.add(muni)

        is_sc_only = "-sc-" in fname.lower()
        report_md += "| Isla | Encontrados | Esperados | Estado | Observaciones |\n|---|---|---|---|---|\n"

        for isla, expected in ESPERADOS_ISLAS.items():
            if is_sc_only and isla not in ISLAS_SC:
                continue
            found = len(conteo[isla])
            if found != expected:
                passed = False
                enc_low = {m.lower() for m in conteo[isla]}
                ofi_low = {m.lower() for m in CANONICOS_ISLA[isla]}
                faltantes = [m for m in CANONICOS_ISLA[isla] if m.lower() not in enc_low]
                sobrantes = [m for m in conteo[isla]          if m.lower() not in ofi_low]
                detalle   = ""
                if faltantes:
                    detalle += f"**Faltan:** {', '.join(faltantes)}. "
                if sobrantes:
                    detalle += f"**Alias/sobra:** {', '.join(sobrantes)}"
                status = f"❌ ({found - expected:+d})"
            else:
                status, detalle = "✅ Exacto", "-"
            report_md += f"| **{isla}** | {found} | {expected} | {status} | {detalle} |\n"

        if desconocidos:
            report_md += f"\n⚠️ No categorizados: {', '.join(desconocidos)}\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Balance_Islas": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Valida rangos numéricos: [0,100] para % y >0 para rentas.",
)
def check_rangos_valores(context, preprocesar_datos_p5: str):
    """
    Gestalt — Figura/Fondo: un outlier extremo (porcentaje >100 o renta
    negativa) aplana el gradiente de color del mapa haciendo que el resto del
    territorio parezca homogéneo cuando no lo es.
    """
    csv_files = glob.glob(os.path.join(preprocesar_datos_p5, "*.csv"))
    passed = True
    report_md = "### Rangos de OBS_VALUE\n\n| Dataset | Tipo | Fuera de rango | Min | Max |\n|---------|------|----------------|-----|-----|\n"

    for file in csv_files:
        fname = os.path.basename(file)
        df    = pd.read_csv(file)
        if "OBS_VALUE" not in df.columns:
            continue

        if "distribucion" in fname:
            fuera = df[(df["OBS_VALUE"] < 0) | (df["OBS_VALUE"] > 100)]
            tipo  = "Porcentaje [0,100]"
        else:
            fuera = df[df["OBS_VALUE"] <= 0]
            tipo  = "Renta > 0"

        n      = len(fuera)
        passed = passed and (n == 0)
        report_md += (
            f"| `{fname}` | {tipo} | {'🟢' if n == 0 else '🔴'} {n} "
            f"| {df['OBS_VALUE'].min():.1f} | {df['OBS_VALUE'].max():.1f} |\n"
        )

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Rangos": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Suma de los 5 componentes de distribución ≈ 100% por sección y año.",
)
def check_suma_componentes_distribucion(context, preprocesar_datos_p5: str):
    """
    Gestalt — Similitud: si los componentes suman 60% en una sección, las
    proporciones no son comparables entre secciones. El lector asume que
    el color/posición codifica el mismo concepto, pero la unidad varía.
    """
    file = os.path.join(preprocesar_datos_p5, "distribucion-renta-ingresos.csv")
    if not os.path.exists(file):
        return AssetCheckResult(passed=True, description="Fichero no presente, check omitido.")

    df   = pd.read_csv(file).dropna(subset=["OBS_VALUE"])
    suma = df.groupby(["TERRITORIO_CODE", "año"])["OBS_VALUE"].sum().reset_index(name="suma")
    fuera = suma[(suma["suma"] < 90) | (suma["suma"] > 110)]
    n     = len(fuera)

    report_md = f"### Suma de componentes ≈ 100%\n\nSecciones fuera de [90,110]: **{n}**\n\n"
    if n > 0:
        report_md += fuera.head(20).to_markdown(index=False)

    return AssetCheckResult(
        passed=bool(n == 0),
        severity=AssetCheckSeverity.WARN,
        metadata={"Suma_Componentes": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Cobertura del join CSV ↔ GeoJSON a nivel municipio ≥ 80%.",
)
def check_cobertura_join_geojson(context, preprocesar_datos_p5: str):
    """
    Gestalt — Cierre + Figura/Fondo: una cobertura baja llena el mapa de
    municipios en gris neutro. El lector interpreta el gris como valor bajo,
    no como dato faltante.
    """
    datasets = {
        "rentamedia-sc-3.csv":             "municipio",
        "distribucion-renta-ingresos.csv": "municipio",
        "actividad-sc-3.csv":              "municipio",
        "ocupacion-sc-3.csv":              "municipio",
    }
    passed = True
    report_md = (
        "### Cobertura join municipio CSV ↔ GeoJSON\n\n"
        "| Dataset | Año | CSV | Match | Cobertura |\n"
        "|---------|-----|-----|-------|-----------|\n"
    )

    for fname, col_mun in datasets.items():
        fpath = os.path.join(preprocesar_datos_p5, fname)
        if not os.path.exists(fpath):
            continue
        df = pd.read_csv(fpath)
        if col_mun not in df.columns:
            report_md += f"| `{fname}` | — | — | — | ⚠️ sin col municipio |\n"
            continue
        col_año = "año" if "año" in df.columns else "Periodo" if "Periodo" in df.columns else None

        for año in sorted(AÑOS_ESPERADOS):
            gjson = get_geojson_path(f"secciones_{año}0101_tenerife.json")
            if not os.path.exists(gjson):
                continue
            try:
                gdf = gpd.read_file(gjson)
                gdf["municipio"] = gdf["etiqueta"].str.extract(r"- (.+)$")
                mun_gdf = set(gdf["municipio"].dropna().unique())
            except Exception as e:
                report_md += f"| `{fname}` | {año} | — | — | ⚠️ {e} |\n"
                continue

            df_año  = df[df[col_año] == año] if col_año else df
            mun_csv = set(df_año[col_mun].dropna().unique())
            if not mun_csv:
                report_md += f"| `{fname}` | {año} | 0 | 0 | ⚠️ sin datos |\n"
                continue

            match = mun_csv & mun_gdf
            cob   = len(match) / len(mun_csv)
            ok    = cob >= 0.8
            if not ok:
                passed = False
            report_md += f"| `{fname}` | {año} | {len(mun_csv)} | {len(match)} | {'🟢' if ok else '🔴'} {cob:.0%} |\n"
            sin_match = mun_csv - mun_gdf
            if sin_match:
                report_md += f"|  |  | *Sin match:* | `{'`, `'.join(sorted(sin_match)[:8])}`{'…' if len(sin_match) > 8 else ''} |  |\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Cobertura_GeoJSON": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Detecta nombres de municipio con espacios extra o formato invertido (Artículo, Nombre).",
)
def check_formato_nombres_municipio(context, preprocesar_datos_p5: str):
    """
    Gestalt — Similitud: "puerto de la cruz" y "Puerto de La Cruz" son el
    mismo municipio pero se pintarían con dos colores distintos al hacer
    groupby. "Cruz, Puerto de la" produce el mismo problema en joins.
    """
    csv_files = glob.glob(os.path.join(preprocesar_datos_p5, "*.csv"))
    passed = True
    report_md = "### Formato de nombres de municipio\n\n| Dataset | Espacios extra | Formato invertido |\n|---------|---------------|------------------|\n"

    for file in csv_files:
        df = pd.read_csv(file)
        if "municipio" not in df.columns:
            continue
        serie      = df["municipio"].dropna().astype(str).unique()
        espacios   = [m for m in serie if m != m.strip()]
        invertidos = [m for m in serie if "," in m]
        n_e, n_i   = len(espacios), len(invertidos)
        passed     = passed and (n_e == 0) and (n_i == 0)
        report_md += f"| `{os.path.basename(file)}` | {'🟢' if n_e == 0 else '🔴'} {n_e} | {'🟢' if n_i == 0 else '🔴'} {n_i} |\n"
        if invertidos:
            report_md += f"|  | Invertidos: | `{'`, `'.join(invertidos[:5])}`{'…' if n_i > 5 else ''} |\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Formato_Nombres": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Verifica que el nº de categorías por columna no supera 9 colores distinguibles.",
)
def check_cardinalidad_categorias(context, preprocesar_datos_p5: str):
    """
    Gestalt — Similitud / Carga cognitiva: más de 9 colores son imposibles
    de distinguir. Aplica a las columnas usadas como canal de color en los
    gráficos (actividad, ocupación, MEDIDAS_CODE).
    """
    cols_a_revisar = {
        "actividad-sc-3.csv":              "Actividad económica",
        "ocupacion-sc-3.csv":              "ocupacion",
        "distribucion-renta-ingresos.csv": "MEDIDAS_CODE",
        "rentamedia-sc-3.csv":             "MEDIDAS_CODE",
    }
    passed = True
    report_md = f"### Cardinalidad de categorías (límite: {MAX_CATEGORIAS})\n\n| Dataset | Columna | N categorías | Estado |\n|---------|---------|-------------|--------|\n"

    for fname, col in cols_a_revisar.items():
        fpath = os.path.join(preprocesar_datos_p5, fname)
        if not os.path.exists(fpath):
            continue
        df = pd.read_csv(fpath)
        if col not in df.columns:
            continue
        n  = int(df[col].nunique())
        ok = n <= MAX_CATEGORIAS
        passed = passed and ok
        report_md += f"| `{fname}` | `{col}` | {n} | {'🟢' if ok else '🔴'} |\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Cardinalidad": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Detecta etiquetas demasiado largas para los ejes de gráficos (límite 35 chars).",
)
def check_longitud_etiquetas(context, preprocesar_datos_p5: str):
    """
    Gestalt — Continuidad: etiquetas largas en coord_flip se solapan entre sí
    rompiendo la legibilidad del eje Y. Las ocupaciones largas se envuelven
    automáticamente con textwrap, pero municipio y actividad no.
    """
    csv_files = glob.glob(os.path.join(preprocesar_datos_p5, "*.csv"))
    passed = True
    report_md = f"### Longitud de etiquetas de eje (límite: {MAX_LABEL} chars)\n\n| Dataset | Columna | Más larga | Chars | Estado |\n|---------|---------|-----------|-------|--------|\n"

    for file in csv_files:
        df = pd.read_csv(file)
        for col in ("municipio", "Actividad económica"):
            if col not in df.columns:
                continue
            serie   = df[col].dropna().astype(str)
            max_len = int(serie.str.len().max())
            longest = serie[serie.str.len() == max_len].iloc[0]
            ok      = max_len <= MAX_LABEL
            passed  = passed and ok
            report_md += f"| `{os.path.basename(file)}` | `{col}` | `{longest[:40]}` | {max_len} | {'🟢' if ok else '⚠️'} |\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Longitud_Etiquetas": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Detecta si un componente de ingresos domina la distribución (>80% del total).",
)
def check_dominancia_componente(context, preprocesar_datos_p5: str):
    """
    Gestalt — Figura/Fondo: un componente que supera el 80% atrae toda la
    atención visual y aplasta los demás en el gráfico de líneas, haciendo
    invisible la variación en el resto de fuentes de ingreso.
    """
    file = os.path.join(preprocesar_datos_p5, "distribucion-renta-ingresos.csv")
    if not os.path.exists(file):
        return AssetCheckResult(passed=True, description="Fichero no presente, check omitido.")

    df    = pd.read_csv(file).dropna(subset=["OBS_VALUE"])
    total = df["OBS_VALUE"].sum()
    pcts  = df.groupby("MEDIDAS_CODE")["OBS_VALUE"].sum() / total
    doms  = pcts[pcts > DOMINANCE_MAX]

    report_md = f"### Dominancia de componentes (umbral: {DOMINANCE_MAX:.0%})\n\n"
    report_md += pcts.sort_values(ascending=False).map(lambda x: f"{x:.1%}").to_frame("% del total").to_markdown()
    if len(doms) > 0:
        report_md += f"\n\n🔴 Dominantes: {doms.index.tolist()}"

    return AssetCheckResult(
        passed=bool(len(doms) == 0),
        severity=AssetCheckSeverity.WARN,
        metadata={"Dominancia": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Detecta outliers que comprimen la escala e invisibilizan el resto de valores.",
)
def check_escala_outliers(context, preprocesar_datos_p5: str):
    """
    Gestalt — Figura/Fondo / Proporcionalidad: si un municipio tiene una renta
    10× superior a la mediana, las barras del resto parecen invisibles.
    Ratio max/mediana > 5 es el umbral de riesgo para boxplots y barras.
    """
    datasets = {
        "rentamedia-sc-3.csv": "OBS_VALUE",
        "actividad-sc-3.csv":  "num_casos",
        "ocupacion-sc-3.csv":  "num_casos",
    }
    passed = True
    report_md = f"### Outliers de escala (ratio max/mediana, umbral: {RATIO_MAX}×)\n\n| Dataset | Max | Mediana | Ratio | Estado |\n|---------|-----|---------|-------|--------|\n"

    for fname, col in datasets.items():
        fpath = os.path.join(preprocesar_datos_p5, fname)
        if not os.path.exists(fpath):
            continue
        df    = pd.read_csv(fpath).dropna(subset=[col])
        vmax  = float(df[col].max())
        vmed  = float(df[col].median())
        ratio = vmax / vmed if vmed > 0 else float("inf")
        ok    = ratio <= RATIO_MAX
        passed = passed and ok
        report_md += f"| `{fname}` | {vmax:.0f} | {vmed:.0f} | {ratio:.1f}× | {'🟢' if ok else '⚠️'} |\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Escala_Outliers": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Verifica consistencia de nombres de municipio entre datasets que se cruzan.",
)
def check_consistencia_municipios_cruzados(context, preprocesar_datos_p5: str):
    """
    Gestalt — Similitud: un municipio con dos grafías distintas entre CSVs
    aparece como dos entidades o desaparece del lollipop / mapa de brecha
    sin ningún aviso. El join inner entre ocupacion y distribución pierde
    filas silenciosamente si los nombres no coinciden exactamente.
    """
    MIN_COB = 0.90
    pares = [
        ("ocupacion-sc-3.csv",  "distribucion-renta-ingresos.csv"),
        ("rentamedia-sc-3.csv", "distribucion-renta-ingresos.csv"),
    ]
    passed = True
    report_md = "### Consistencia de municipios entre datasets cruzados\n\n| Par | Mun. A | Mun. B | Intersección | Cobertura |\n|-----|--------|--------|-------------|----------|\n"

    for fa, fb in pares:
        pfa = os.path.join(preprocesar_datos_p5, fa)
        pfb = os.path.join(preprocesar_datos_p5, fb)
        if not os.path.exists(pfa) or not os.path.exists(pfb):
            continue
        mun_a = set(pd.read_csv(pfa)["municipio"].dropna().unique())
        mun_b = set(pd.read_csv(pfb)["municipio"].dropna().unique())
        inter = mun_a & mun_b
        cob   = len(inter) / len(mun_a) if mun_a else 0
        ok    = cob >= MIN_COB
        passed = passed and ok
        report_md += f"| `{fa}` × `{fb}` | {len(mun_a)} | {len(mun_b)} | {len(inter)} | {'🟢' if ok else '🔴'} {cob:.0%} |\n"
        sin_match = mun_a - mun_b
        if sin_match:
            report_md += f"|  | Sin match: | `{'`, `'.join(sorted(sin_match)[:8])}`{'…' if len(sin_match) > 8 else ''} |  |  |\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Consistencia_Municipios": MetadataValue.md(report_md)},
    )


# ══════════════════════════════════════════════════════════════════════════════
# BLOQUE 2 — CHECKS DE PLOTS (precondiciones de datos)
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset=plot_actividad_barras,
    description="Precondiciones para plot_actividad_barras.",
)
def check_datos_actividad_barras(context):
    """
    Gestalt — Similitud: un valor inesperado en Sexo rompe la paleta manual
    azul/rosa (sin color asignado la barra queda sin relleno).
    Gestalt — Proximidad: municipios de otras islas mezclados con Tenerife
    rompen la agrupación geográfica implícita del gráfico.
    Gramática de gráficos: con modo=fill se necesitan ≥ 2 periodos para que
    la comparación temporal tenga sentido; con 1 periodo solo hay una barra.
    """
    cfg  = get_plot_config()["actividad_barras"]
    df   = pd.read_csv(get_processed_path(cfg["dataset"]))
    MODO = cfg.get("modo", "fill")
    passed = True
    report_md = f"### Precondiciones: actividad_barras (modo={MODO!r})\n\n"

    req   = {"Actividad económica", "num_casos", "Periodo", "Sexo", "geocode"}
    falta = req - set(df.columns)
    ok    = len(falta) == 0
    passed = passed and ok
    report_md += f"- Columnas requeridas: {'🟢' if ok else '🔴'} (faltan: {falta or '–'})\n"

    mun = set(df["municipio"].dropna().unique()) if "municipio" in df.columns else set()
    ajenas = {inferir_isla(m) for m in mun} - ISLAS_SC - {"Desconocida"}
    ok = len(ajenas) == 0
    passed = passed and ok
    report_md += f"- Solo islas SC Tenerife: {'🟢' if ok else '🔴'} (otras: {ajenas or '–'})\n"

    extra = set(df["Sexo"].dropna().unique()) - SEXOS_ESPERADOS - {"No consta"} if "Sexo" in df.columns else set()
    ok    = len(extra) == 0
    passed = passed and ok
    report_md += f"- Valores Sexo válidos: {'🟢' if ok else '🔴'} (extra: {extra or '–'})\n"

    neg = int((df["num_casos"].dropna() < 0).sum()) if "num_casos" in df.columns else 0
    ok  = neg == 0
    passed = passed and ok
    report_md += f"- num_casos ≥ 0: {'🟢' if ok else '🔴'} ({neg} negativos)\n"

    acts  = df["Actividad económica"].dropna().unique() if "Actividad económica" in df.columns else []
    n_act = sum(1 for a in acts if a != "No consta")
    ok    = n_act >= 4
    passed = passed and ok
    report_md += f"- Actividades válidas ≥ 4: {'🟢' if ok else '🔴'} ({n_act})\n"

    # Con modo fill necesitamos ≥ 2 periodos para que la comparación tenga sentido
    if MODO == "fill" and "Periodo" in df.columns:
        n_periodos = int(df["Periodo"].nunique())
        ok = n_periodos >= 2
        passed = passed and ok
        report_md += f"- Periodos ≥ 2 (requerido por modo=fill): {'🟢' if ok else '🔴'} ({n_periodos})\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Actividad": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=plot_ocupacion_divergente,
    description="Precondiciones para plot_ocupacion_divergente.",
)
def check_datos_ocupacion_divergente(context):
    """
    Gestalt — Cierre: si falta un sexo para una ocupación, el pivot produce
    NaN y la barra desaparece sin aviso (percepción de categoría ausente).
    Gestalt — Simetría: si todos los valores son del mismo signo el gráfico
    divergente pierde su razón de ser.
    Gramática de gráficos: si el año configurado no existe en los datos el
    plot sale vacío sin lanzar ningún error.
    """
    cfg  = get_plot_config()["ocupacion_divergente"]
    df   = pd.read_csv(get_processed_path(cfg["dataset"])).dropna(subset=["num_casos"])
    passed = True
    report_md = "### Precondiciones: ocupacion_divergente\n\n"

    sexos = set(df["sexo"].dropna().unique()) if "sexo" in df.columns else set()
    ok    = SEXOS_ESPERADOS.issubset(sexos)
    passed = passed and ok
    report_md += f"- Ambos sexos presentes: {'🟢' if ok else '🔴'} ({sexos})\n"

    df_v = df[(df["sexo"].isin(SEXOS_ESPERADOS)) & (df["ocupacion"] != "No consta")]
    pivot = df_v.groupby(["ocupacion", "sexo"])["num_casos"].sum().unstack("sexo")
    incompletas = pivot[pivot.isna().any(axis=1)].index.tolist()
    ok = len(incompletas) == 0
    passed = passed and ok
    report_md += f"- Pivot completo por ocupación: {'🟢' if ok else '🔴'} (incompletas: {incompletas or '–'})\n"

    n_ocu = int(df_v["ocupacion"].nunique())
    ok    = n_ocu >= 3
    passed = passed and ok
    report_md += f"- Ocupaciones válidas ≥ 3: {'🟢' if ok else '🔴'} ({n_ocu})\n"

    brechas = pivot["Hombres"] - pivot["Mujeres"]
    ok = bool((brechas > 0).any()) and bool((brechas < 0).any())
    passed = passed and ok
    report_md += f"- Divergencia real (+ y −): {'🟢' if ok else '🔴'}\n"

    # BUG FIX: verificar que el año configurado (si existe) tiene datos
    ano = cfg.get("ano", None)
    if ano is not None and "año" in df.columns:
        n_ano = int((df["año"] == ano).sum())
        ok    = n_ano > 0
        passed = passed and ok
        report_md += f"- Año configurado ({ano}) con datos: {'🟢' if ok else '🔴'} ({n_ano} filas)\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Divergente": MetadataValue.md(report_md)},
    )




@asset_check(
    asset=plot_brecha_salarial,
    description="Precondiciones para plot_brecha_salarial (lollipop de delta).",
)
def check_datos_brecha_salarial(context):
    """
    Gestalt — Continuidad: si el join entre ocupacion y distribución falla por
    nombres inconsistentes, los palos del lollipop desaparecen sin error
    visible, dejando un gráfico vacío sin narrativa de cambio.
    Gestalt — Proporcionalidad: municipios con pocos trabajadores tienen
    ratios H/M inestables — un contrato cambia el índice varias décimas.
    Gramática de gráficos: top_n < 3 no permite comparación; > 10 satura
    el eje Y del lollipop horizontal.
    """
    cfg             = get_plot_config()["brecha_salarial"]
    ocu             = pd.read_csv(get_processed_path(cfg["dataset_ocu"])).dropna(subset=["num_casos"])
    dist            = pd.read_csv(get_processed_path(cfg["dataset_dist"])).dropna(subset=["OBS_VALUE"])
    AÑO_INI, AÑO_FIN, TOP_N = cfg["ano_ini"], cfg["ano_fin"], cfg["top_n"]
    passed = True
    report_md = "### Precondiciones: brecha_salarial (lollipop)\n\n"

    for df_tmp, nombre in [(ocu, "ocupacion"), (dist, "distribucion")]:
        años = set(df_tmp["año"].dropna().unique())
        ok   = {AÑO_INI, AÑO_FIN}.issubset(años)
        passed = passed and ok
        report_md += f"- `{nombre}` años {AÑO_INI}/{AÑO_FIN}: {'🟢' if ok else '🔴'} ({años})\n"

    ok = "SUELDOS_SALARIOS" in set(dist["MEDIDAS_CODE"].dropna().unique())
    passed = passed and ok
    report_md += f"- SUELDOS_SALARIOS presente: {'🟢' if ok else '🔴'}\n"

    comunes = set(ocu["municipio"].dropna().unique()) & set(dist["municipio"].dropna().unique())
    ok = len(comunes) >= TOP_N
    passed = passed and ok
    report_md += f"- Municipios comunes ≥ {TOP_N}: {'🟢' if ok else '🔴'} ({len(comunes)})\n"

    ok = SEXOS_ESPERADOS.issubset(set(ocu["sexo"].dropna().unique()))
    passed = passed and ok
    report_md += f"- Ambos sexos en ocupación: {'🟢' if ok else '🔴'}\n"

    merged = _indice_brecha(cfg)
    ok = bool((merged["indice"] > 0).any()) and bool((merged["indice"] < 0).any())
    passed = passed and ok
    report_md += f"- Índice con valores + y −: {'🟢' if ok else '🔴'}\n"

    # top_n en rango visual útil [3, 10]
    ok_rango = 3 <= TOP_N <= 10
    passed = passed and ok_rango
    report_md += f"- top_n en rango [3,10]: {'🟢' if ok_rango else '🔴'} ({TOP_N})\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Brecha": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=plot_mapa_brecha_salarial,
    description="Precondiciones para plot_mapa_brecha_salarial.",
)
def check_datos_mapa_brecha(context):
    """
    Gestalt — Cierre: TwoSlopeNorm requiere valores + y − en el índice; si
    todos son positivos matplotlib lanza un error y el mapa no se genera.
    Gestalt — Figura/Fondo: municipios con geometría vacía tras dissolve
    producen centroides NaN y las anotaciones directas en el mapa fallan.
    """
    cfg      = get_plot_config()["brecha_salarial"]
    AÑO_MAPA = cfg.get("ano_mapa", 2023)
    N_ANOT   = cfg.get("n_anotaciones_mapa", 3)
    passed = True
    report_md = f"### Precondiciones: mapa_brecha_salarial (año={AÑO_MAPA})\n\n"

    gjson = get_geojson_path(f"secciones_{AÑO_MAPA}0101_tenerife.json")
    ok    = os.path.exists(gjson)
    passed = passed and ok
    report_md += f"- GeoJSON año={AÑO_MAPA}: {'🟢' if ok else '🔴'}\n"

    if ok:
        try:
            gdf = gpd.read_file(gjson).set_crs("EPSG:4326", allow_override=True)
            gdf["municipio"] = gdf["etiqueta"].str.extract(r"- (.+)$")
            gdf_d = gdf.dissolve(by="municipio", as_index=False)
            n_mun = int(gdf_d["municipio"].nunique())
            ok2   = n_mun >= 30
            passed = passed and ok2
            report_md += f"- Municipios tras dissolve ≥ 30: {'🟢' if ok2 else '🔴'} ({n_mun})\n"

            inv = int((~gdf_d.geometry.is_valid).sum())
            ok3 = inv == 0
            passed = passed and ok3
            report_md += f"- Geometrías válidas: {'🟢' if ok3 else '🔴'} ({inv} inválidas)\n"

            # Centroides calculables para las anotaciones
            centroides_nan = int(gdf_d.geometry.centroid.isna().sum())
            ok4 = centroides_nan == 0
            passed = passed and ok4
            report_md += f"- Centroides calculables ({N_ANOT} anotaciones): {'🟢' if ok4 else '🔴'} ({centroides_nan} NaN)\n"
        except Exception as e:
            report_md += f"- Error GeoJSON: ⚠️ {e}\n"

    merged  = _indice_brecha(cfg)
    idx_año = merged[merged["año"] == AÑO_MAPA]["indice"]
    ok = bool((idx_año > 0).any()) and bool((idx_año < 0).any())
    passed = passed and ok
    report_md += f"- TwoSlopeNorm viable (+ y − en {AÑO_MAPA}): {'🟢' if ok else '🔴'}\n"

    # Año mapa existe en los datos
    ok_ano = AÑO_MAPA in set(merged["año"].unique())
    passed = passed and ok_ano
    report_md += f"- Año {AÑO_MAPA} presente en datos: {'🟢' if ok_ano else '🔴'}\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Mapa_Brecha": MetadataValue.md(report_md)},
    )


# ══════════════════════════════════════════════════════════════════════════════
# BLOQUE 3 — CHECKS DE SALIDA (ficheros PNG generados)
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset=commitear_plots_a_github,
    description=(
        "Verifica que cada PNG esperado existe por nombre canónico, "
        "pesa > 50 KB y tiene menos de 30 min de antigüedad."
    ),
)
def check_output_plots(context):
    """
    Gestalt — Veracidad Visual: un PNG vacío (<50 KB) indica un lienzo en
    blanco o un gráfico sin datos. Un fichero con más de 30 min de antigüedad
    puede ser del run anterior, comprometiendo la integridad del commit.

    BUG FIX: en lugar de glob *.png (que pasaba con ficheros anticuados como
    brecha_salarial_slope.png), verifica explícitamente cada nombre canónico
    actual. Detecta tanto ficheros faltantes como nombres obsoletos en disco.
    """
    plot_dir  = get_plot_dir()
    esperados = _png_esperados_dinamicos()
    now       = time.time()
    passed    = True
    report_md = (
        f"### PNG esperados (mín. {MIN_KB_PLOT} KB, máx. {MAX_AGE_S//60} min)\n\n"
        "| Fichero | Existe | Tamaño | Antigüedad | Estado |\n"
        "|---------|--------|--------|-----------|--------|\n"
    )

    for fname in esperados:
        fpath = os.path.join(plot_dir, fname)
        if not os.path.exists(fpath):
            passed = False
            report_md += f"| `{fname}` | 🔴 No existe | — | — | 🔴 |\n"
            continue
        kb    = os.path.getsize(fpath) / 1024
        age_m = (now - os.path.getmtime(fpath)) / 60
        ok    = (kb >= MIN_KB_PLOT) and (age_m <= MAX_AGE_S / 60)
        passed = passed and ok
        report_md += f"| `{fname}` | ✅ | {kb:.1f} KB | {age_m:.0f} min | {'🟢' if ok else '🔴'} |\n"

    # Avisar de ficheros huérfanos en disco (runs anteriores)
    todos_png = {os.path.basename(f) for f in glob.glob(os.path.join(plot_dir, "*.png"))}
    huerfanos = todos_png - set(esperados)
    if huerfanos:
        report_md += f"\n⚠️ Ficheros en disco no esperados (posibles runs anteriores): `{'`, `'.join(sorted(huerfanos))}`\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Output_Plots": MetadataValue.md(report_md)},
    )


# ── Constantes adicionales ────────────────────────────────────────────────────
AÑOS_GINI        = set(range(2015, 2024))
AÑOS_COMUNES     = {2021, 2022, 2023}
ISLAS            = set(ISLAS_ORDEN)
PROVINCIAS       = {"Las Palmas"}
MEDIDAS_GINI     = {"Índice de Gini", "Distribución de la renta P80/P20"}
MEDIDAS_RENTAS   = {
    "Sueldos y salarios", "Pensiones", "Prestaciones por desempleo",
    "Otras prestaciones", "Otros ingresos",
}
N_MUNICIPIOS_CAN = 88
MIN_KB           = 50
MAX_AGE_MIN      = 30


# ══════════════════════════════════════════════════════════════════════════════
# BLOQUE 4 — CHECKS TSV / GINI / RENTAS
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset=preprocesar_datos_p5,
    description="Verifica que gini.csv y rentas.csv existen en processed/ tras el preprocesado.",
)
def check_tsv_procesados(context, preprocesar_datos_p5: str):
    passed = True
    report_md = "### TSV procesados como CSV\n\n| Fichero | Existe | Filas |\n|---------|--------|-------|\n"

    for fname in ("gini.csv", "rentas.csv"):
        fpath = os.path.join(preprocesar_datos_p5, fname)
        existe = os.path.exists(fpath)
        if existe:
            n = len(pd.read_csv(fpath))
            report_md += f"| `{fname}` | 🟢 | {n} |\n"
        else:
            passed = False
            report_md += f"| `{fname}` | 🔴 No encontrado | — |\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"TSV_Procesados": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Verifica que la deduplicación de Santa Cruz de Tenerife funcionó correctamente.",
)
def check_dedup_santa_cruz(context, preprocesar_datos_p5: str):
    fpath = os.path.join(preprocesar_datos_p5, "gini.csv")
    if not os.path.exists(fpath):
        return AssetCheckResult(passed=True, description="gini.csv no presente, check omitido.")

    df   = pd.read_csv(fpath)
    dups = df.groupby(["TERRITORIO", "TIME_PERIOD", "MEDIDAS"]).size()
    dups = dups[dups > 1]
    passed = len(dups) == 0

    report_md = "### Deduplicación Santa Cruz de Tenerife\n\n"
    if passed:
        report_md += "🟢 Sin duplicados en ningún (TERRITORIO, TIME_PERIOD, MEDIDAS).\n"
    else:
        report_md += f"🔴 {len(dups)} combinaciones duplicadas:\n\n"
        report_md += dups.reset_index().to_markdown(index=False)

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Dedup_SC": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Verifica que tipo_territorio se asignó correctamente en gini.csv y rentas.csv.",
)
def check_tipo_territorio(context, preprocesar_datos_p5: str):
    passed = True
    report_md = "### Columna tipo_territorio\n\n| Fichero | isla | provincia | municipio | Desconocidos |\n|---------|------|-----------|-----------|-------------|\n"

    for fname in ("gini.csv", "rentas.csv"):
        fpath = os.path.join(preprocesar_datos_p5, fname)
        if not os.path.exists(fpath):
            continue
        df = pd.read_csv(fpath)
        if "tipo_territorio" not in df.columns:
            passed = False
            report_md += f"| `{fname}` | 🔴 columna ausente | — | — | — |\n"
            continue
        vc = df["tipo_territorio"].value_counts().to_dict()
        desconocidos = df[~df["tipo_territorio"].isin({"isla", "provincia", "municipio"})]
        ok = len(desconocidos) == 0
        passed = passed and ok
        report_md += (
            f"| `{fname}` "
            f"| {vc.get('isla', 0)} "
            f"| {vc.get('provincia', 0)} "
            f"| {vc.get('municipio', 0)} "
            f"| {'🟢 0' if ok else f'🔴 {len(desconocidos)}'} |\n"
        )

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Tipo_Territorio": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Verifica cobertura temporal completa (2015-2023) y 88 municipios en gini.csv.",
)
def check_cobertura_gini(context, preprocesar_datos_p5: str):
    fpath = os.path.join(preprocesar_datos_p5, "gini.csv")
    if not os.path.exists(fpath):
        return AssetCheckResult(passed=True, description="gini.csv no presente, check omitido.")

    df     = pd.read_csv(fpath)
    passed = True
    report_md = "### Cobertura temporal y territorial — gini.csv\n\n"

    años_islas = set(
        df[df["tipo_territorio"] == "isla"]["TIME_PERIOD"].dropna().unique()
    )
    ok = AÑOS_GINI.issubset(años_islas)
    passed = passed and ok
    report_md += f"- Años 2015-2023 en islas: {'🟢' if ok else '🔴'} ({sorted(años_islas)})\n"

    n_mun = df[df["tipo_territorio"] == "municipio"]["TERRITORIO"].nunique()
    ok    = n_mun == N_MUNICIPIOS_CAN
    passed = passed and ok
    report_md += f"- Municipios únicos: {'🟢' if ok else '🔴'} {n_mun} (esperados: {N_MUNICIPIOS_CAN})\n"

    medidas = set(df["MEDIDAS"].dropna().unique())
    ok = MEDIDAS_GINI.issubset(medidas)
    passed = passed and ok
    report_md += f"- Medidas completas {MEDIDAS_GINI}: {'🟢' if ok else '🔴'} ({medidas})\n"

    v_min, v_max = float(df["OBS_VALUE"].min()), float(df["OBS_VALUE"].max())
    ok = (v_min >= 20) and (v_max <= 45)
    passed = passed and ok
    report_md += f"- Rango OBS_VALUE ∈ [20, 45]: {'🟢' if ok else '🔴'} [{v_min:.1f}, {v_max:.1f}]\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Cobertura_Gini": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Verifica cobertura y rangos de rentas.csv.",
)
def check_cobertura_rentas(context, preprocesar_datos_p5: str):
    fpath = os.path.join(preprocesar_datos_p5, "rentas.csv")
    if not os.path.exists(fpath):
        return AssetCheckResult(passed=True, description="rentas.csv no presente, check omitido.")

    df     = pd.read_csv(fpath)
    passed = True
    report_md = "### Cobertura y rangos — rentas.csv\n\n"

    medidas = set(df["MEDIDAS"].dropna().unique())
    ok = MEDIDAS_RENTAS.issubset(medidas)
    passed = passed and ok
    report_md += f"- Fuentes de ingreso completas: {'🟢' if ok else '🔴'} (faltan: {MEDIDAS_RENTAS - medidas or '–'})\n"

    fuera = int(((df["OBS_VALUE"] < 0) | (df["OBS_VALUE"] > 100)).sum())
    ok    = fuera == 0
    passed = passed and ok
    report_md += f"- OBS_VALUE ∈ [0, 100]: {'🟢' if ok else '🔴'} ({fuera} fuera de rango)\n"

    n_mun = df[df["tipo_territorio"] == "municipio"]["TERRITORIO"].nunique()
    ok    = n_mun == N_MUNICIPIOS_CAN
    passed = passed and ok
    report_md += f"- Municipios únicos: {'🟢' if ok else '🔴'} {n_mun} (esperados: {N_MUNICIPIOS_CAN})\n"

    años = set(df["TIME_PERIOD"].dropna().unique())
    ok   = AÑOS_COMUNES.issubset(años)
    passed = passed and ok
    report_md += f"- Años 2021-2023 presentes: {'🟢' if ok else '🔴'} ({sorted(años)})\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Cobertura_Rentas": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Verifica consistencia de nombres de municipio entre gini, rentas y datasets anteriores.",
)
def check_consistencia_municipios_gini(context, preprocesar_datos_p5: str):
    MIN_COB = 0.90
    pares = [
        ("gini.csv",   "rentas.csv"),
        ("gini.csv",   "distribucion-renta-ingresos.csv"),
        ("rentas.csv", "ocupacion-sc-3.csv"),
    ]
    passed = True
    report_md = "### Consistencia municipios entre datasets\n\n| Par | Mun. A | Mun. B | Intersección | Cobertura |\n|-----|--------|--------|-------------|----------|\n"

    for fa, fb in pares:
        pfa = os.path.join(preprocesar_datos_p5, fa)
        pfb = os.path.join(preprocesar_datos_p5, fb)
        if not os.path.exists(pfa) or not os.path.exists(pfb):
            continue

        col_a = "TERRITORIO" if "TERRITORIO" in pd.read_csv(pfa, nrows=1).columns else "municipio"
        col_b = "TERRITORIO" if "TERRITORIO" in pd.read_csv(pfb, nrows=1).columns else "municipio"

        mun_a = set(pd.read_csv(pfa)[col_a].dropna().unique())
        mun_b = set(pd.read_csv(pfb)[col_b].dropna().unique())

        excluir = ISLAS | PROVINCIAS
        mun_a   = {m for m in mun_a if m not in excluir}
        mun_b   = {m for m in mun_b if m not in excluir}

        inter = mun_a & mun_b
        cob   = len(inter) / len(mun_a) if mun_a else 0
        ok    = cob >= MIN_COB
        passed = passed and ok
        report_md += f"| `{fa}` × `{fb}` | {len(mun_a)} | {len(mun_b)} | {len(inter)} | {'🟢' if ok else '🔴'} {cob:.0%} |\n"
        sin_match = mun_a - mun_b
        if sin_match:
            report_md += f"|  | Sin match: | `{'`, `'.join(sorted(sin_match)[:8])}`{'…' if len(sin_match)>8 else ''} |  |  |\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Consistencia_Gini": MetadataValue.md(report_md)},
    )


# ══════════════════════════════════════════════════════════════════════════════
# BLOQUE 5 — CHECKS DE PLOTS (gini, heatmap, covid, contratos)
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset=plot_gini_evolucion_islas,
    description="Precondiciones para plot_gini_evolucion_islas.",
)
def check_datos_gini_evolucion(context):
    """
    Gestalt — Continuidad: sin los 9 años completos para todas las islas,
    la línea une puntos no consecutivos generando pendientes falsas.

    BUG FIX: el check ya no verifica un TOP3 hardcodeado. Comprueba que el
    top_n configurado (dinámico) tiene datos en el último año disponible, sea
    cual sea ese año.
    """
    cfg_plot  = get_plot_config().get("gini_evolucion_islas", {})
    top_n     = cfg_plot.get("top_n_islas", 3)

    df     = pd.read_csv(get_processed_path("gini.csv")).dropna(subset=["OBS_VALUE"])
    passed = True
    report_md = f"### Precondiciones: gini_evolucion_islas (top_n={top_n})\n\n"

    ISLAS_SIN_CANARIAS = {i for i in ISLAS_ORDEN if i != "Canarias"}
    islas_data = df[
        (df["MEDIDAS"] == "Índice de Gini") &
        (df["tipo_territorio"] == "isla") &
        (df["TERRITORIO"] != "Canarias")
    ]

    islas_presentes = set(islas_data["TERRITORIO"].unique())
    faltantes       = ISLAS_SIN_CANARIAS - islas_presentes
    ok = len(faltantes) == 0
    passed = passed and ok
    report_md += f"- 7 islas presentes (sin Canarias): {'🟢' if ok else '🔴'} (faltan: {faltantes or '–'})\n"

    por_isla = islas_data.groupby("TERRITORIO")["TIME_PERIOD"].nunique()
    incompletas = por_isla[por_isla < 9].index.tolist()
    ok = len(incompletas) == 0
    passed = passed and ok
    report_md += f"- 9 años completos por isla: {'🟢' if ok else '🔴'} (incompletas: {incompletas or '–'})\n"

    # BUG FIX: top N dinámico desde el último año disponible
    ultimo_año = int(islas_data["TIME_PERIOD"].max())
    con_ultimo = set(islas_data[islas_data["TIME_PERIOD"] == ultimo_año]["TERRITORIO"].unique())
    ok = len(con_ultimo) >= top_n
    passed = passed and ok
    report_md += f"- ≥ {top_n} islas con dato en {ultimo_año} (para el top dinámico): {'🟢' if ok else '🔴'} ({len(con_ultimo)})\n"

    for isla, grupo in islas_data.groupby("TERRITORIO"):
        años = sorted(grupo["TIME_PERIOD"].unique())
        if len(años) > 1:
            saltos = [años[i+1]-años[i] for i in range(len(años)-1)]
            if any(s > 1 for s in saltos):
                passed = False
                report_md += f"- Salto temporal en `{isla}`: 🔴 {años}\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Gini_Evolucion": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=plot_historico_tipos_contrato_por_edad,
    description="Precondiciones para el gráfico histórico de contratos (small multiple 2×2).",
)
def check_datos_historico_contratos(context):
    """
    Gestalt — Continuidad: el small multiple 2×2 necesita ≥ 4 años para que
    la reforma laboral de 2022 sea visible como inflexión. Con 2 años solo
    hay una línea recta sin narrativa.
    Gramática de gráficos: se verifica que existen los 4 tipos de contrato,
    las 3 franjas de edad y ambos sexos — sin ellos algunos paneles quedan
    vacíos y el layout 2×2 pierde coherencia.
    """
    data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)

    passed = True
    report_md = "### Precondiciones: historico_tipos_contrato_por_edad (2×2)\n\n"

    # Años disponibles
    años_encontrados = []
    for año in range(2019, 2026):
        if os.path.exists(os.path.join(data_dir, f"contratos{año}.csv")):
            años_encontrados.append(año)
    for año in [2023, 2024, 2025]:
        patron = os.path.join(data_dir, str(año), "contratos_registrados_*.csv")
        if glob.glob(patron):
            if año not in años_encontrados:
                años_encontrados.append(año)
    has_2026 = os.path.exists(os.path.join(data_dir, "processed", "contratos_202603.csv"))
    if has_2026:
        años_encontrados.append(2026)

    n_años = len(años_encontrados)
    ok = n_años >= 4
    passed = passed and ok
    report_md += f"- Años disponibles ≥ 4 (inflexión 2022 visible): {'🟢' if ok else '🔴'} ({sorted(años_encontrados)})\n"
    report_md += f"- Datos 2026 (marzo) presentes: {'🟢' if has_2026 else '🔴'}\n"

    # Verificar contenido de contratos_202603.csv (fuente más reciente)
    fpath_26 = os.path.join(data_dir, "processed", "contratos_202603.csv")
    if os.path.exists(fpath_26):
        df = pd.read_csv(fpath_26)
        df.columns = df.columns.str.strip()
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].str.strip()

        TC_MAP = {"Indefinido", "Temporal Tiempo Completo",
                  "Temporal Tiempo Parcial", "Conversión a Indefinido"}
        tipos_ok = TC_MAP & set(df["Tipo Contrato"].dropna().unique()) if "Tipo Contrato" in df.columns else set()
        ok = len(tipos_ok) == 4
        passed = passed and ok
        report_md += f"- 4 tipos de contrato en 2026: {'🟢' if ok else '🔴'} ({len(tipos_ok)}/4)\n"

        edades_ok = {"Menor de 25", "Entre 25 y 44", "45 o más"} & \
                    set(df["edad"].dropna().unique()) if "edad" in df.columns else set()
        ok = len(edades_ok) == 3
        passed = passed and ok
        report_md += f"- 3 franjas de edad en 2026: {'🟢' if ok else '🔴'} ({len(edades_ok)}/3)\n"

        ok = SEXOS_ESPERADOS.issubset(set(df["sexo"].dropna().unique())) if "sexo" in df.columns else False
        passed = passed and ok
        report_md += f"- Ambos sexos en 2026: {'🟢' if ok else '🔴'}\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Historico_Contratos": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=plot_heatmap_segregacion_sectorial,
    description=(
        "Verifica que contratos_202603.csv tiene las 7 islas, ambos sexos, "
        "top-12 actividades con masa suficiente y TwoSlopeNorm viable."
    ),
)
def check_datos_heatmap_segregacion(context):
    """
    Gestalt — Similitud: TwoSlopeNorm centrada en 0.5 requiere que haya
    celdas por encima y por debajo de la paridad.
    Gestalt — Proximidad: las 7 islas deben estar presentes para que la
    lectura izquierda→derecha tenga sentido geográfico.
    Gestalt — Cierre: con n < 10 contratos el ratio H/M es inestable.
    """
    passed   = True
    report_md = "### Check: heatmap_segregacion_sectorial\n\n"

    fpath = get_processed_path("contratos_202603.csv")
    if not os.path.exists(fpath):
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.WARN,
            metadata={"check": MetadataValue.md("🔴 contratos_202603.csv no encontrado.")})

    df = pd.read_csv(fpath)
    df.columns = df.columns.str.strip()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    sexos = set(df["sexo"].dropna().unique())
    ok    = {"Hombres","Mujeres"}.issubset(sexos)
    passed = passed and ok
    report_md += f"- Ambos sexos: {'🟢' if ok else '🔴'}\n"

    ISLAS_ESPERADAS = {"EL HIERRO","LA GOMERA","LA PALMA","TENERIFE",
                       "GRAN CANARIA","LANZAROTE","FUERTEVENTURA"}
    islas_datos = set(df["isla"].dropna().str.upper().unique())
    faltantes   = ISLAS_ESPERADAS - islas_datos
    ok = len(faltantes) == 0
    passed = passed and ok
    report_md += f"- 7 islas presentes: {'🟢' if ok else '🔴'} (faltan: {faltantes or '–'})\n"

    top_act = (df.groupby("Actividad económica")["Contratos"]
               .sum().nlargest(12).index.tolist())
    pivot = (df[df["Actividad económica"].isin(top_act)]
             .groupby(["Actividad económica","isla","sexo"])["Contratos"]
             .sum().unstack("sexo").fillna(0))
    pivot["ratio"] = pivot.get("Hombres",0) / (
        pivot.get("Hombres",0) + pivot.get("Mujeres",0) + 1e-9)
    celdas_escasas = int(((pivot.get("Hombres",0) + pivot.get("Mujeres",0)) < 10).sum())
    ok = celdas_escasas < 5
    passed = passed and ok
    report_md += f"- Celdas con n < 10 contratos: {'🟢' if ok else '⚠️'} ({celdas_escasas})\n"

    ok = bool((pivot["ratio"] > 0.5).any()) and bool((pivot["ratio"] < 0.5).any())
    passed = passed and ok
    report_md += f"- Ratios por encima y debajo de 0.5 (TwoSlopeNorm): {'🟢' if ok else '🔴'}\n"

    return AssetCheckResult(
        passed=bool(passed), severity=AssetCheckSeverity.WARN,
        metadata={"check": MetadataValue.md(report_md)})


@asset_check(
    asset=plot_covid_sueldos_islas,
    description="Verifica serie de sueldos/salarios 2015-2023 por isla.",
)
def check_datos_covid_sueldos(context):
    """
    Gestalt — Continuidad: un salto temporal produce una pendiente artificial.
    Gestalt — Figura/Fondo: el área COVID debe contener el mínimo de las
    islas turísticas para que la narrativa sea visualmente coherente.
    """
    passed   = True
    report_md = "### Check: covid_sueldos_islas\n\n"

    rentas = _load_rentas()
    sub    = rentas[rentas["MEDIDAS"] == "Sueldos y salarios"]

    if sub.empty:
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.WARN,
            metadata={"check": MetadataValue.md("🔴 'Sueldos y salarios' no encontrado en rentas.csv.")})

    AÑOS_ESP = set(range(2015, 2024))
    años_ok  = set(sub["TIME_PERIOD"].dropna().unique())
    ok = AÑOS_ESP.issubset(años_ok)
    passed = passed and ok
    report_md += f"- Años 2015-2023: {'🟢' if ok else '🔴'} ({sorted(años_ok)})\n"

    for isla in ["Tenerife","Fuerteventura","Lanzarote"]:
        años = sorted(sub[sub["TERRITORIO"]==isla]["TIME_PERIOD"].unique())
        if len(años) > 1:
            saltos = [años[i+1]-años[i] for i in range(len(años)-1)]
            if any(s > 1 for s in saltos):
                passed = False
                report_md += f"- Salto temporal `{isla}`: 🔴 {años}\n"
            else:
                report_md += f"- Serie `{isla}` continua: 🟢\n"

    vmin, vmax = float(sub["OBS_VALUE"].min()), float(sub["OBS_VALUE"].max())
    ok = (vmin >= 40) and (vmax <= 80)
    passed = passed and ok
    report_md += f"- Rango OBS_VALUE ∈ [40, 80]: {'🟢' if ok else '🔴'} [{vmin:.1f}, {vmax:.1f}]\n"

    turisticas = sub[sub["TERRITORIO"].isin(["Tenerife","Fuerteventura","Lanzarote"])]
    año_min    = int(turisticas.loc[turisticas["OBS_VALUE"].idxmin(), "TIME_PERIOD"])
    ok = año_min in (2020, 2021)
    passed = passed and ok
    report_md += f"- Mínimo turísticas dentro del área COVID: {'🟢' if ok else '⚠️'} (año {año_min})\n"

    return AssetCheckResult(
        passed=bool(passed), severity=AssetCheckSeverity.WARN,
        metadata={"check": MetadataValue.md(report_md)})


@asset_check(
    asset=plot_covid_prestaciones_islas,
    description="Verifica serie de prestaciones por desempleo 2015-2023 por isla.",
)
def check_datos_covid_prestaciones(context):
    """
    Gestalt — Figura/Fondo: el pico de prestaciones debe estar en 2020
    (dentro del área COVID) para que la narrativa sea coherente.
    """
    passed   = True
    report_md = "### Check: covid_prestaciones_islas\n\n"

    rentas = _load_rentas()
    sub    = rentas[rentas["MEDIDAS"] == "Prestaciones por desempleo"]

    if sub.empty:
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.WARN,
            metadata={"check": MetadataValue.md("🔴 'Prestaciones por desempleo' no encontrado.")})

    AÑOS_ESP = set(range(2015, 2024))
    años_ok  = set(sub["TIME_PERIOD"].dropna().unique())
    ok = AÑOS_ESP.issubset(años_ok)
    passed = passed and ok
    report_md += f"- Años 2015-2023: {'🟢' if ok else '🔴'} ({sorted(años_ok)})\n"

    turisticas = sub[sub["TERRITORIO"].isin(["Tenerife","Fuerteventura","Lanzarote"])]
    año_max    = int(turisticas.loc[turisticas["OBS_VALUE"].idxmax(), "TIME_PERIOD"])
    ok = año_max in (2020, 2021)
    passed = passed and ok
    report_md += f"- Pico turísticas dentro COVID: {'🟢' if ok else '⚠️'} (año {año_max})\n"

    vmin, vmax = float(sub["OBS_VALUE"].min()), float(sub["OBS_VALUE"].max())
    ok = (vmin >= 0) and (vmax <= 30)
    passed = passed and ok
    report_md += f"- Rango OBS_VALUE ∈ [0, 30]: {'🟢' if ok else '🔴'} [{vmin:.1f}, {vmax:.1f}]\n"

    return AssetCheckResult(
        passed=bool(passed), severity=AssetCheckSeverity.WARN,
        metadata={"check": MetadataValue.md(report_md)})


@asset_check(
    asset=plot_brecha_temporal_edad,
    description="Precondiciones para plot_brecha_temporal_edad.",
)
def check_datos_brecha_temporal_edad(context):
    """
    Gestalt — Proximidad: las barras H/M del mismo tipo de contrato deben
    estar adyacentes. Si falta un sexo en algún tipo, la barra desaparece
    y el lector interpreta paridad donde hay ausencia de dato.
    Diseño — escala compartida: verifica que el rango de porcentajes es
    similar entre grupos de edad (sharey=True es válido).
    """
    cfg      = get_plot_config().get("brecha_temporal_edad", {})
    ISLA     = cfg.get("isla", "Todas")
    passed   = True
    report_md = f"### Check: brecha_temporal_edad (isla={ISLA!r})\n\n"

    fpath = get_processed_path("contratos_202603.csv")
    if not os.path.exists(fpath):
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.WARN,
            metadata={"check": MetadataValue.md("🔴 contratos_202603.csv no encontrado.")})

    df = pd.read_csv(fpath)
    df.columns = df.columns.str.strip()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
    df = df[df["sexo"].isin(["Hombres","Mujeres"])]

    if ISLA != "Todas":
        df = df[df["isla"].str.upper() == ISLA.upper()]
        ok = len(df) > 0
        passed = passed and ok
        report_md += f"- Isla `{ISLA}` con datos: {'🟢' if ok else '🔴'}\n"

    ok = {"Hombres","Mujeres"}.issubset(set(df["sexo"].unique()))
    passed = passed and ok
    report_md += f"- Ambos sexos: {'🟢' if ok else '🔴'}\n"

    TC_MAP = {"Indefinido","Temporal Tiempo Completo",
              "Temporal Tiempo Parcial","Conversión a Indefinido"}
    tipos_ok = TC_MAP & set(df["Tipo Contrato"].dropna().unique())
    ok = len(tipos_ok) == 4
    passed = passed and ok
    report_md += f"- 4 tipos de contrato: {'🟢' if ok else '🔴'} ({len(tipos_ok)}/4)\n"

    edades_ok = {"Menor de 25","Entre 25 y 44","45 o más"} & \
                set(df["edad"].dropna().unique())
    ok = len(edades_ok) == 3
    passed = passed and ok
    report_md += f"- 3 franjas de edad: {'🟢' if ok else '🔴'} ({len(edades_ok)}/3)\n"

    TC_RENAME = {"Indefinido":"Indefinido","Temporal Tiempo Completo":"Temp. Completo",
                 "Temporal Tiempo Parcial":"Temp. Parcial",
                 "Conversión a Indefinido":"Conversión"}
    df["tc"] = df["Tipo Contrato"].map(TC_RENAME)
    agg = df.groupby(["edad","tc","sexo"])["Contratos"].sum().reset_index()
    totales = agg.groupby(["edad","sexo"])["Contratos"].sum().reset_index(name="total")
    agg = agg.merge(totales, on=["edad","sexo"])
    agg["pct"] = agg["Contratos"] / agg["total"] * 100
    rango_por_edad = agg.groupby("edad")["pct"].max()
    if len(rango_por_edad) > 1:
        ratio_rangos = rango_por_edad.max() / rango_por_edad.min()
        ok = ratio_rangos < 2.0
        passed = passed and ok
        report_md += (f"- Escala compartida coherente (ratio rangos < 2): "
                      f"{'🟢' if ok else '⚠️'} ({ratio_rangos:.1f}×)\n")

    return AssetCheckResult(
        passed=bool(passed), severity=AssetCheckSeverity.WARN,
        metadata={"check": MetadataValue.md(report_md)})


# ══════════════════════════════════════════════════════════════════════════════
# BLOQUE 6 — CHECKS NUEVOS
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset=preprocesar_datos_p5,
    description=(
        "Verifica que títulos de figura, etiquetas de eje Y (coord_flip) y "
        "etiquetas de facet no superan los límites de longitud por contexto."
    ),
)
def check_longitud_titulos_plots(context, preprocesar_datos_p5: str):
    """
    Gestalt — Figura/Fondo: un título que supera el ancho del canvas se
    trunca silenciosamente o se desborda fuera del área de dibujo. El lector
    pierde el contexto del gráfico sin entender por qué.

    Gramática de gráficos: en coord_flip (barras horizontales de ocupación
    y lollipop) el eje Y tiene espacio limitado. Etiquetas > 40 chars se
    solapan con las barras o se salen del margen izquierdo. En facet_wrap
    los paneles son estrechos — labels > 30 chars se cortan en pantalla.

    Comprueba:
      - Nombres de municipio como etiqueta de eje Y (lollipop, mapa) ≤ 20 chars
      - Nombres de actividad como facet label ≤ 30 chars
      - Nombres de ocupación como eje Y coord_flip ≤ 40 chars (textwrap lo
        envuelve, pero la primera línea no debe superar este límite)
    """
    passed = True
    report_md = "### Longitud de etiquetas por contexto de uso\n\n"
    report_md += "| Dataset | Columna | Contexto | Máx. permitido | Máx. encontrado | Estado |\n"
    report_md += "|---------|---------|----------|----------------|-----------------|--------|\n"

    checks = [
        ("ocupacion-sc-3.csv",              "ocupacion",            "eje Y coord_flip",  MAX_CHARS_EJE_Y_FLIP),
        ("actividad-sc-3.csv",              "Actividad económica",  "facet_wrap label",  MAX_CHARS_FACET_LABEL),
        ("distribucion-renta-ingresos.csv", "municipio",            "anotación mapa",    MAX_CHARS_ANOTACION_MAPA),
        ("ocupacion-sc-3.csv",              "municipio",            "anotación mapa",    MAX_CHARS_ANOTACION_MAPA),
    ]

    for fname, col, contexto, limite in checks:
        fpath = os.path.join(preprocesar_datos_p5, fname)
        if not os.path.exists(fpath):
            continue
        df = pd.read_csv(fpath)
        if col not in df.columns:
            continue
        serie   = df[col].dropna().astype(str)
        max_len = int(serie.str.len().max())
        longest = serie[serie.str.len() == max_len].iloc[0]
        ok      = max_len <= limite
        passed  = passed and ok
        report_md += (
            f"| `{fname}` | `{col}` | {contexto} | {limite} | "
            f"{max_len} (`{longest[:30]}{'…' if len(longest)>30 else ''}`) "
            f"| {'🟢' if ok else '⚠️'} |\n"
        )

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Longitud_Titulos": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description=(
        "Verifica que cada panel de facet y cada subgráfico tiene suficientes "
        "filas para producir una visualización con sentido (mín. 2 puntos)."
    ),
)
def check_min_filas_por_panel(context, preprocesar_datos_p5: str):
    """
    Gestalt — Proximidad: un panel vacío en facet_wrap se percibe como
    "categoría sin datos" y rompe el ritmo visual de la cuadrícula.
    Con 1 sola fila no hay barra de comparación — el panel parece lleno
    pero no comunica nada.

    Comprueba:
      - actividad_barras: cada (actividad × sexo × isla) tiene ≥ 2 periodos
      - brecha_temporal_edad: cada (edad × sexo × tipo_contrato) tiene ≥ 1 fila
      - historico: cada (tipo_contrato × edad × sexo) tiene ≥ 2 años
    """
    passed = True
    report_md = "### Mínimo de filas por panel de facet\n\n"

    # actividad_barras: ≥ 2 periodos por (actividad × Sexo)
    fpath = os.path.join(preprocesar_datos_p5, "actividad-sc-3.csv")
    if os.path.exists(fpath):
        df = pd.read_csv(fpath)
        if {"Actividad económica", "Sexo", "Periodo", "num_casos"}.issubset(df.columns):
            conteo = (df[df["Sexo"].isin(["Hombres","Mujeres"]) &
                        (df["Actividad económica"] != "No consta")]
                      .groupby(["Actividad económica","Sexo"])["Periodo"].nunique())
            vacios = conteo[conteo < 2]
            ok     = len(vacios) == 0
            passed = passed and ok
            report_md += f"- `actividad-sc-3.csv` paneles con < 2 periodos: {'🟢' if ok else '⚠️'} ({len(vacios)})\n"
            if not vacios.empty:
                report_md += f"  - Afectados: `{', '.join([str(i) for i in vacios.index[:5]])}`\n"

    # brecha_temporal_edad: ≥ 1 fila por (edad × sexo × tc)
    fpath = get_processed_path("contratos_202603.csv")
    if os.path.exists(fpath):
        df = pd.read_csv(fpath)
        df.columns = df.columns.str.strip()
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].str.strip()
        TC_MAP = {"Indefinido":"Indefinido","Temporal Tiempo Completo":"Temp. Completo",
                  "Temporal Tiempo Parcial":"Temp. Parcial","Conversión a Indefinido":"Conversión"}
        df["tc"] = df["Tipo Contrato"].map(TC_MAP)
        df_v = df[df["sexo"].isin(["Hombres","Mujeres"]) & df["tc"].notna()]
        if {"edad","tc","sexo"}.issubset(df_v.columns):
            conteo = df_v.groupby(["edad","tc","sexo"])["Contratos"].sum()
            vacios = conteo[conteo == 0]
            ok     = len(vacios) == 0
            passed = passed and ok
            report_md += f"- `contratos_202603` paneles vacíos (suma=0): {'🟢' if ok else '⚠️'} ({len(vacios)})\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Min_Filas_Panel": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description=(
        "Verifica coherencia de la paleta configurada en plot_config.yaml: "
        "hex válidos para colores y cmap existente en matplotlib."
    ),
)
def check_coherencia_paleta_config(context, preprocesar_datos_p5: str):
    """
    Gramática de gráficos: si cmap_brecha es un string que no existe en
    matplotlib, el mapa coroplético lanza ValueError en runtime. Si los hex
    de color_hombres/color_mujeres son inválidos, la paleta manual de
    scale_color_manual falla silenciosamente asignando el color por defecto
    (negro) a todas las series, haciendo la leyenda ilegible.
    """
    passed = True
    report_md = "### Coherencia de paleta en plot_config.yaml\n\n"

    try:
        cfg_paleta = get_plot_config().get("paleta", {})
    except Exception as e:
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.WARN,
            metadata={"check": MetadataValue.md(f"🔴 Error leyendo plot_config.yaml: {e}")})

    # Validar hex de colores
    HEX_RE = re.compile(r"^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$")
    for key in ("color_hombres", "color_mujeres",
                "color_brecha_favorable_hombres", "color_brecha_favorable_mujeres"):
        val = cfg_paleta.get(key, "")
        if not val:
            continue
        ok = bool(HEX_RE.match(val))
        passed = passed and ok
        report_md += f"- `{key}` = `{val}`: {'🟢 hex válido' if ok else '🔴 hex inválido'}\n"

    # Validar cmap_brecha si es string
    cmap_val = cfg_paleta.get("cmap_brecha", None)
    if isinstance(cmap_val, str):
        try:
            plt.get_cmap(cmap_val)
            report_md += f"- `cmap_brecha` = `{cmap_val}`: 🟢 existe en matplotlib\n"
        except ValueError:
            passed = False
            report_md += f"- `cmap_brecha` = `{cmap_val}`: 🔴 no existe en matplotlib\n"
    else:
        report_md += "- `cmap_brecha` no configurado en YAML → se usará cmap azul/rosa por defecto 🟢\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Paleta_Config": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description=(
        "Verifica que los años configurados en plot_config.yaml existen "
        "realmente en los CSVs correspondientes."
    ),
)
def check_ano_configurable_existe(context, preprocesar_datos_p5: str):
    """
    Gramática de gráficos: si ano_mapa=2023 pero ese año no está en el CSV
    de ocupación, el mapa de brecha sale en blanco sin ningún error en Dagster.
    Mismo riesgo con ocupacion_divergente.ano y mapa_distribucion.ano.
    """
    passed = True
    report_md = "### Años configurados vs años en datos\n\n"
    report_md += "| Config | Año | Dataset | Años disponibles | Estado |\n"
    report_md += "|--------|-----|---------|-----------------|--------|\n"

    try:
        cfg = get_plot_config()
    except Exception as e:
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.WARN,
            metadata={"check": MetadataValue.md(f"🔴 Error leyendo config: {e}")})

    # mapa_distribucion.ano
    ano_md = cfg.get("mapa_distribucion", {}).get("ano")
    if ano_md:
        fpath = os.path.join(preprocesar_datos_p5,
                             cfg["mapa_distribucion"].get("dataset", ""))
        if os.path.exists(fpath):
            df   = pd.read_csv(fpath)
            años = sorted(df["año"].dropna().unique()) if "año" in df.columns else []
            ok   = ano_md in años
            passed = passed and ok
            report_md += f"| `mapa_distribucion.ano` | {ano_md} | `{os.path.basename(fpath)}` | {años} | {'🟢' if ok else '🔴'} |\n"

    # brecha_salarial.ano_ini / ano_fin / ano_mapa
    bs_cfg = cfg.get("brecha_salarial", {})
    for key in ("ano_ini", "ano_fin", "ano_mapa"):
        ano = bs_cfg.get(key)
        if not ano:
            continue
        for ds_key in ("dataset_ocu", "dataset_dist"):
            ds = bs_cfg.get(ds_key, "")
            fpath = os.path.join(preprocesar_datos_p5, ds)
            if not os.path.exists(fpath):
                continue
            df   = pd.read_csv(fpath)
            años = sorted(df["año"].dropna().unique()) if "año" in df.columns else []
            ok   = ano in años
            passed = passed and ok
            report_md += f"| `brecha_salarial.{key}` | {ano} | `{ds}` | {años} | {'🟢' if ok else '🔴'} |\n"

    # ocupacion_divergente.ano (opcional)
    ano_od = cfg.get("ocupacion_divergente", {}).get("ano")
    if ano_od:
        ds = cfg.get("ocupacion_divergente", {}).get("dataset", "")
        fpath = os.path.join(preprocesar_datos_p5, ds)
        if os.path.exists(fpath):
            df   = pd.read_csv(fpath)
            años = sorted(df["año"].dropna().unique()) if "año" in df.columns else []
            ok   = ano_od in años
            passed = passed and ok
            report_md += f"| `ocupacion_divergente.ano` | {ano_od} | `{ds}` | {años} | {'🟢' if ok else '🔴'} |\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Ano_Configurable": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=plot_brecha_salarial,
    description=(
        "Verifica que los municipios del top N tienen suficientes trabajadores "
        "para que el ratio H/M del lollipop sea estadísticamente estable."
    ),
)
def check_ratio_hm_estabilidad(context):
    """
    Gestalt — Proporcionalidad: en un municipio con 20 trabajadores, añadir
    un contrato de hombre mueve el ratio H/M en 5 puntos porcentuales.
    Ese municipio puede dominar el top N del lollipop por pura volatilidad
    estadística, no por una brecha salarial real.

    Umbral: municipios con < 30 trabajadores en ambos años se marcan como
    inestables. Si el top N contiene alguno, el check falla con WARN.
    """
    MIN_TRABAJADORES = 30
    cfg   = get_plot_config()["brecha_salarial"]
    ocu   = pd.read_csv(get_processed_path(cfg["dataset_ocu"])).dropna(subset=["num_casos"])
    TOP_N = cfg.get("top_n", 5)
    AÑO_INI, AÑO_FIN = cfg["ano_ini"], cfg["ano_fin"]

    passed = True
    report_md = f"### Estabilidad del ratio H/M en top {TOP_N} (mín. {MIN_TRABAJADORES} trabajadores)\n\n"

    masa = (
        ocu[ocu["sexo"].isin(["Hombres","Mujeres"]) &
            ocu["año"].isin([AÑO_INI, AÑO_FIN])]
        .groupby(["municipio","año"])["num_casos"].sum()
        .reset_index()
    )
    masa_min = masa.groupby("municipio")["num_casos"].min()
    inestables = masa_min[masa_min < MIN_TRABAJADORES].index.tolist()

    merged = _indice_brecha(cfg)
    ini    = merged[merged["año"] == AÑO_INI][["municipio","indice"]].rename(columns={"indice":"ini"})
    fin    = merged[merged["año"] == AÑO_FIN][["municipio","indice"]].rename(columns={"indice":"fin"})
    slope  = ini.merge(fin, on="municipio")
    slope["delta"] = slope["fin"] - slope["ini"]
    top_idx = slope["delta"].abs().nlargest(TOP_N).index
    top_munis = slope.loc[top_idx, "municipio"].tolist()

    inestables_en_top = [m for m in top_munis if m in inestables]
    ok = len(inestables_en_top) == 0
    passed = passed and ok

    report_md += f"- Municipios inestables (< {MIN_TRABAJADORES} trab.) en el top {TOP_N}: {'🟢' if ok else '⚠️'}\n"
    if inestables_en_top:
        report_md += f"  - {inestables_en_top}\n"
        report_md += f"  - Considera aumentar `top_n` o filtrar municipios con masa insuficiente.\n"

    report_md += f"\nTotal municipios inestables en el dataset: {len(inestables)}\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Ratio_HM_Estabilidad": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=plot_brecha_salarial,
    description=(
        "Verifica que todos los municipios del top N tienen datos de ambos "
        "sexos en los dos años del lollipop (sin NaN en el pivot)."
    ),
)
def check_balance_sexos_por_municipio(context):
    """
    Gestalt — Similitud: si en un municipio solo hay datos de un sexo, el
    pivot produce NaN y ese municipio desaparece del lollipop sin aviso.
    El lector interpreta la ausencia como "sin brecha" cuando en realidad
    es un dato faltante — el error de interpretación más grave posible en
    un gráfico de brecha de género.
    """
    cfg   = get_plot_config()["brecha_salarial"]
    ocu   = pd.read_csv(get_processed_path(cfg["dataset_ocu"])).dropna(subset=["num_casos"])
    TOP_N = cfg.get("top_n", 5)
    AÑO_INI, AÑO_FIN = cfg["ano_ini"], cfg["ano_fin"]

    passed = True
    report_md = f"### Balance de sexos por municipio (años {AÑO_INI} y {AÑO_FIN})\n\n"

    df_v = ocu[
        ocu["sexo"].isin(["Hombres","Mujeres"]) &
        ocu["año"].isin([AÑO_INI, AÑO_FIN]) &
        (ocu["ocupacion"] != "No consta")
    ]

    pivot = (df_v.groupby(["municipio","año","sexo"])["num_casos"]
             .sum().unstack("sexo").reset_index())
    pivot.columns.name = None

    for col in ["Hombres", "Mujeres"]:
        if col not in pivot.columns:
            pivot[col] = float("nan")

    incompletos = pivot[pivot[["Hombres","Mujeres"]].isna().any(axis=1)]
    n_inc = len(incompletos["municipio"].unique())
    ok    = n_inc == 0
    passed = passed and ok
    report_md += f"- Municipios con solo un sexo: {'🟢' if ok else '🔴'} ({n_inc})\n"
    if n_inc > 0:
        report_md += f"  - Municipios: `{', '.join(incompletos['municipio'].unique()[:8])}`\n"
        report_md += f"  - Estos municipios desaparecerán del lollipop si caen en el top {TOP_N}.\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Balance_Sexos_Municipio": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description=(
        "Verifica que hay suficientes años en el histórico de contratos para "
        "que la reforma laboral de 2022 sea visible como inflexión en el small multiple."
    ),
)
def check_suficientes_anios_historico(context, preprocesar_datos_p5: str):
    """
    Gestalt — Continuidad: con solo 2 años en el eje X el small multiple
    muestra una línea recta — no hay narrativa de cambio. La reforma laboral
    de 2022 (que transforma temporales en indefinidos) solo es visible si
    hay datos antes y después, es decir, ≥ 1 año antes de 2022 y ≥ 1 año
    después.

    Gramática de gráficos: el eje X del histórico se deriva dinámicamente
    de los datos; si solo hay 2 puntos el small multiple 2×2 no aporta más
    que un gráfico de barras simple.
    """
    data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
    passed   = True
    report_md = "### Años en histórico de contratos\n\n"

    años_disponibles = []
    for año in range(2019, 2026):
        path = os.path.join(data_dir, f"contratos{año}.csv")
        if os.path.exists(path):
            años_disponibles.append(año)
    for año in [2023, 2024, 2025]:
        if glob.glob(os.path.join(data_dir, str(año), "contratos_registrados_*.csv")):
            if año not in años_disponibles:
                años_disponibles.append(año)
    if os.path.exists(os.path.join(data_dir, "processed", "contratos_202603.csv")):
        años_disponibles.append(2026)

    años_disponibles = sorted(set(años_disponibles))
    n = len(años_disponibles)

    ok_min = n >= 4
    passed = passed and ok_min
    report_md += f"- Total años disponibles ≥ 4: {'🟢' if ok_min else '🔴'} ({años_disponibles})\n"

    antes_reforma  = [a for a in años_disponibles if a < 2022]
    despues_reforma = [a for a in años_disponibles if a >= 2022]
    ok_reforma = len(antes_reforma) >= 1 and len(despues_reforma) >= 1
    passed = passed and ok_reforma
    report_md += (
        f"- Datos antes y después de reforma 2022: {'🟢' if ok_reforma else '🔴'} "
        f"(antes: {antes_reforma}, después: {despues_reforma})\n"
    )

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Años_Historico": MetadataValue.md(report_md)},
    )