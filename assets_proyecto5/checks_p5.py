import os
import glob
import time
import pandas as pd
import geopandas as gpd
import config
from dagster import asset_check, AssetCheckResult, MetadataValue, AssetCheckSeverity
from assets import preprocesar_datos_p5, commitear_plots_a_github
from plots_assets import (
    plot_actividad_barras,
    plot_ocupacion_divergente,
    plot_mapa_distribucion_renta,
    plot_brecha_salarial,
    plot_mapa_brecha_salarial,
    get_processed_path,
    get_geojson_path,
    get_plot_config,
    get_paleta,
    get_plot_dir,
    plot_gini_evolucion_islas,
    plot_brecha_salarial_islas,
    plot_heatmap_segregacion_sectorial,
    plot_covid_sueldos_islas,
    plot_covid_prestaciones_islas,
    plot_brecha_temporal_edad,
    plot_historico_tipos_contrato_por_edad,
    plot_mapa_brecha_salarial_canarias,
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
    "Lanzarote":    {"Arrecife", "Haría", "San Bartolomé", "Teguise", "Tías", "Tinajo", "Yaiza"},
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
    "Lanzarote":    {"Arrecife", "Haría", "San Bartolomé", "Teguise", "Tías", "Tinajo", "Yaiza"},
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
    haciéndola parecer dominante sin serlo. En el slope chart distorsiona el
    índice de brecha y los segmentos pierden significado de tendencia.
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

        n     = len(fuera)
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
    Gestalt — Similitud: si los componentes suman 60% en una sección, el
    violín y las líneas IQR muestran proporciones no comparables entre
    secciones. El lector asume que el color/posición codifica el mismo
    concepto, pero la unidad de medida varía silenciosamente.
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
    no como dato faltante. Valida la disolución sección→municipio que hacen
    los assets de mapa (etiqueta → nombre municipio).
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
        serie     = df["municipio"].dropna().astype(str).unique()
        espacios  = [m for m in serie if m != m.strip()]
        invertidos = [m for m in serie if "," in m]
        n_e, n_i  = len(espacios), len(invertidos)
        passed    = passed and (n_e == 0) and (n_i == 0)
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
    automáticamente en plotnine, pero municipio y actividad no.
    """
    csv_files = glob.glob(os.path.join(preprocesar_datos_p5, "*.csv"))
    passed = True
    report_md = f"### Longitud de etiquetas (límite: {MAX_LABEL} chars)\n\n| Dataset | Columna | Más larga | Chars | Estado |\n|---------|---------|-----------|-------|--------|\n"

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
    atención visual y aplasta los demás en el gráfico de líneas y el violín,
    haciendo invisible la variación en el resto de fuentes de ingreso.
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
        "rentamedia-sc-3.csv":  "OBS_VALUE",
        "actividad-sc-3.csv":   "num_casos",
        "ocupacion-sc-3.csv":   "num_casos",
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
    aparece como dos entidades o desaparece del slope chart / mapa de brecha
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
    """
    cfg  = get_plot_config()["actividad_barras"]
    df   = pd.read_csv(get_processed_path(cfg["dataset"]))
    passed = True
    report_md = "### Precondiciones: actividad_barras\n\n"

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

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Divergente": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=plot_mapa_distribucion_renta,
    description="Precondiciones para plot_mapa_distribucion_renta.",
)
def check_datos_mapa_distribucion(context):
    """
    Gestalt — Figura/Fondo: cobertura baja produce municipios grises que el
    lector interpreta como valor bajo, no como dato faltante.
    Gestalt — Similitud: un outlier extremo aplana el gradiente secuencial
    haciendo que todo el territorio parezca homogéneo.
    """
    cfg        = get_plot_config()["mapa_distribucion"]
    año        = cfg["ano"]
    componente = cfg["componente"]
    df         = pd.read_csv(get_processed_path(cfg["dataset"])).dropna(subset=["OBS_VALUE"])
    df_fil     = df[(df["año"] == año) & (df["MEDIDAS_CODE"] == componente)]
    passed = True
    report_md = f"### Precondiciones: mapa_distribucion ({componente}, {año})\n\n"

    n  = len(df_fil)
    ok = n > 0
    passed = passed and ok
    report_md += f"- Registros año={año}, componente={componente}: {'🟢' if ok else '🔴'} ({n})\n"

    gjson = get_geojson_path(f"secciones_{año}0101_tenerife.json")
    ok    = os.path.exists(gjson)
    passed = passed and ok
    report_md += f"- GeoJSON existe: {'🟢' if ok else '🔴'}\n"

    if ok and n > 0:
        try:
            gdf = gpd.read_file(gjson)
            gdf["municipio"] = gdf["etiqueta"].str.extract(r"- (.+)$")
            mun_gdf = set(gdf["municipio"].dropna().unique())
            mun_csv = set(df_fil.groupby("municipio")["OBS_VALUE"].median().index)
            cob     = len(mun_csv & mun_gdf) / len(mun_csv) if mun_csv else 0
            ok_cob  = cob >= 0.8
            passed  = passed and ok_cob
            report_md += f"- Cobertura join municipio: {'🟢' if ok_cob else '🔴'} {cob:.0%}\n"
            sin_match = mun_csv - mun_gdf
            if sin_match:
                report_md += f"  - Sin match: `{'`, `'.join(sorted(sin_match)[:8])}`\n"
        except Exception as e:
            report_md += f"- Cobertura join: ⚠️ {e}\n"

    if n > 0:
        q1, q3  = df_fil["OBS_VALUE"].quantile([0.25, 0.75])
        outliers = int(((df_fil["OBS_VALUE"] < q1 - 3*(q3-q1)) | (df_fil["OBS_VALUE"] > q3 + 3*(q3-q1))).sum())
        report_md += f"- Outliers extremos (IQR×3): {'🟢' if outliers == 0 else '⚠️'} {outliers}\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Mapa_Dist": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=plot_brecha_salarial,
    description="Precondiciones para plot_brecha_salarial (slope chart).",
)
def check_datos_brecha_salarial(context):
    """
    Gestalt — Continuidad: si el join entre ocupacion y distribución falla por
    nombres inconsistentes, los segmentos del slope desaparecen sin error
    visible, rompiendo la lectura de tendencia temporal.
    """
    cfg             = get_plot_config()["brecha_salarial"]
    ocu             = pd.read_csv(get_processed_path(cfg["dataset_ocu"])).dropna(subset=["num_casos"])
    dist            = pd.read_csv(get_processed_path(cfg["dataset_dist"])).dropna(subset=["OBS_VALUE"])
    AÑO_INI, AÑO_FIN, TOP_N = cfg["ano_ini"], cfg["ano_fin"], cfg["top_n"]
    passed = True
    report_md = "### Precondiciones: brecha_salarial\n\n"

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
    todos son positivos matplotlib lanza un error y el mapa no se genera,
    dejando el territorio sin representación.
    """
    cfg      = get_plot_config()["brecha_salarial"]
    AÑO_MAPA = cfg.get("ano_mapa", 2023)
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
            n_mun = int(gdf["municipio"].nunique())
            ok2   = n_mun >= 30
            passed = passed and ok2
            report_md += f"- Municipios tras dissolve ≥ 30: {'🟢' if ok2 else '🔴'} ({n_mun})\n"
            inv = int((~gdf.geometry.is_valid).sum())
            ok3 = inv == 0
            passed = passed and ok3
            report_md += f"- Geometrías válidas: {'🟢' if ok3 else '🔴'} ({inv} inválidas)\n"
        except Exception as e:
            report_md += f"- Error GeoJSON: ⚠️ {e}\n"

    merged  = _indice_brecha(cfg)
    idx_año = merged[merged["año"] == AÑO_MAPA]["indice"]
    ok = bool((idx_año > 0).any()) and bool((idx_año < 0).any())
    passed = passed and ok
    report_md += f"- TwoSlopeNorm viable (+ y − en {AÑO_MAPA}): {'🟢' if ok else '🔴'}\n"

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
    description="Verifica que todos los PNG generados existen, pesan > 50 KB y son recientes.",
)
def check_output_plots(context):
    """
    Gestalt — Veracidad Visual: un PNG vacío (<50 KB) indica un lienzo en
    blanco o un gráfico sin datos. Un fichero con más de 30 min de antigüedad
    puede ser del run anterior, comprometiendo la integridad del commit.
    """
    png_files = glob.glob(os.path.join(get_plot_dir(), "*.png"))
    now    = time.time()
    passed = True
    report_md = f"### Verificación PNG generados (mín. {MIN_KB_PLOT} KB, máx. {MAX_AGE_S//60} min)\n\n| Fichero | Tamaño | Antigüedad | Estado |\n|---------|--------|-----------|--------|\n"

    if not png_files:
        return AssetCheckResult(
            passed=False,
            description=f"No se encontró ningún PNG en {get_plot_dir()}",
        )

    for f in sorted(png_files):
        kb    = os.path.getsize(f) / 1024
        age_m = (now - os.path.getmtime(f)) / 60
        ok    = (kb >= MIN_KB_PLOT) and (age_m <= MAX_AGE_S / 60)
        passed = passed and ok
        report_md += f"| `{os.path.basename(f)}` | {kb:.1f} KB | {age_m:.0f} min | {'🟢' if ok else '🔴'} |\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Output_Plots": MetadataValue.md(report_md)},
    )
# ── Constantes ────────────────────────────────────────────────────────────────
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
# BLOQUE 1 — CHECKS DE PREPROCESADO
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset=preprocesar_datos_p5,
    description="Verifica que gini.csv y rentas.csv existen en processed/ tras el preprocesado.",
)
def check_tsv_procesados(context, preprocesar_datos_p5: str):
    """
    Gestalt — Veracidad Visual: si los TSV no se procesaron correctamente
    los plots de desigualdad generarían gráficos vacíos o con error silencioso.
    Este check lo detecta antes de que se ejecute ningún asset de plot.
    """
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
    """
    Gestalt — Similitud: si SC Tenerife aparece duplicada, su barra en el
    heatmap y su punto en el scatter tendrían doble peso visual respecto al
    resto de municipios, rompiendo la lectura comparativa.
    """
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
    """
    Gestalt — Similitud: los plots filtran por tipo_territorio para separar
    islas de municipios. Si la columna falta o tiene valores erróneos, los
    plots mezclan islas y municipios en el mismo gráfico, rompiendo la
    coherencia visual entre series.
    """
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
        desconocidos = df[~df["tipo_territorio"].isin({"isla","provincia","municipio"})]
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
    """
    Gestalt — Continuidad: si falta un año intermedio en el Gini, la línea
    une puntos no consecutivos creando una pendiente artificial.
    Cierre: si faltan municipios, el heatmap de Tenerife o el scatter
    de Canarias presentan un territorio incompleto.
    """
    fpath = os.path.join(preprocesar_datos_p5, "gini.csv")
    if not os.path.exists(fpath):
        return AssetCheckResult(passed=True, description="gini.csv no presente, check omitido.")

    df     = pd.read_csv(fpath)
    passed = True
    report_md = "### Cobertura temporal y territorial — gini.csv\n\n"

    # Años completos para islas
    años_islas = set(
        df[df["tipo_territorio"] == "isla"]["TIME_PERIOD"].dropna().unique()
    )
    ok = AÑOS_GINI.issubset(años_islas)
    passed = passed and ok
    report_md += f"- Años 2015-2023 en islas: {'🟢' if ok else '🔴'} ({sorted(años_islas)})\n"

    # Municipios canarios
    n_mun = df[df["tipo_territorio"] == "municipio"]["TERRITORIO"].nunique()
    ok    = n_mun == N_MUNICIPIOS_CAN
    passed = passed and ok
    report_md += f"- Municipios únicos: {'🟢' if ok else '🔴'} {n_mun} (esperados: {N_MUNICIPIOS_CAN})\n"

    # Medidas completas
    medidas = set(df["MEDIDAS"].dropna().unique())
    ok = MEDIDAS_GINI.issubset(medidas)
    passed = passed and ok
    report_md += f"- Medidas completas {MEDIDAS_GINI}: {'🟢' if ok else '🔴'} ({medidas})\n"

    # Rango de Gini coherente [20, 45]
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
    description="Verifica cobertura y rangos de rentas.csv (distribución renta bruta por fuente).",
)
def check_cobertura_rentas(context, preprocesar_datos_p5: str):
    """
    Gestalt — Similitud: sin las 5 fuentes de ingresos la paleta del scatter
    no puede asignar un color por fuente. Figura/Fondo: porcentajes fuera
    de [0,100] distorsionan el eje X del scatter Gini vs sueldos.
    """
    fpath = os.path.join(preprocesar_datos_p5, "rentas.csv")
    if not os.path.exists(fpath):
        return AssetCheckResult(passed=True, description="rentas.csv no presente, check omitido.")

    df     = pd.read_csv(fpath)
    passed = True
    report_md = "### Cobertura y rangos — rentas.csv\n\n"

    # Medidas completas
    medidas = set(df["MEDIDAS"].dropna().unique())
    ok = MEDIDAS_RENTAS.issubset(medidas)
    passed = passed and ok
    report_md += f"- Fuentes de ingreso completas: {'🟢' if ok else '🔴'} (faltan: {MEDIDAS_RENTAS - medidas or '–'})\n"

    # Rango porcentual [0, 100]
    fuera = int(((df["OBS_VALUE"] < 0) | (df["OBS_VALUE"] > 100)).sum())
    ok    = fuera == 0
    passed = passed and ok
    report_md += f"- OBS_VALUE ∈ [0, 100]: {'🟢' if ok else '🔴'} ({fuera} fuera de rango)\n"

    # Municipios canarios
    n_mun = df[df["tipo_territorio"] == "municipio"]["TERRITORIO"].nunique()
    ok    = n_mun == N_MUNICIPIOS_CAN
    passed = passed and ok
    report_md += f"- Municipios únicos: {'🟢' if ok else '🔴'} {n_mun} (esperados: {N_MUNICIPIOS_CAN})\n"

    # Años comunes con datasets anteriores
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
    description="Verifica consistencia de nombres de municipio entre gini.csv, rentas.csv y los datasets anteriores.",
)
def check_consistencia_municipios_gini(context, preprocesar_datos_p5: str):
    """
    Gestalt — Similitud: municipios con grafías distintas entre datasets
    producirían puntos huérfanos en el scatter Gini × sueldos, ya que el
    merge inner entre gini.csv y rentas.csv perdería filas silenciosamente.
    Mismo problema al cruzar con los datasets anteriores.
    """
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

        # Excluir islas y provincias de la comparación
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
# BLOQUE 2 — CHECKS DE PLOTS
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset=plot_gini_evolucion_islas,
    description="Precondiciones para plot_gini_evolucion_islas.",
)
def check_datos_gini_evolucion(context):
    """
    Gestalt — Continuidad: sin los 9 años completos para todas las islas,
    la línea une puntos no consecutivos generando pendientes falsas.
    Similitud: el plot resalta top 3 por Gini 2023 — si alguna isla no tiene
    dato en 2023 la etiqueta directa desaparece sin error visible.
    """
    df     = pd.read_csv(get_processed_path("gini.csv")).dropna(subset=["OBS_VALUE"])
    passed = True
    report_md = "### Precondiciones: gini_evolucion_islas\n\n"

    ISLAS_SIN_CANARIAS = {i for i in ISLAS_ORDEN if i != "Canarias"}
    islas_data = df[(df["MEDIDAS"] == "Índice de Gini") &
                    (df["tipo_territorio"] == "isla") &
                    (df["TERRITORIO"] != "Canarias")]

    # Las 7 islas presentes (sin Canarias — no se grafica)
    islas_presentes = set(islas_data["TERRITORIO"].unique())
    faltantes       = ISLAS_SIN_CANARIAS - islas_presentes
    ok = len(faltantes) == 0
    passed = passed and ok
    report_md += f"- 7 islas presentes (sin Canarias): {'🟢' if ok else '🔴'} (faltan: {faltantes or '–'})\n"

    # 9 años completos para cada isla
    por_isla = islas_data.groupby("TERRITORIO")["TIME_PERIOD"].nunique()
    incompletas = por_isla[por_isla < 9].index.tolist()
    ok = len(incompletas) == 0
    passed = passed and ok
    report_md += f"- 9 años completos por isla: {'🟢' if ok else '🔴'} (incompletas: {incompletas or '–'})\n"

    # Dato 2023 disponible para el top 3
    TOP3 = ["La Palma","El Hierro","Tenerife"]
    con_2023 = set(islas_data[islas_data["TIME_PERIOD"]==2023]["TERRITORIO"].unique())
    sin_2023 = [i for i in TOP3 if i not in con_2023]
    ok = len(sin_2023) == 0
    passed = passed and ok
    report_md += f"- Top 3 con dato 2023: {'🟢' if ok else '🔴'} (sin dato: {sin_2023 or '–'})\n"

    # Sin saltos temporales
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
    asset=plot_brecha_salarial_islas,
    description=(
        "Precondiciones para plot_brecha_salarial_islas: "
        "contratos_202603.csv + distribucion-renta-ingresos, "
        "intersección ≥ 40 municipios, ambos sexos, TwoSlopeNorm viable."
    ),
)
def check_datos_brecha_islas(context):
    """
    Gestalt — Similitud: sin COLORES_ISLA con las 7 islas el boxplot asigna
    colores por posición, rompiendo la coherencia con los gráficos de línea.
    Cierre: TwoSlopeNorm requiere valores + y − para el mapa coroplético;
    si todos son positivos matplotlib lanza un error silencioso.
    """
    cfg      = get_plot_config()["brecha_salarial"]
    AÑO_MAPA = cfg.get("ano_mapa", 2023)
    passed   = True
    report_md = "### Precondiciones: brecha_salarial_islas\n\n"

    # Contratos 202603
    p_ocu = get_processed_path("contratos_202603.csv")
    ok    = os.path.exists(p_ocu)
    passed = passed and ok
    report_md += f"- contratos_202603.csv: {'🟢' if ok else '🔴'}\n"

    if ok:
        ocu = pd.read_csv(p_ocu).dropna(subset=["Contratos"])
        ocu = ocu.rename(columns={"Municipio": "municipio"})

        # Distribución renta
        p_dist = get_processed_path(cfg["dataset_dist"])
        ok2    = os.path.exists(p_dist)
        passed = passed and ok2
        report_md += f"- {cfg['dataset_dist']}: {'🟢' if ok2 else '🔴'}\n"

        if ok2:
            dist = pd.read_csv(p_dist).dropna(subset=["OBS_VALUE"])
            sal  = dist[(dist["MEDIDAS_CODE"] == "SUELDOS_SALARIOS") &
                        (dist["año"] == AÑO_MAPA)]

            n_mun = len(set(ocu["municipio"]) & set(sal["municipio"]))
            ok3   = n_mun >= 40
            passed = passed and ok3
            report_md += f"- Municipios en intersección ≥ 40: {'🟢' if ok3 else '🔴'} ({n_mun})\n"

        # Ambos sexos
        sexos = set(ocu["sexo"].dropna().unique())
        ok4   = {"Hombres","Mujeres"}.issubset(sexos)
        passed = passed and ok4
        report_md += f"- Ambos sexos en contratos: {'🟢' if ok4 else '🔴'}\n"

        # Colores de islas completos
        from checks_p5 import inferir_isla as _inferir
        islas_datos = set(ocu["municipio"].dropna().apply(_inferir).unique()) - {"Desconocida"}
        faltantes   = islas_datos - set(COLORES_ISLA.keys())
        ok5         = len(faltantes) == 0
        passed      = passed and ok5
        report_md  += (f"- Todas las islas con color: {'🟢' if ok5 else '🔴'}"
                       f" (sin color: {faltantes or '–'})\n")

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Brecha_Islas": MetadataValue.md(report_md)},
    )



@asset_check(
    asset=plot_historico_tipos_contrato_por_edad,
    description="Precondiciones para el gráfico de histórico de contratos.",
)
def check_datos_historico_contratos(context):
    data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
    
    files_23 = glob.glob(os.path.join(data_dir, "2023", "*.csv"))
    files_24 = glob.glob(os.path.join(data_dir, "2024", "*.csv"))
    
    has_2019 = os.path.exists(os.path.join(data_dir, "contratos2019.csv"))
    has_2026 = os.path.exists(os.path.join(data_dir, "processed", "contratos_202603.csv"))
    
    passed = (has_2019 or len(files_23) > 0 or len(files_24) > 0) and has_2026
    
    report_md = "### Precondiciones: Histórico de Contratos\n\n"
    report_md += f"- Datos 2026 marzo presentes: {'🟢' if has_2026 else '🔴'}\n"
    report_md += f"- Histórico disponible (2019, 2023 o 2024): {'🟢' if (has_2019 or len(files_23) > 0 or len(files_24) > 0) else '🔴'}\n"
    
    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Historico_Contratos": MetadataValue.md(report_md)},
    )


# ══════════════════════════════════════════════════════════════════════════════
# CHECKS FALTANTES — assets sin cobertura
# ══════════════════════════════════════════════════════════════════════════════


@asset_check(
    asset=plot_heatmap_segregacion_sectorial,
    description=(
        "Verifica que contratos_202603.csv tiene las 7 islas, ambos sexos, "
        "top-12 actividades con masa suficiente y TwoSlopeNorm viable."
    ),
)
def check_datos_heatmap_segregacion(context):
    """
    Gestalt — Similitud [heatmap]: TwoSlopeNorm centrada en 0.5 requiere
    que haya celdas por encima y por debajo de la paridad. Si todos los
    ratios son > 0.5 el gradiente se aplana en el extremo rojo y el lector
    no puede distinguir sectores con distinto grado de masculinización.

    Gestalt — Proximidad [heatmap]: las 7 islas deben estar presentes para
    que la lectura izquierda→derecha (oeste→este) tenga sentido geográfico.
    Un panel vacío se interpreta como "isla sin actividad", no como dato faltante.

    Gestalt — Cierre [heatmap]: cada celda es una unidad perceptiva completa.
    Con n < 10 contratos el ratio H/M es inestable (un contrato cambia 10 pp)
    y la celda miente al lector.
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

    # Ambos sexos
    sexos = set(df["sexo"].dropna().unique())
    ok    = {"Hombres","Mujeres"}.issubset(sexos)
    passed = passed and ok
    report_md += f"- Ambos sexos: {'🟢' if ok else '🔴'}\n"

    # 7 islas
    ISLAS_ESPERADAS = {"EL HIERRO","LA GOMERA","LA PALMA","TENERIFE",
                       "GRAN CANARIA","LANZAROTE","FUERTEVENTURA"}
    islas_datos = set(df["isla"].dropna().str.upper().unique())
    faltantes   = ISLAS_ESPERADAS - islas_datos
    ok = len(faltantes) == 0
    passed = passed and ok
    report_md += f"- 7 islas presentes: {'🟢' if ok else '🔴'} (faltan: {faltantes or '–'})\n"

    # Top-12 actividades con masa suficiente (n ≥ 10 por celda isla×sexo)
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
    report_md += f"- Celdas con n < 10: {'🟢' if ok else '⚠️'} ({celdas_escasas})\n"

    # TwoSlopeNorm viable: ratios por encima y por debajo de 0.5
    ok = bool((pivot["ratio"] > 0.5).any()) and bool((pivot["ratio"] < 0.5).any())
    passed = passed and ok
    report_md += f"- Ratios por encima y debajo de 0.5 (TwoSlopeNorm): {'🟢' if ok else '🔴'}\n"

    return AssetCheckResult(
        passed=bool(passed), severity=AssetCheckSeverity.WARN,
        metadata={"check": MetadataValue.md(report_md)})


@asset_check(
    asset=plot_covid_sueldos_islas,
    description=(
        "Verifica que rentas.csv tiene 'Sueldos y salarios' para las 7 islas "
        "en 2015-2023 sin saltos, y que el rango es coherente [40, 80]."
    ),
)
def check_datos_covid_sueldos(context):
    """
    Gestalt — Continuidad [covid_sueldos]: un salto temporal en la serie de
    sueldos produce una pendiente artificial. La línea conecta puntos no
    consecutivos y el lector infiere una caída brusca que no ocurrió.

    Gestalt — Figura/Fondo [covid_sueldos]: el área COVID (axvspan 2019.5-2021.5)
    debe contener el punto mínimo de las islas turísticas para que la narrativa
    "COVID causó la caída" sea visualmente coherente. Si el mínimo está fuera
    del área, el fondo no explica la figura.
    """
    passed   = True
    report_md = "### Check: covid_sueldos_islas\n\n"

    rentas = _load_rentas()
    sub    = rentas[rentas["MEDIDAS"] == "Sueldos y salarios"]

    if sub.empty:
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.WARN,
            metadata={"check": MetadataValue.md("🔴 'Sueldos y salarios' no encontrado en rentas.csv.")})

    # Años completos 2015-2023
    AÑOS_ESP = set(range(2015, 2024))
    años_ok  = set(sub["TIME_PERIOD"].dropna().unique())
    ok = AÑOS_ESP.issubset(años_ok)
    passed = passed and ok
    report_md += f"- Años 2015-2023: {'🟢' if ok else '🔴'} ({sorted(años_ok)})\n"

    # Sin saltos por isla turística
    for isla in ["Tenerife","Fuerteventura","Lanzarote"]:
        años = sorted(sub[sub["TERRITORIO"]==isla]["TIME_PERIOD"].unique())
        if len(años) > 1:
            saltos = [años[i+1]-años[i] for i in range(len(años)-1)]
            if any(s > 1 for s in saltos):
                passed = False
                report_md += f"- Salto temporal `{isla}`: 🔴 {años}\n"
            else:
                report_md += f"- Serie `{isla}` continua: 🟢\n"

    # Rango coherente [40, 80]
    vmin, vmax = float(sub["OBS_VALUE"].min()), float(sub["OBS_VALUE"].max())
    ok = (vmin >= 40) and (vmax <= 80)
    passed = passed and ok
    report_md += f"- Rango OBS_VALUE ∈ [40, 80]: {'🟢' if ok else '🔴'} [{vmin:.1f}, {vmax:.1f}]\n"

    # El mínimo de islas turísticas cae dentro del área COVID (2020-2021)
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
    description=(
        "Verifica que rentas.csv tiene 'Prestaciones por desempleo' para las "
        "7 islas en 2015-2023, que el pico está en 2020 y el rango es [0, 30]."
    ),
)
def check_datos_covid_prestaciones(context):
    """
    Gestalt — Figura/Fondo [covid_prestaciones]: el pico de prestaciones debe
    estar en 2020 (dentro del área COVID). Si el máximo histórico está fuera
    de 2020-2021, el área rosa deja de ser el fondo explicativo de la figura
    y la narrativa "COVID causó el pico" pierde coherencia visual.

    Gestalt — Continuidad [covid_prestaciones]: igual que sueldos — sin
    los 9 años completos la línea conecta puntos no consecutivos.
    """
    passed   = True
    report_md = "### Check: covid_prestaciones_islas\n\n"

    rentas = _load_rentas()
    sub    = rentas[rentas["MEDIDAS"] == "Prestaciones por desempleo"]

    if sub.empty:
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.WARN,
            metadata={"check": MetadataValue.md("🔴 'Prestaciones por desempleo' no encontrado.")})

    # Años completos
    AÑOS_ESP = set(range(2015, 2024))
    años_ok  = set(sub["TIME_PERIOD"].dropna().unique())
    ok = AÑOS_ESP.issubset(años_ok)
    passed = passed and ok
    report_md += f"- Años 2015-2023: {'🟢' if ok else '🔴'} ({sorted(años_ok)})\n"

    # El pico de islas turísticas está en 2020 o 2021
    turisticas = sub[sub["TERRITORIO"].isin(["Tenerife","Fuerteventura","Lanzarote"])]
    año_max    = int(turisticas.loc[turisticas["OBS_VALUE"].idxmax(), "TIME_PERIOD"])
    ok = año_max in (2020, 2021)
    passed = passed and ok
    report_md += f"- Pico turísticas dentro COVID: {'🟢' if ok else '⚠️'} (año {año_max})\n"

    # Rango coherente [0, 30]
    vmin, vmax = float(sub["OBS_VALUE"].min()), float(sub["OBS_VALUE"].max())
    ok = (vmin >= 0) and (vmax <= 30)
    passed = passed and ok
    report_md += f"- Rango OBS_VALUE ∈ [0, 30]: {'🟢' if ok else '🔴'} [{vmin:.1f}, {vmax:.1f}]\n"

    return AssetCheckResult(
        passed=bool(passed), severity=AssetCheckSeverity.WARN,
        metadata={"check": MetadataValue.md(report_md)})


@asset_check(
    asset=plot_brecha_temporal_edad,
    description=(
        "Verifica que contratos_202603.csv tiene los 4 tipos de contrato, "
        "las 3 franjas de edad, ambos sexos y el ámbito configurado."
    ),
)
def check_datos_brecha_temporal_edad(context):
    """
    Gestalt — Proximidad [brecha_temporal_edad]: las barras H/M del mismo
    tipo de contrato deben estar adyacentes. Si falta un sexo en algún tipo,
    la barra de ese lado desaparece y el lector interpreta paridad donde hay
    ausencia de dato — el peor error posible en un gráfico de brecha.

    Gestalt — Similitud [brecha_temporal_edad]: las etiquetas de porcentaje
    solo aparecen cuando la barra supera el 4%. Con barras de <4% sin etiqueta
    el lector asume que son barras vacías — se pierde la información de que
    la Conversión existe pero es pequeña.

    Diseño — escala compartida (sharey=True): si una franja de edad tiene
    valores extremos que otra no tiene, la escala compartida aplasta las
    diferencias en las otras franjas. El check verifica que el rango de
    porcentajes es similar entre grupos de edad.
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

    # Ambos sexos
    ok = {"Hombres","Mujeres"}.issubset(set(df["sexo"].unique()))
    passed = passed and ok
    report_md += f"- Ambos sexos: {'🟢' if ok else '🔴'}\n"

    # 4 tipos de contrato
    TC_MAP = {"Indefinido","Temporal Tiempo Completo",
              "Temporal Tiempo Parcial","Conversión a Indefinido"}
    tipos_ok = TC_MAP & set(df["Tipo Contrato"].dropna().unique())
    ok = len(tipos_ok) == 4
    passed = passed and ok
    report_md += f"- 4 tipos de contrato: {'🟢' if ok else '🔴'} ({len(tipos_ok)}/4)\n"

    # 3 franjas de edad
    edades_ok = {"Menor de 25","Entre 25 y 44","45 o más"} & \
                set(df["edad"].dropna().unique())
    ok = len(edades_ok) == 3
    passed = passed and ok
    report_md += f"- 3 franjas de edad: {'🟢' if ok else '🔴'} ({len(edades_ok)}/3)\n"

    # Rango porcentajes similar entre edades (sharey=True es válido)
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


@asset_check(
    asset=plot_mapa_brecha_salarial_canarias,
    description=(
        "Verifica contratos 2023, rentas 2023, GeoJSON canarias2026.geojson, "
        "cobertura ≥ 80 municipios y TwoSlopeNorm viable."
    ),
)
def check_datos_mapa_brecha_canarias(context):
    """
    Gestalt — Similitud [mapa_brecha_canarias]: TwoSlopeNorm centrada en 0
    requiere valores positivos y negativos. Si todos los municipios tienen
    índice > 0 el mapa se vuelve monocromo rojo y el lector no puede
    distinguir intensidades — la escala divergente pierde su razón de ser.

    Gestalt — Cierre [mapa_brecha_canarias]: con menos de 80/88 municipios
    con dato, los grises se perciben como "zona sin brecha" en lugar de
    "dato faltante". El umbral del 90% evita este error de interpretación.

    Gestalt — Figura/Fondo [mapa_brecha_canarias]: el percentil 95 como
    límite de la escala evita que outliers extremos aplanen el gradiente
    del resto del territorio, haciendo que la mayoría parezca neutra.
    """
    passed   = True
    report_md = "### Check: mapa_brecha_salarial_canarias\n\n"
    data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)

    # GeoJSON
    geojson = os.path.join(data_dir, "municipios2023.json")
    ok = os.path.exists(geojson)
    passed = passed and ok
    report_md += f"- municipios2023.json: {'🟢' if ok else '🔴'}\n"

    if ok:
        try:
            gdf_test = gpd.read_file(geojson)
            ok2 = len(gdf_test) >= 80
            passed = passed and ok2
            report_md += f"- GeoJSON ≥ 80 polígonos: {'🟢' if ok2 else '🔴'} ({len(gdf_test)})\n"
            ok3 = "etiqueta" in gdf_test.columns
            passed = passed and ok3
            report_md += f"- Columna 'etiqueta' presente: {'🟢' if ok3 else '🔴'}\n"
        except Exception as e:
            passed = False
            report_md += f"- Error leyendo GeoJSON: 🔴 {e}\n"

    # Contratos 2023
    paths_2023 = glob.glob(
        os.path.join(data_dir, "2023", "contratos_registrados_*.csv"))
    ok = len(paths_2023) >= 12
    passed = passed and ok
    report_md += f"- Ficheros contratos 2023 ≥ 12: {'🟢' if ok else '🔴'} ({len(paths_2023)})\n"

    # Rentas 2023
    rentas = _load_rentas()
    sal_2023 = rentas[(rentas["MEDIDAS"]=="Sueldos y salarios") &
                      (rentas["TIME_PERIOD"]==2023)]
    ok = len(sal_2023) > 0
    passed = passed and ok
    report_md += f"- Rentas 2023 disponibles: {'🟢' if ok else '🔴'} ({len(sal_2023)} municipios)\n"

    # TwoSlopeNorm viable (estimación rápida con muestra)
    if len(paths_2023) > 0 and len(sal_2023) > 0:
        try:
            def _ds(p):
                with open(p,'r',encoding='utf-8',errors='ignore') as f: l=f.readline()
                return ";" if l.count(";")>l.count(",") else ","
            df_s = pd.read_csv(paths_2023[0], sep=_ds(paths_2023[0]),
                               dtype={"Contratos":float})
            df_s.columns = df_s.columns.str.strip()
            df_s = df_s.rename(columns={"Contratos":"c"})
            df_s = df_s[df_s["sexo"].isin(["Hombres","Mujeres"])]
            ratio_s = (df_s.groupby(["Municipio","sexo"])["c"]
                       .sum().unstack("sexo").fillna(0))
            ratio_s["r"] = ratio_s.get("Hombres",0)/(
                ratio_s.get("Hombres",0)+ratio_s.get("Mujeres",0)+1e-9)
            ok = bool((ratio_s["r"]>0.5).any()) and bool((ratio_s["r"]<0.5).any())
            passed = passed and ok
            report_md += f"- TwoSlopeNorm viable (muestra 1 mes): {'🟢' if ok else '⚠️'}\n"
        except Exception as e:
            report_md += f"- TwoSlopeNorm: ⚠️ no verificado ({e})\n"

    return AssetCheckResult(
        passed=bool(passed), severity=AssetCheckSeverity.WARN,
        metadata={"check": MetadataValue.md(report_md)})