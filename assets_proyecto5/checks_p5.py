import os
import glob
import time
import pandas as pd
import geopandas as gpd
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
    get_plot_dir,
    plot_gini_evolucion_islas,
    plot_brecha_salarial_islas,
    plot_heatmap_segregacion_sectorial,
    plot_covid_sueldos_islas,
    plot_covid_prestaciones_islas,
    plot_brecha_temporal_edad,
    plot_historico_tipos_contrato_por_edad,
    ISLAS_ORDEN,
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
    Similitud: sin Canarias como referencia, el lector no puede calibrar
    si una isla está por encima o debajo del agregado regional.
    """
    df     = pd.read_csv(get_processed_path("gini.csv")).dropna(subset=["OBS_VALUE"])
    passed = True
    report_md = "### Precondiciones: gini_evolucion_islas\n\n"

    islas_data = df[(df["MEDIDAS"] == "Índice de Gini") &
                    (df["tipo_territorio"] == "isla")]

    # Todas las islas + Canarias presentes
    islas_presentes = set(islas_data["TERRITORIO"].unique())
    faltantes       = set(ISLAS_ORDEN) - islas_presentes
    ok = len(faltantes) == 0
    passed = passed and ok
    report_md += f"- Todos los territorios presentes: {'🟢' if ok else '🔴'} (faltan: {faltantes or '–'})\n"

    # 9 años completos para cada isla
    por_isla = islas_data.groupby("TERRITORIO")["TIME_PERIOD"].nunique()
    incompletas = por_isla[por_isla < 9].index.tolist()
    ok = len(incompletas) == 0
    passed = passed and ok
    report_md += f"- 9 años completos por isla: {'🟢' if ok else '🔴'} (incompletas: {incompletas or '–'})\n"

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
    description="Precondiciones para el boxplot de brecha salarial territorial.",
)
def check_datos_brecha_islas(context):
    from plots_assets import get_plot_config
    cfg = get_plot_config()["brecha_salarial"]
    
    ocu = pd.read_csv(os.path.join("..", "data-P5", "processed", "contratos_202603.csv")).dropna(subset=["Contratos"])
    dist = pd.read_csv(os.path.join("..", "data-P5", "processed", cfg["dataset_dist"])).dropna(subset=["OBS_VALUE"])
    
    n_mun = len(set(ocu["Municipio"]) & set(dist["municipio"]))
    passed = n_mun >= 40
    
    report_md = "### Precondiciones: Brecha Salarial Islas\n\n"
    report_md += f"- Intersección de municipios suficiente (≥ 40): {'🟢' if passed else '🔴'} ({n_mun})\n"
    
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
    import os, glob
    import config
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
