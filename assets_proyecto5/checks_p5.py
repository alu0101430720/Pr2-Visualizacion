import os
import glob
import pandas as pd
import geopandas as gpd
from dagster import asset_check, AssetCheckResult, MetadataValue, AssetCheckSeverity
from assets import preprocesar_datos_p5
from plots_assets import (
    plot_distribucion_lineas,
    plot_actividad_barras,
    plot_ocupacion_divergente,
    plot_renta_cajas,
    plot_mapa_distribucion_renta,
    plot_mapa_generico,
    plot_brecha_salarial,
    plot_mapa_brecha_salarial,
    plot_renta_violin,          # nuevo plot añadido en plots_assets.py
    get_processed_path,
    get_geojson_path,
    get_plot_config,
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
    "Lanzarote": {"Arrecife", "Haría", "San Bartolomé", "Teguise", "Tías", "Tinajo", "Yaiza"},
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
    "Lanzarote": {"Arrecife", "Haría", "San Bartolomé", "Teguise", "Tías", "Tinajo", "Yaiza"},
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

ISLAS_SC = {"Tenerife", "La Palma", "La Gomera", "El Hierro"}

MUNICIPIOS_TENERIFE = CANONICOS_ISLA["Tenerife"]

AÑOS_ESPERADOS  = {2021, 2022, 2023}
SEXOS_ESPERADOS = {"Hombres", "Mujeres"}

COMPONENTES_DIST = {
    "OTRAS_PRESTACIONES", "OTROS_INGRESOS", "PENSIONES",
    "PRESTACIONES_DESEMPLEO", "SUELDOS_SALARIOS",
}
MEDIDAS_RENTA = {
    "RENTA_BRUTA_MEDIA_HOGAR", "RENTA_BRUTA_MEDIA_PERSONA",
    "RENTA_NETA_MEDIA_HOGAR",  "RENTA_NETA_MEDIA_PERSONA",
    "RENTA_NETA_UNIDAD_CONSUMO_MEDIA", "RENTA_NETA_UNIDAD_CONSUMO_MEDIANA",
}

def inferir_isla(municipio):
    for isla, munis in MUNICIPIOS_POR_ISLA.items():
        if municipio in munis:
            return isla
    return "Desconocida"

def _check_fichero_plot(out_path: str, nombre: str, min_kb: int = 100):
    """Reutilizable: verifica existencia y tamaño mínimo de un PNG generado."""
    resultados = []
    existe = os.path.exists(out_path)
    resultados.append(AssetCheckResult(
        passed=existe,
        description=f"[{nombre}] Fichero generado: {out_path}",
    ))
    if existe:
        kb = os.path.getsize(out_path) / 1024
        resultados.append(AssetCheckResult(
            passed=kb >= min_kb,
            description=f"[{nombre}] Tamaño {kb:.1f} KB (mínimo {min_kb} KB)",
        ))
    return resultados


# ══════════════════════════════════════════════════════════════════════════════
# CHECKS DE PREPROCESAMIENTO (existentes, sin cambios)
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(asset=preprocesar_datos_p5, description="Comprueba que no existen valores nulos en el dataset.")
def check_ausencia_nulos(context, preprocesar_datos_p5: str):
    csv_files = glob.glob(os.path.join(preprocesar_datos_p5, "*.csv"))
    total_nulos = 0
    report_md = (
        "### Reporte de Nulos\n\n"
        "| Dataset | Total Nulos | Columnas Afectadas |\n"
        "|---------|-------------|--------------------|\n"
    )
    for file in csv_files:
        df = pd.read_csv(file)
        n_nulos = df.isna().sum().sum()
        total_nulos += n_nulos
        if n_nulos > 0:
            status = "🔴 Alerta"
            cols = df.columns[df.isna().any()].tolist()
            detalles = ", ".join([f"`{c}` ({df[c].isna().sum()})" for c in cols])
        else:
            status = "🟢 Limpio"
            detalles = "-"
        report_md += f"| `{os.path.basename(file)}` | {n_nulos} ({status}) | {detalles} |\n"

    return AssetCheckResult(
        passed=bool(total_nulos == 0),
        severity=AssetCheckSeverity.WARN,
        metadata={"Resumen_Nulos": MetadataValue.md(report_md)},
    )


@asset_check(asset=preprocesar_datos_p5, description="Verifica el conteo de municipios por isla.")
def check_conteo_municipios(context, preprocesar_datos_p5: str):
    csv_files = glob.glob(os.path.join(preprocesar_datos_p5, "*.csv"))
    if not csv_files:
        return AssetCheckResult(passed=False, metadata={"Error": MetadataValue.md("No se encontraron CSVs.")})

    passed = True
    report_md = (
        "### Balance Geográfico\n\n"
        "Verifica que el número de municipios por isla coincide con los esperados.\n\n"
    )

    for file in csv_files:
        fname = os.path.basename(file)
        report_md += f"\n#### Dataset: `{fname}`\n"
        df = pd.read_csv(file)
        if "municipio" not in df.columns:
            passed = False
            report_md += "🔴 Columna `municipio` faltante.\n"
            continue

        conteo = {isla: set() for isla in ESPERADOS_ISLAS}
        desconocidos = set()
        for muni in df["municipio"].dropna().unique():
            isla = inferir_isla(muni)
            if isla != "Desconocida":
                conteo[isla].add(muni)
            else:
                desconocidos.add(muni)

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
                faltantes  = [m for m in CANONICOS_ISLA[isla] if m.lower() not in enc_low]
                sobrantes  = [m for m in conteo[isla]          if m.lower() not in ofi_low]
                detalle = ""
                if faltantes:
                    detalle += f"**Faltan:** {', '.join(faltantes)}. "
                if sobrantes:
                    detalle += f"**Sobra/alias:** {', '.join(sobrantes)}"
                status = f"❌ ({found - expected:+d})"
            else:
                status, detalle = "✅ Exacto", "-"
            report_md += f"| **{isla}** | {found} | {expected} | {status} | {detalle} |\n"

        if desconocidos:
            report_md += f"\n⚠️ Municipios no categorizados: {', '.join(desconocidos)}\n"

    return AssetCheckResult(
        passed=bool(passed),
        metadata={"Balance_Islas": MetadataValue.md(report_md)},
    )


@asset_check(asset=preprocesar_datos_p5, description="Asegura continuidad temporal y dicotomía en Sexo.")
def check_temporal_y_sexo(context, preprocesar_datos_p5: str):
    csv_files = glob.glob(os.path.join(preprocesar_datos_p5, "*.csv"))
    report_md = "### Continuidad Temporal y Sexo\n"
    passed = True

    for file in csv_files:
        df = pd.read_csv(file)
        fname = os.path.basename(file)
        report_md += f"\n#### `{fname}`\n"

        for col_periodo in ("Periodo", "año"):
            if col_periodo in df.columns:
                periodos = sorted(df[col_periodo].dropna().unique())
                if len(periodos) > 1:
                    saltos = [periodos[i+1] - periodos[i] for i in range(len(periodos)-1)]
                    if any(s > 1 for s in saltos):
                        passed = False
                        report_md += f"- **{col_periodo}**: 🔴 Salto detectado: {periodos}\n"
                    else:
                        report_md += f"- **{col_periodo}**: 🟢 Continuo: {periodos}\n"

        for col_sexo in ("Sexo", "sexo"):
            if col_sexo in df.columns:
                extra = set(df[col_sexo].dropna().unique()) - SEXOS_ESPERADOS - {"No consta"}
                if extra:
                    passed = False
                    report_md += f"- **{col_sexo}**: 🔴 Categorías extra: {extra}\n"
                else:
                    report_md += f"- **{col_sexo}**: 🟢 Correcto\n"

    return AssetCheckResult(
        passed=bool(passed),
        metadata={"Reporte_Estructural": MetadataValue.md(report_md)},
    )


# ══════════════════════════════════════════════════════════════════════════════
# CHECKS NUEVOS DE PREPROCESAMIENTO (gaps detectados en revisión)
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset=preprocesar_datos_p5,
    description="Verifica que no hay filas duplicadas en ningún CSV procesado.",
)
def check_duplicados(context, preprocesar_datos_p5: str):
    """
    Gap detectado: los checks existentes no comprueban duplicados.
    Un registro duplicado inflaría sumas y medianas silenciosamente.
    """
    csv_files = glob.glob(os.path.join(preprocesar_datos_p5, "*.csv"))
    passed = True
    report_md = "### Duplicados por Dataset\n\n| Dataset | Filas duplicadas |\n|---------|------------------|\n"

    for file in csv_files:
        df = pd.read_csv(file)
        n_dup = df.duplicated().sum()
        passed = passed and bool(n_dup == 0)
        icono = "🟢" if n_dup == 0 else "🔴"
        report_md += f"| `{os.path.basename(file)}` | {icono} {n_dup} |\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Duplicados": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Verifica que OBS_VALUE está en rango [0, 100] para distribución y >0 para renta.",
)
def check_rangos_valores(context, preprocesar_datos_p5: str):
    """
    Gap detectado: los checks existentes no validan rangos numéricos.
    Detecta valores negativos, ceros inesperados o porcentajes > 100.
    """
    csv_files = glob.glob(os.path.join(preprocesar_datos_p5, "*.csv"))
    passed = True
    report_md = "### Rangos de OBS_VALUE\n\n| Dataset | Tipo | Fuera de rango | Min | Max |\n|---------|------|----------------|-----|-----|\n"

    for file in csv_files:
        fname = os.path.basename(file)
        df = pd.read_csv(file)
        if "OBS_VALUE" not in df.columns or "MEDIDAS_CODE" not in df.columns:
            continue

        if "distribucion" in fname:
            # Porcentajes: [0, 100]
            fuera = df[(df["OBS_VALUE"] < 0) | (df["OBS_VALUE"] > 100)]
            tipo = "Porcentaje [0,100]"
        else:
            # Renta: > 0
            fuera = df[df["OBS_VALUE"] <= 0]
            tipo = "Renta > 0"

        n_fuera = len(fuera)
        vmin = df["OBS_VALUE"].min()
        vmax = df["OBS_VALUE"].max()
        icono = "🟢" if n_fuera == 0 else "🔴"
        passed = passed and (n_fuera == 0)
        report_md += f"| `{fname}` | {tipo} | {icono} {n_fuera} | {vmin:.1f} | {vmax:.1f} |\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Rangos": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Verifica que la suma de componentes de distribución por sección ≈ 100%.",
)
def check_suma_componentes_distribucion(context, preprocesar_datos_p5: str):
    """
    Gap detectado: nadie valida que los 5 componentes sumen ~100 por sección y año.
    Una sección con suma 60 o 130 indica un problema de ingesta o codificación.
    """
    file = os.path.join(preprocesar_datos_p5, "distribucion-renta-ingresos.csv")
    if not os.path.exists(file):
        return AssetCheckResult(passed=True, description="Fichero no presente, check omitido.")

    df = pd.read_csv(file).dropna(subset=["OBS_VALUE"])
    suma = (
        df.groupby(["TERRITORIO_CODE", "año"])["OBS_VALUE"]
        .sum()
        .reset_index(name="suma_total")
    )
    desviadas = suma[(suma["suma_total"] < 90) | (suma["suma_total"] > 110)]
    n = len(desviadas)
    passed = n == 0

    report_md = (
        f"### Suma de componentes ≈ 100%\n\n"
        f"Secciones con suma fuera de [90, 110]: **{n}**\n\n"
    )
    if n > 0:
        report_md += desviadas.head(20).to_markdown(index=False)

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Suma_Componentes": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=preprocesar_datos_p5,
    description="Verifica cobertura del join CSV ↔ GeoJSON a nivel municipio.",
)
def check_cobertura_join_geojson(context, preprocesar_datos_p5: str):
    """
    Los mapas agregan datos a nivel municipio y hacen el join por nombre.
    Este check valida que los municipios presentes en cada CSV tienen
    correspondencia en el GeoJSON tras la disolución sección→municipio.
    Cobertura esperada ≥ 80%.
    """
    años = [2021, 2022, 2023]
    # Columna municipio en cada dataset
    datasets = {
        "rentamedia-sc-3.csv":             "municipio",
        "distribucion-renta-ingresos.csv": "municipio",
        "actividad-sc-3.csv":              "municipio",
        "ocupacion-sc-3.csv":              "municipio",
    }

    passed = True
    report_md = (
        "### Cobertura join CSV ↔ GeoJSON (nivel municipio)\n\n"
        "| Dataset | Año | Municipios CSV | Match GeoJSON | Cobertura |\n"
        "|---------|-----|---------------|---------------|----------|\n"
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

        for año in años:
            geojson_path = get_geojson_path(f"secciones_{año}0101_tenerife.json")
            if not os.path.exists(geojson_path):
                continue

            try:
                gdf = gpd.read_file(geojson_path)
                # Reproducir exactamente la disolución que hacen los assets de mapa
                gdf["municipio"] = gdf["etiqueta"].str.extract(r"- (.+)$")
                municipios_gdf = set(gdf["municipio"].dropna().unique())
            except Exception as e:
                report_md += f"| `{fname}` | {año} | — | — | ⚠️ error GeoJSON: {e} |\n"
                continue

            # Municipios presentes en el CSV para ese año
            df_año = df[df[col_año] == año] if col_año else df
            municipios_csv = set(df_año[col_mun].dropna().unique())

            if not municipios_csv:
                report_md += f"| `{fname}` | {año} | 0 | 0 | ⚠️ sin datos |\n"
                continue

            match     = municipios_csv & municipios_gdf
            sin_match = municipios_csv - municipios_gdf
            cobertura = len(match) / len(municipios_csv)
            ok        = cobertura >= 0.8
            icono     = "🟢" if ok else "🔴"
            if not ok:
                passed = False

            report_md += (
                f"| `{fname}` | {año} | {len(municipios_csv)} "
                f"| {len(match)} | {icono} {cobertura:.0%} |\n"
            )
            if sin_match:
                report_md += (
                    f"|  |  | *Sin match:* | "
                    f"`{'`, `'.join(sorted(sin_match)[:10])}`"
                    f"{'…' if len(sin_match) > 10 else ''} | |\n"
                )

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Cobertura_Municipio": MetadataValue.md(report_md)},
    )

# ══════════════════════════════════════════════════════════════════════════════
# CHECKS DE PLOTS — DATOS DE ENTRADA
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset=plot_distribucion_lineas,
    description="Verifica precondiciones de datos para plot_distribucion_lineas.",
)
def check_datos_distribucion_lineas(context):
    """
    ¿Por qué este check?
    El gráfico de líneas con banda IQR necesita al menos 3 puntos temporales
    y los 5 componentes presentes para que la composición sea legible.
    Con n < 30 por componente el violín/banda IQR no es estadísticamente fiable.
    """
    cfg = get_plot_config()["distribucion_lineas"]
    file = get_processed_path(cfg["dataset"])
    df = pd.read_csv(file).dropna(subset=["OBS_VALUE"])
    passed = True
    report_md = "### Precondiciones: distribución_lineas\n\n"

    # 1. Componentes completos
    encontrados = set(df["MEDIDAS_CODE"].dropna().unique())
    faltantes   = COMPONENTES_DIST - encontrados
    ok = len(faltantes) == 0
    passed = passed and ok
    report_md += f"- Componentes completos: {'🟢' if ok else '🔴'} (faltan: {faltantes or '–'})\n"

    # 2. Tres años disponibles
    años = set(df["año"].dropna().unique())
    ok = AÑOS_ESPERADOS.issubset(años)
    passed = passed and ok
    report_md += f"- Años {AÑOS_ESPERADOS} presentes: {'🟢' if ok else '🔴'} (encontrados: {años})\n"

    # 3. n mínimo por componente (fiabilidad IQR)
    conteos = df.groupby("MEDIDAS_CODE")["OBS_VALUE"].count()
    insuf = conteos[conteos < 30].index.tolist()
    ok = len(insuf) == 0
    passed = passed and ok
    report_md += f"- n ≥ 30 por componente: {'🟢' if ok else '🔴'} (insuf: {insuf or '–'})\n"

    # 4. Sin porcentajes fuera de [0, 100]
    fuera = int(((df["OBS_VALUE"] < 0) | (df["OBS_VALUE"] > 100)).sum())
    ok = fuera == 0
    passed = passed and ok
    report_md += f"- OBS_VALUE ∈ [0,100]: {'🟢' if ok else '🔴'} ({fuera} fuera de rango)\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Lineas": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=plot_actividad_barras,
    description="Verifica precondiciones para plot_actividad_barras.",
)
def check_datos_actividad_barras(context):
    """
    ¿Por qué este check?
    Las barras apiladas son sensibles a valores nulos en num_casos
    (inflan o vacían silenciosamente un segmento) y a categorías
    de Sexo inesperadas que romperían la paleta manual.
    También verifica que el dataset es sc-3 (solo SC Tenerife),
    evitando que se cuelen municipios de otras provincias.
    """
    cfg = get_plot_config()["actividad_barras"]
    file = get_processed_path(cfg["dataset"])
    df = pd.read_csv(file)
    passed = True
    report_md = "### Precondiciones: actividad_barras\n\n"

    # 1. Columnas requeridas
    req = {"Actividad económica", "num_casos", "Periodo", "Sexo", "geocode"}
    falta = req - set(df.columns)
    ok = len(falta) == 0
    passed = passed and ok
    report_md += f"- Columnas requeridas: {'🟢' if ok else '🔴'} (faltan: {falta or '–'})\n"

    # 2. Solo municipios de SC de Tenerife
    mun_encontrados = set(df["municipio"].dropna().unique()) if "municipio" in df.columns else set()
    islas_ajenas = {inferir_isla(m) for m in mun_encontrados} - ISLAS_SC - {"Desconocida"}
    ok = len(islas_ajenas) == 0
    passed = passed and ok
    report_md += f"- Solo islas SC de Tenerife: {'🟢' if ok else '🔴'} (otras: {islas_ajenas or '–'})\n"

    # 3. Sexo solo Hombres/Mujeres/No consta
    extra_sexo = set(df["Sexo"].dropna().unique()) - SEXOS_ESPERADOS - {"No consta"} if "Sexo" in df.columns else set()
    ok = len(extra_sexo) == 0
    passed = passed and ok
    report_md += f"- Valores Sexo válidos: {'🟢' if ok else '🔴'} (extra: {extra_sexo or '–'})\n"

    # 4. Sin num_casos negativos
    neg = int((df["num_casos"].dropna() < 0).sum()) if "num_casos" in df.columns else 0
    ok = neg == 0
    passed = passed and ok
    report_md += f"- num_casos ≥ 0: {'🟢' if ok else '🔴'} ({neg} negativos)\n"

    # 5. Al menos 4 actividades válidas (sin "No consta")
    acts = df["Actividad económica"].dropna().unique() if "Actividad económica" in df.columns else []
    n_acts = sum(1 for a in acts if a != "No consta")
    ok = n_acts >= 4
    passed = passed and ok
    report_md += f"- Actividades válidas (sin 'No consta'): {'🟢' if ok else '🔴'} ({n_acts})\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Actividad": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=plot_ocupacion_divergente,
    description="Verifica precondiciones para plot_ocupacion_divergente.",
)
def check_datos_ocupacion_divergente(context):
    """
    ¿Por qué este check?
    El gráfico divergente H-M calcula una resta. Si falta uno de los dos sexos
    para alguna ocupación, el pivot produce NaN y la barra desaparece
    sin ningún aviso visible. Este check lo detecta antes del render.
    """
    cfg = get_plot_config()["ocupacion_divergente"]
    file = get_processed_path(cfg["dataset"])
    df = pd.read_csv(file).dropna(subset=["num_casos"])
    passed = True
    report_md = "### Precondiciones: ocupacion_divergente\n\n"

    # 1. Ambos sexos presentes para poder calcular brecha
    sexos = set(df["sexo"].dropna().unique()) if "sexo" in df.columns else set()
    ok = SEXOS_ESPERADOS.issubset(sexos)
    passed = passed and ok
    report_md += f"- Ambos sexos presentes: {'🟢' if ok else '🔴'} ({sexos})\n"

    # 2. Cada ocupación (sin "No consta") tiene datos de ambos sexos
    df_v = df[(df["sexo"].isin(SEXOS_ESPERADOS)) & (df["ocupacion"] != "No consta")]
    por_ocu_sexo = df_v.groupby(["ocupacion", "sexo"])["num_casos"].sum().unstack("sexo")
    incompletas = por_ocu_sexo[por_ocu_sexo.isna().any(axis=1)].index.tolist()
    ok = len(incompletas) == 0
    passed = passed and ok
    report_md += f"- Ocupaciones con datos de ambos sexos: {'🟢' if ok else '🔴'} (incompletas: {incompletas or '–'})\n"

    # 3. Al menos 3 ocupaciones válidas
    n_ocu = df_v["ocupacion"].nunique()
    ok = n_ocu >= 3
    passed = passed and ok
    report_md += f"- Ocupaciones válidas ≥ 3: {'🟢' if ok else '🔴'} ({n_ocu})\n"

    # 4. Brecha tiene valores a ambos lados de 0 (el gráfico divergente tiene sentido)
    brechas = por_ocu_sexo["Hombres"] - por_ocu_sexo["Mujeres"]
    ok = (brechas > 0).any() and (brechas < 0).any()
    passed = passed and ok
    report_md += f"- Divergencia real (valores + y −): {'🟢' if ok else '🔴'}\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Divergente": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=plot_renta_cajas,
    description="Verifica precondiciones para plot_renta_cajas.",
)
def check_datos_renta_cajas(context):
    """
    ¿Por qué este check?
    El boxplot + jitter ordena por mediana y filtra top 15. Si el dataset
    contiene municipios de otras islas, el top 15 puede no ser de Tenerife
    (bug observado en la imagen entregada). Este check garantiza el filtro
    y que la medida configurada existe en los datos.
    """
    cfg = get_plot_config()["renta_cajas"]
    file = get_processed_path(cfg["dataset"])
    medida = cfg["medida"]
    df = pd.read_csv(file).dropna(subset=["OBS_VALUE"])
    passed = True
    report_md = "### Precondiciones: renta_cajas\n\n"

    # 1. La medida configurada existe
    medidas = set(df["MEDIDAS_CODE"].dropna().unique()) if "MEDIDAS_CODE" in df.columns else set()
    ok = medida in medidas
    passed = passed and ok
    report_md += f"- Medida `{medida}` presente: {'🟢' if ok else '🔴'}\n"

    # 2. Solo municipios de Tenerife en el subconjunto filtrado
    rent = df[df["MEDIDAS_CODE"] == medida] if ok else df
    mun = set(rent["municipio"].dropna().unique()) if "municipio" in rent.columns else set()
    ajenos = {m for m in mun if inferir_isla(m) not in ("Tenerife", "Desconocida")}
    ok = len(ajenos) == 0
    passed = passed and ok
    report_md += (
        f"- Sin municipios de otras islas: {'🟢' if ok else f'🔴 Detectados: {ajenos}'}\n"
        f"  ➜ **Solución**: filtrar `df[df[\"municipio\"].isin(MUNICIPIOS_TENERIFE)]` antes del top15.\n"
        if not ok else
        f"- Sin municipios de otras islas: 🟢\n"
    )

    # 3. Top 15 alcanzable (≥ 15 municipios de Tenerife con datos)
    mun_tenerife = {m for m in mun if inferir_isla(m) == "Tenerife"}
    ok = len(mun_tenerife) >= 15
    passed = passed and ok
    report_md += f"- Municipios Tenerife con datos ≥ 15: {'🟢' if ok else '🔴'} ({len(mun_tenerife)})\n"

    # 4. Valores de renta en rango razonable [5000, 300000]
    fuera = int(((rent["OBS_VALUE"] < 5_000) | (rent["OBS_VALUE"] > 300_000)).sum())
    ok = fuera == 0
    passed = passed and ok
    report_md += f"- OBS_VALUE ∈ [5k, 300k]: {'🟢' if ok else '🔴'} ({fuera} fuera de rango)\n"

    # 5. Los 3 años presentes (necesario para que el jitter por año sea completo)
    años = set(rent["año"].dropna().unique()) if "año" in rent.columns else set()
    ok = AÑOS_ESPERADOS.issubset(años)
    passed = passed and ok
    report_md += f"- Años {AÑOS_ESPERADOS} presentes: {'🟢' if ok else '🔴'} ({años})\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Cajas": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=plot_mapa_distribucion_renta,
    description="Verifica precondiciones para plot_mapa_distribucion_renta.",
)
def check_datos_mapa_distribucion(context):
    """
    ¿Por qué este check?
    Un mapa con cobertura baja muestra secciones grises sin aviso;
    y una escala secuencial centrada en un outlier extremo aplana
    el gradiente del resto. Este check detecta ambos problemas.
    """
    cfg = get_plot_config()["mapa_distribucion"]
    año = cfg["ano"]
    componente = cfg["componente"]
    file = get_processed_path(cfg["dataset"])
    df = pd.read_csv(file).dropna(subset=["OBS_VALUE"])
    df_fil = df[(df["año"] == año) & (df["MEDIDAS_CODE"] == componente)]
    passed = True
    report_md = f"### Precondiciones: mapa_distribucion ({componente}, {año})\n\n"

    # 1. Registros del año/componente configurado
    n = len(df_fil)
    ok = n > 0
    passed = passed and ok
    report_md += f"- Registros año={año}, componente={componente}: {'🟢' if ok else '🔴'} ({n})\n"

    # 2. GeoJSON del año configurado existe
    geojson = get_geojson_path(f"secciones_{año}0101_tenerife.json")
    ok = os.path.exists(geojson)
    passed = passed and ok
    report_md += f"- GeoJSON `secciones_{año}0101_tenerife.json` existe: {'🟢' if ok else '🔴'}\n"

    # 3. Cobertura join ≥ 80%
    if ok and n > 0:
        try:
            gdf = gpd.read_file(geojson)
            geo_gdf = set(gdf["geocode"].dropna().apply(lambda x: "_".join(str(x).split("_")[1:])))
            geo_csv = set(df_fil["TERRITORIO_CODE"].dropna().apply(lambda x: "_".join(str(x).split("_")[1:])))
            cob = len(geo_csv & geo_gdf) / len(geo_csv) if geo_csv else 0
            ok_cob = cob >= 0.8
            passed = passed and ok_cob
            report_md += f"- Cobertura join: {'🟢' if ok_cob else '🔴'} {cob:.0%}\n"
        except Exception as e:
            report_md += f"- Cobertura join: ⚠️ No computable ({e})\n"

    # 4. Sin outliers extremos que aplasten la escala (IQR × 3)
    if n > 0:
        q1, q3 = df_fil["OBS_VALUE"].quantile([0.25, 0.75])
        iqr = q3 - q1
        outliers = int(((df_fil["OBS_VALUE"] < q1 - 3 * iqr) | (df_fil["OBS_VALUE"] > q3 + 3 * iqr)).sum())
        ok = outliers == 0
        report_md += f"- Outliers extremos (IQR×3): {'🟢' if ok else '⚠️'} {outliers} detectados\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Mapa_Dist": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=plot_brecha_salarial,
    description="Verifica precondiciones para plot_brecha_salarial (slope chart).",
)
def check_datos_brecha_salarial(context):
    """
    ¿Por qué este check?
    El slope chart cruza dos datasets. Si el join municipio×año falla
    (municipios con nombres inconsistentes entre CSVs), los segmentos
    del slope desaparecen sin ningún error visible.
    """
    cfg = get_plot_config()["brecha_salarial"]
    ocu  = pd.read_csv(get_processed_path(cfg["dataset_ocu"])).dropna(subset=["num_casos"])
    dist = pd.read_csv(get_processed_path(cfg["dataset_dist"])).dropna(subset=["OBS_VALUE"])
    AÑO_INI, AÑO_FIN, TOP_N = cfg["ano_ini"], cfg["ano_fin"], cfg["top_n"]
    passed = True
    report_md = "### Precondiciones: brecha_salarial\n\n"

    # 1. Ambos datasets contienen los años ini y fin
    for df_tmp, nombre in [(ocu, "ocupacion"), (dist, "distribucion")]:
        años = set(df_tmp["año"].dropna().unique())
        ok = {AÑO_INI, AÑO_FIN}.issubset(años)
        passed = passed and ok
        report_md += f"- `{nombre}` tiene años {AÑO_INI}/{AÑO_FIN}: {'🟢' if ok else '🔴'} ({años})\n"

    # 2. SUELDOS_SALARIOS presente en distribución
    comp = set(dist["MEDIDAS_CODE"].dropna().unique())
    ok = "SUELDOS_SALARIOS" in comp
    passed = passed and ok
    report_md += f"- SUELDOS_SALARIOS en distribución: {'🟢' if ok else '🔴'}\n"

    # 3. Municipios comunes suficientes para el join (≥ TOP_N)
    mun_ocu  = set(ocu["municipio"].dropna().unique())
    mun_dist = set(dist["municipio"].dropna().unique())
    comunes  = mun_ocu & mun_dist
    ok = len(comunes) >= TOP_N
    passed = passed and ok
    report_md += f"- Municipios comunes ≥ {TOP_N}: {'🟢' if ok else '🔴'} ({len(comunes)})\n"

    # 4. Ambos sexos presentes en ocupación
    sexos = set(ocu["sexo"].dropna().unique())
    ok = SEXOS_ESPERADOS.issubset(sexos)
    passed = passed and ok
    report_md += f"- Ambos sexos en ocupación: {'🟢' if ok else '🔴'} ({sexos})\n"

    # 5. El índice resultante tiene varianza > 0
    ocu_hm = (
        ocu[ocu["sexo"].isin(SEXOS_ESPERADOS)]
        .groupby(["municipio", "año", "sexo"])["num_casos"].sum()
        .unstack("sexo").reset_index()
    )
    ocu_hm["ratio_hm"] = ocu_hm["Hombres"] / (ocu_hm["Hombres"] + ocu_hm["Mujeres"])
    ok = ocu_hm["ratio_hm"].std() > 0
    passed = passed and ok
    report_md += f"- Varianza del ratio H/M > 0: {'🟢' if ok else '🔴'} (std={ocu_hm['ratio_hm'].std():.4f})\n"

    # 6. Divergencia real (valores + y − en el índice)
    sal = dist[dist["MEDIDAS_CODE"] == "SUELDOS_SALARIOS"].groupby(["municipio", "año"])["OBS_VALUE"].median().reset_index()
    merged = ocu_hm.merge(sal, on=["municipio", "año"])
    merged["indice"] = (merged["ratio_hm"] - 0.5) * merged["OBS_VALUE"]
    ok = (merged["indice"] > 0).any() and (merged["indice"] < 0).any()
    passed = passed and ok
    report_md += f"- Índice con valores + y −: {'🟢' if ok else '🔴'}\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Brecha": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=plot_mapa_brecha_salarial,
    description="Verifica precondiciones para plot_mapa_brecha_salarial.",
)
def check_datos_mapa_brecha(context):
    """
    ¿Por qué este check?
    El mapa divergente usa TwoSlopeNorm centrado en 0. Si todos los
    valores del índice son positivos (sin valores negativos), la norma
    lanza un error en matplotlib. Este check lo previene.
    También verifica que la disolución sección→municipio produce
    un número razonable de polígonos.
    """
    cfg = get_plot_config()["brecha_salarial"]
    AÑO_MAPA = cfg.get("ano_mapa", 2023)
    passed = True
    report_md = f"### Precondiciones: mapa_brecha_salarial (año={AÑO_MAPA})\n\n"

    # 1. GeoJSON del año mapa existe
    geojson = get_geojson_path(f"secciones_{AÑO_MAPA}0101_tenerife.json")
    ok = os.path.exists(geojson)
    passed = passed and ok
    report_md += f"- GeoJSON año={AÑO_MAPA}: {'🟢' if ok else '🔴'}\n"

    # 2. Disolución produce ≥ 30 municipios
    if ok:
        try:
            gdf = gpd.read_file(geojson).set_crs("EPSG:4326", allow_override=True)
            gdf["municipio"] = gdf["etiqueta"].str.extract(r"- (.+)$")
            n_mun = gdf["municipio"].nunique()
            ok2 = n_mun >= 30
            passed = passed and ok2
            report_md += f"- Municipios extraídos de etiqueta ≥ 30: {'🟢' if ok2 else '🔴'} ({n_mun})\n"
            # 3. Geometrías válidas
            inv = (~gdf.geometry.is_valid).sum()
            ok3 = inv == 0
            passed = passed and ok3
            report_md += f"- Geometrías válidas: {'🟢' if ok3 else '🔴'} ({inv} inválidas)\n"
        except Exception as e:
            report_md += f"- Error leyendo GeoJSON: ⚠️ {e}\n"

    # 4. TwoSlopeNorm requiere valores + y − en el índice
    ocu  = pd.read_csv(get_processed_path(cfg["dataset_ocu"])).dropna(subset=["num_casos"])
    dist = pd.read_csv(get_processed_path(cfg["dataset_dist"])).dropna(subset=["OBS_VALUE"])
    ocu_hm = (
        ocu[ocu["sexo"].isin(SEXOS_ESPERADOS)]
        .groupby(["municipio", "año", "sexo"])["num_casos"].sum()
        .unstack("sexo").reset_index()
    )
    ocu_hm["ratio_hm"] = ocu_hm["Hombres"] / (ocu_hm["Hombres"] + ocu_hm["Mujeres"])
    sal = dist[dist["MEDIDAS_CODE"] == "SUELDOS_SALARIOS"].groupby(["municipio", "año"])["OBS_VALUE"].median().reset_index()
    merged = ocu_hm.merge(sal, on=["municipio", "año"])
    merged["indice"] = (merged["ratio_hm"] - 0.5) * merged["OBS_VALUE"]
    idx_año = merged[merged["año"] == AÑO_MAPA]["indice"]
    ok = (idx_año > 0).any() and (idx_año < 0).any()
    passed = passed and ok
    report_md += f"- TwoSlopeNorm viable (+ y − en año={AÑO_MAPA}): {'🟢' if ok else '🔴'}\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Mapa_Brecha": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=plot_mapa_generico,
    description="Verifica precondiciones para plot_mapa_generico.",
)
def check_datos_mapa_generico(context):
    """
    ¿Por qué este check?
    El mapa genérico es el más flexible del pipeline pero el más
    frágil: cualquier combinación dataset/filtro/año puede producir
    0 registros si la configuración es inconsistente.
    """
    cfg = get_plot_config()["mapa_generico"]
    año = cfg["ano"]
    file = get_processed_path(cfg["dataset"])
    df = pd.read_csv(file)
    passed = True
    report_md = f"### Precondiciones: mapa_generico (año={año})\n\n"

    # 1. GeoJSON existe
    geojson = get_geojson_path(f"secciones_{año}0101_tenerife.json")
    ok = os.path.exists(geojson)
    passed = passed and ok
    report_md += f"- GeoJSON año={año}: {'🟢' if ok else '🔴'}\n"

    # 2. Año presente en el dataset
    col_año = "año" if "año" in df.columns else "Periodo" if "Periodo" in df.columns else None
    if col_año:
        ok = año in df[col_año].dropna().unique()
        passed = passed and ok
        report_md += f"- Año {año} en dataset: {'🟢' if ok else '🔴'}\n"

    # 3. Filtros configurados producen registros > 0
    mask = df[col_año] == año if col_año else pd.Series([True] * len(df))
    for param, col in [("filtro_medida", "MEDIDAS_CODE"),
                       ("filtro_actividad", "Actividad económica"),
                       ("filtro_ocupacion", "ocupacion"),
                       ("filtro_sexo", "Sexo")]:
        val = cfg.get(param)
        if val and col in df.columns:
            mask = mask & (df[col] == val)
    n = int(mask.sum())
    ok = n > 0
    passed = passed and ok
    report_md += f"- Registros tras aplicar filtros: {'🟢' if ok else '🔴'} ({n})\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Mapa_Generico": MetadataValue.md(report_md)},
    )


@asset_check(
    asset=plot_renta_violin,
    description="Verifica precondiciones para el violín de renta por fuente de ingresos.",
)
def check_datos_renta_violin(context):
    """
    ¿Por qué este check?
    El violín no es fiable con n < 30 por grupo (la estimación KDE
    produce formas artefactuales). Este check garantiza masa suficiente
    y que los 5 componentes están presentes para que la comparación
    sea completa.
    """
    file = get_processed_path("distribucion-renta-ingresos.csv")
    df = pd.read_csv(file).dropna(subset=["OBS_VALUE"])
    passed = True
    report_md = "### Precondiciones: renta_violin\n\n"

    faltantes = COMPONENTES_DIST - set(df["MEDIDAS_CODE"].dropna().unique())
    ok = len(faltantes) == 0
    passed = passed and ok
    report_md += f"- Todos los componentes presentes: {'🟢' if ok else '🔴'} (faltan: {faltantes or '–'})\n"

    conteos = df.groupby("MEDIDAS_CODE")["OBS_VALUE"].count()
    insuf = conteos[conteos < 30].index.tolist()
    ok = len(insuf) == 0
    passed = passed and ok
    report_md += f"- n ≥ 30 por componente (KDE fiable): {'🟢' if ok else '🔴'} (insuf: {insuf or '–'})\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"Check_Violin": MetadataValue.md(report_md)},
    )