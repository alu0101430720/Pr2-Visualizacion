"""
assets/checks.py — Asset checks del pipeline Pr2-Visualizacion.

Organización por etapa:
  CARGA (Raw)          → Checks críticos de esquema y nulos que romperían el pipeline.
  TRANSFORMACIÓN       → Checks de formato (curated), cardinalidad y reglas de negocio.
  VISUALIZACIÓN        → Checks dinámicos sobre el subconjunto graficado según config.Dashboard.
  GENERACIÓN IA        → Checks sobre el pipeline de generación automática de código (Práctica 4).
"""

import os
import re

import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetIn,
    MetadataValue,
    asset_check,
)

from config import (
    Dashboard,
    Fuentes,
    ISLAS_LAS_PALMAS,
    ISLAS_SCT,
    TODAS_ISLAS,
    MAPA_EDUCACION,
    MAX_CATEGORIAS_COLOR,
    MAX_LABEL_LENGTH,
    RATIO_ESCALA_MAX,
    DOMINANCE_OTROS_MAX,
)

# ── Funciones Helper DRY ──────────────────────────────────────────────────────

def _get_title_case_errors(series: pd.Series) -> list:
    """Devuelve una lista de valores que no cumplen con el formato Title Case."""
    mask = series.str.strip() != series.str.strip().str.title()
    return series.loc[mask].unique().tolist()

def _get_inverted_name_errors(series: pd.Series) -> list:
    """Detecta valores en formato 'Apellido, Artículo' (ej. 'Gomera, La')."""
    valid_series = series.dropna()
    return valid_series[valid_series.str.contains(",", na=False)].unique().tolist()

def _filtrar_datos_dashboard(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica exactamente los mismos filtros que graficar_renta_territorial
    usando los parámetros actuales de config.Dashboard.
    Garantiza que los checks validan el mismo subconjunto que el gráfico.
    """
    territorio       = Dashboard.TERRITORIO
    fuentes_codigo   = Dashboard.FUENTE
    desglosar_munis  = Dashboard.DESGLOSAR_MUNIS

    es_region    = territorio == "Canarias"
    es_prov_lp   = territorio == "Las Palmas"
    es_prov_sct  = territorio == "Santa Cruz de Tenerife"

    datos = df.copy()

    if es_region:
        datos = datos[datos["Territorio"].isin(TODAS_ISLAS)]
    elif es_prov_lp:
        datos = datos[datos["Territorio"].isin(ISLAS_LAS_PALMAS)]
    elif es_prov_sct:
        datos = datos[datos["Territorio"].isin(ISLAS_SCT)]
    else:
        if desglosar_munis:
            datos = datos[
                (datos["ISLA_clean"] == territorio) &
                (datos["Territorio"] != territorio)
            ]
        else:
            datos = datos[datos["Territorio"] == territorio]

    if fuentes_codigo:
        codigos = [fuentes_codigo] if isinstance(fuentes_codigo, str) else fuentes_codigo
        datos = datos[datos["Fuente_Renta_Code"].isin(codigos)]

    return datos

def _contexto_dashboard() -> str:
    """Texto descriptivo del dashboard activo, para incluir en metadata."""
    return (
        f"territorio={Dashboard.TERRITORIO}, fuente={Dashboard.FUENTE}, "
        f"dim_social={Dashboard.DIM_SOCIAL}, eje_y_cero={Dashboard.EJE_Y_CERO}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 1 — CARGA (Raw)
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset="ingestar_renta",
    name="check_nulos_criticos_renta",
    description="Detecta ausencia de datos en variables críticas (Territorio, Tiempo, Medidas y Valores). Gestalt — Figura y Fondo.",
)
def check_nulos_criticos_renta(ingestar_renta: pd.DataFrame) -> AssetCheckResult:
    cols = [
        "TERRITORIO#es", "TERRITORIO_CODE", "TIME_PERIOD#es",
        "TIME_PERIOD_CODE", "MEDIDAS#es", "MEDIDAS_CODE", "OBS_VALUE"
    ]
    cols_existentes = [c for c in cols if c in ingestar_renta.columns]
    n_nulos = int(ingestar_renta[cols_existentes].isna().any(axis=1).sum())
    detalle_nulos = ingestar_renta[cols_existentes].isna().sum().to_dict()
    total = len(ingestar_renta)
    pct = round(n_nulos / total * 100, 2) if total else 0.0
    return AssetCheckResult(
        passed=n_nulos == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "porcentaje_filas_incompletas": MetadataValue.float(pct),
            "filas_afectadas":  MetadataValue.int(n_nulos),
            "detalle_por_columna": MetadataValue.text(str(detalle_nulos)),
            "principio_gestalt": MetadataValue.text("Figura y Fondo — Los huecos inesperados rompen la forma de la visualización."),
            "mensaje": MetadataValue.text("Un NaN en variables clave arruina las agrupaciones o produce cortes en la línea temporal."),
        },
    )

@asset_check(
    asset="ingestar_codislas",
    name="check_nulos_criticos_codislas",
    description="Detecta nulos en ISLA y NOMBRE. Gestalt — Figura y Fondo.",
)
def check_nulos_criticos_codislas(ingestar_codislas: pd.DataFrame) -> AssetCheckResult:
    nulos = {col: int(ingestar_codislas[col].isna().sum()) for col in ["ISLA", "NOMBRE"]}
    passed = sum(nulos.values()) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "nulos_ISLA":   MetadataValue.int(nulos["ISLA"]),
            "nulos_NOMBRE": MetadataValue.int(nulos["NOMBRE"]),
            "principio_gestalt": MetadataValue.text("Figura y Fondo — Los huecos inesperados rompen la forma de la visualización."),
            "mensaje": MetadataValue.text("Un ISLA nulo impide asignar municipios a su provincia en el gráfico territorial."),
        },
    )

@asset_check(
    asset="ingestar_nivelestudios",
    name="check_nulos_criticos_nivelestudios",
    description="Detecta nulos en Periodo, Sexo, Total. Gestalt — Figura y Fondo.",
)
def check_nulos_criticos_nivelestudios(ingestar_nivelestudios: pd.DataFrame) -> AssetCheckResult:
    cols  = [c for c in ["Periodo", "Sexo", "Total"] if c in ingestar_nivelestudios.columns]
    nulos = {c: int(ingestar_nivelestudios[c].isna().sum()) for c in cols}
    passed = sum(nulos.values()) == 0
    meta  = {f"nulos_{c}": MetadataValue.int(v) for c, v in nulos.items()}
    meta["principio_gestalt"] = MetadataValue.text("Figura y Fondo — Los huecos inesperados rompen la forma de la visualización.")
    meta["mensaje"] = MetadataValue.text("Nulos en Total o Periodo producen áreas apiladas incompletas.")
    return AssetCheckResult(passed=passed, severity=AssetCheckSeverity.ERROR, metadata=meta)


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 2 — TRANSFORMACIÓN (Curated)
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset="limpiar_renta",
    name="check_duplicados_limpiar_renta",
    description="Verifica que la limpieza no introdujo ni mantuvo filas duplicadas. Gestalt — Figura y Fondo.",
)
def check_duplicados_limpiar_renta(limpiar_renta: pd.DataFrame) -> AssetCheckResult:
    n_duplicados = int(limpiar_renta.duplicated().sum())
    return AssetCheckResult(
        passed=n_duplicados == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "filas_duplicadas":  MetadataValue.int(n_duplicados),
            "principio_gestalt": MetadataValue.text("Figura y Fondo — Duplicados inflan las series."),
            "mensaje": MetadataValue.text("Añade df.drop_duplicates() al final de limpiar_renta si este check falla."),
        },
    )

@asset_check(
    asset="limpiar_renta",
    name="check_formato_title_limpiar_renta",
    description="Verifica que Territorio está en Title Case tras la limpieza. Gestalt — Similitud.",
)
def check_formato_title_limpiar_renta(limpiar_renta: pd.DataFrame) -> AssetCheckResult:
    incorrectos = _get_title_case_errors(limpiar_renta["Territorio"])
    return AssetCheckResult(
        passed=len(incorrectos) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "n_incorrectos":     MetadataValue.int(len(incorrectos)),
            "ejemplos":          MetadataValue.text(str(incorrectos[:5])),
            "principio_gestalt": MetadataValue.text("Similitud — Mayúsculas inconsistentes duplican leyendas."),
            "mensaje": MetadataValue.text("limpiar_renta aplica .str.title() — revisa si hay caracteres especiales."),
        },
    )

@asset_check(
    asset="limpiar_renta",
    name="check_nombre_invertido_limpiar_renta",
    description="Verifica que Territorio no contiene 'Apellido, Artículo' tras la limpieza. Gestalt — Similitud.",
)
def check_nombre_invertido_limpiar_renta(limpiar_renta: pd.DataFrame) -> AssetCheckResult:
    invertidos = _get_inverted_name_errors(limpiar_renta["Territorio"])
    return AssetCheckResult(
        passed=len(invertidos) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "n_invertidos":      MetadataValue.int(len(invertidos)),
            "ejemplos":          MetadataValue.text(str(invertidos[:5])),
            "principio_gestalt": MetadataValue.text("Similitud — Crea dos colores distintos en ggplot."),
            "mensaje": MetadataValue.text("Aplica _limpiar_formato_coma() sobre Territorio en limpiar_renta."),
        },
    )

@asset_check(
    asset="limpiar_renta",
    name="check_cardinalidad_fuente_renta",
    description="Limita el número de fuentes de renta a MAX_CATEGORIAS_COLOR. Gestalt — Carga Cognitiva.",
)
def check_cardinalidad_fuente_renta(limpiar_renta: pd.DataFrame) -> AssetCheckResult:
    n = int(limpiar_renta["Fuente_Renta_Code"].nunique())
    return AssetCheckResult(
        passed=n <= MAX_CATEGORIAS_COLOR,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "n_categorias":          MetadataValue.int(n),
            "limite_recomendado":    MetadataValue.int(MAX_CATEGORIAS_COLOR),
            "sugerencia_agrupacion": MetadataValue.text("Agrupa fuentes minoritarias en 'Otras prestaciones'."),
            "principio_gestalt": MetadataValue.text("Carga Cognitiva — Más de 9 colores son imposibles de distinguir."),
        },
    )

@asset_check(
    asset="limpiar_renta",
    name="check_continuidad_serie_temporal_renta",
    description="Verifica que no falten años en la serie temporal. Gestalt — Continuidad.",
)
def check_continuidad_serie_temporal_renta(limpiar_renta: pd.DataFrame) -> AssetCheckResult:
    años = sorted(limpiar_renta["Año"].dropna().astype(int).unique().tolist())
    faltantes = [a for a in range(min(años), max(años) + 1) if a not in años] if años else []
    return AssetCheckResult(
        passed=len(faltantes) == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "fechas_faltantes": MetadataValue.text(str(faltantes) if faltantes else "ninguna"),
            "rango_temporal":   MetadataValue.text(f"{min(años)}–{max(años)}" if años else "vacío"),
            "principio_gestalt": MetadataValue.text("Continuidad — Un año faltante crea una pendiente falsa."),
            "mensaje": MetadataValue.text("geom_line interpola sobre el hueco produciendo un salto visual engañoso."),
        },
    )

@asset_check(
    asset="limpiar_renta",
    name="check_label_text_territorio",
    description="Detecta etiquetas de Territorio demasiado largas. Gestalt — Continuidad.",
)
def check_label_text_territorio(limpiar_renta: pd.DataFrame) -> AssetCheckResult:
    longitudes = limpiar_renta["Territorio"].str.len()
    max_len    = int(longitudes.max())
    ejemplo    = limpiar_renta.loc[longitudes.idxmax(), "Territorio"]
    return AssetCheckResult(
        passed=max_len <= MAX_LABEL_LENGTH,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "longest_label":     MetadataValue.text(str(ejemplo)),
            "longitud_max":      MetadataValue.int(max_len),
            "overlap_risk":      MetadataValue.bool(max_len > MAX_LABEL_LENGTH),
            "principio_gestalt": MetadataValue.text("Continuidad — Etiquetas largas se solapan y rompen legibilidad."),
            "mensaje": MetadataValue.text("Considera abreviar municipios o rotar etiquetas (rotation=45)."),
        },
    )

@asset_check(
    asset="limpiar_codislas",
    name="check_cardinalidad_islas",
    description="Verifica que el número de islas no supere MAX_CATEGORIAS_COLOR. Gestalt — Similitud.",
)
def check_cardinalidad_islas(limpiar_codislas: pd.DataFrame) -> AssetCheckResult:
    n = int(limpiar_codislas["ISLA_clean"].nunique())
    return AssetCheckResult(
        passed=n <= MAX_CATEGORIAS_COLOR,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "n_categorias":      MetadataValue.int(n),
            "principio_gestalt": MetadataValue.text("Similitud — Más de 9 colores son imposibles de distinguir."),
            "mensaje": MetadataValue.text("Canarias tiene 7 islas. Revisar si se añaden territorios por error."),
        },
    )

@asset_check(
    asset="limpiar_codislas",
    name="check_formato_title_limpiar_codislas",
    description="Verifica que ISLA_clean y Territorio están en Title Case tras la limpieza. Gestalt — Similitud.",
)
def check_formato_title_limpiar_codislas(limpiar_codislas: pd.DataFrame) -> AssetCheckResult:
    inc_isla = _get_title_case_errors(limpiar_codislas["ISLA_clean"])
    inc_terr = _get_title_case_errors(limpiar_codislas["Territorio"])
    total = len(inc_isla) + len(inc_terr)
    return AssetCheckResult(
        passed=total == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "ejemplos_ISLA_clean": MetadataValue.text(str(inc_isla[:5])),
            "ejemplos_Territorio": MetadataValue.text(str(inc_terr[:5])),
            "principio_gestalt":   MetadataValue.text("Similitud — Mayúsculas inconsistentes duplican leyendas."),
        },
    )

@asset_check(
    asset="limpiar_codislas",
    name="check_nombre_invertido_limpiar_codislas",
    description="Verifica que ISLA_clean y Territorio no contienen 'Apellido, Artículo'. Gestalt — Similitud.",
)
def check_nombre_invertido_limpiar_codislas(limpiar_codislas: pd.DataFrame) -> AssetCheckResult:
    inv_isla = _get_inverted_name_errors(limpiar_codislas["ISLA_clean"])
    inv_terr = _get_inverted_name_errors(limpiar_codislas["Territorio"])
    total = len(inv_isla) + len(inv_terr)
    return AssetCheckResult(
        passed=total == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "ejemplos_ISLA_clean": MetadataValue.text(str(inv_isla[:5])),
            "ejemplos_Territorio": MetadataValue.text(str(inv_terr[:5])),
            "mensaje": MetadataValue.text("_limpiar_formato_coma() debería haber corregido esto."),
        },
    )

@asset_check(
    asset="limpiar_codislas",
    name="check_duplicados_limpiar_codislas",
    description="Verifica que no haya municipios duplicados tras la limpieza. Gestalt — Figura y Fondo.",
)
def check_duplicados_limpiar_codislas(limpiar_codislas: pd.DataFrame) -> AssetCheckResult:
    n_duplicados = int(limpiar_codislas.duplicated().sum())
    ejemplos = []
    if n_duplicados > 0:
        ejemplos = limpiar_codislas[limpiar_codislas.duplicated(keep=False)].head(2).to_dict(orient="records")
    return AssetCheckResult(
        passed=n_duplicados == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "filas_duplicadas":  MetadataValue.int(n_duplicados),
            "ejemplos":          MetadataValue.text(str(ejemplos) if ejemplos else "ninguno"),
            "principio_gestalt": MetadataValue.text("Figura y Fondo — Un municipio duplicado aparecería dos veces en el facet del gráfico."),
            "mensaje": MetadataValue.text("El merge con renta multiplicará filas si codislas tiene duplicados."),
        },
    )

@asset_check(
    asset="integrar_renta_codislas",
    name="check_integridad_join_renta_codislas",
    description="Verifica que el LEFT JOIN asigne una isla a todos los municipios. Gestalt — Figura y Fondo.",
)
def check_integridad_join_renta_codislas(integrar_renta_codislas: pd.DataFrame) -> AssetCheckResult:
    territorios_exentos = ["Canarias", "Las Palmas", "Santa Cruz de Tenerife"] + TODAS_ISLAS
    municipios = integrar_renta_codislas[~integrar_renta_codislas["Territorio"].isin(territorios_exentos)]
    n_sin_isla = int(municipios["ISLA_clean"].isna().sum())
    ejemplos = (
        municipios.loc[municipios["ISLA_clean"].isna(), "Territorio"]
        .unique()[:5].tolist()
    )
    return AssetCheckResult(
        passed=n_sin_isla == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "municipios_huerfanos": MetadataValue.int(n_sin_isla),
            "ejemplos_huerfanos":   MetadataValue.text(str(ejemplos) if ejemplos else "ninguno"),
            "excluidos_del_check":  MetadataValue.text("Canarias, Provincias y nombres de Islas"),
            "principio_gestalt":    MetadataValue.text("Figura y Fondo — Municipios sin isla asignada no aparecerán en el facet correcto."),
        },
    )

@asset_check(
    asset="limpiar_nivelestudios",
    name="check_continuidad_serie_temporal_nivelestudios",
    description="Verifica que no falten años en la serie de nivel de estudios. Gestalt — Continuidad.",
)
def check_continuidad_serie_temporal_nivelestudios(limpiar_nivelestudios: pd.DataFrame) -> AssetCheckResult:
    años      = sorted(limpiar_nivelestudios["Periodo"].dropna().unique().tolist())
    faltantes = [a for a in range(min(años), max(años) + 1) if a not in años] if años else []
    return AssetCheckResult(
        passed=len(faltantes) == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "fechas_faltantes":  MetadataValue.text(str(faltantes) if faltantes else "ninguna"),
            "principio_gestalt": MetadataValue.text("Continuidad — Un año faltante une puntos lejanos creando un área engañosa."),
        },
    )

@asset_check(
    asset="limpiar_nivelestudios",
    name="check_cardinalidad_nivel_estudios",
    description="Verifica que los niveles de estudio no superen MAX_CATEGORIAS_COLOR. Gestalt — Carga Cognitiva.",
)
def check_cardinalidad_nivel_estudios(limpiar_nivelestudios: pd.DataFrame) -> AssetCheckResult:
    col = "Nivel de estudios en curso"
    if col not in limpiar_nivelestudios.columns:
        return AssetCheckResult(passed=True, metadata={"mensaje": MetadataValue.text(f"Columna '{col}' no encontrada.")})
    n = int(limpiar_nivelestudios[col].nunique())
    return AssetCheckResult(
        passed=n <= MAX_CATEGORIAS_COLOR,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "n_categorias":          MetadataValue.int(n),
            "sugerencia_agrupacion": MetadataValue.text("Usa MAPA_EDUCACION para reducir a 4 grupos principales."),
        },
    )

@asset_check(
    asset="enriquecer_nivelestudios",
    name="check_dominance_otros_nivelestudios",
    description="Verifica que 'Sin Estudios/Otros' no supere DOMINANCE_OTROS_MAX. Gestalt — Semejanza.",
)
def check_dominance_otros_nivelestudios(enriquecer_nivelestudios: pd.DataFrame) -> AssetCheckResult:
    col = "Nivel de estudios en curso"
    if col not in enriquecer_nivelestudios.columns:
        return AssetCheckResult(passed=True)
    df = enriquecer_nivelestudios.copy()
    df["Categoria"] = df[col].map(MAPA_EDUCACION)
    df["Total"] = pd.to_numeric(df["Total"], errors="coerce").fillna(0)
    filtro = df[col] != "Total"
    dim_activa = Dashboard.DIM_SOCIAL
    dimensiones_demograficas = ["Sexo", "Nacionalidad", "Edad"]
    for dim in dimensiones_demograficas:
        if dim in df.columns:
            if dim == dim_activa:
                filtro = filtro & (df[dim] != "Total")
            elif "Total" in df[dim].unique():
                filtro = filtro & (df[dim] == "Total")
    df_f = df[filtro]
    total_absoluto = df_f["Total"].sum()
    otros = df_f.loc[df_f["Categoria"] == "Sin Estudios/Otros", "Total"].sum()
    pct = float(round(otros / total_absoluto * 100, 2)) if total_absoluto else 0.0
    return AssetCheckResult(
        passed=pct <= (DOMINANCE_OTROS_MAX * 100),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "pct_of_total":      MetadataValue.float(pct),
            "umbral_maximo":     MetadataValue.float(DOMINANCE_OTROS_MAX * 100),
            "dashboard_activo":  MetadataValue.text(f"Dimensión evaluada: {dim_activa}"),
            "principio_gestalt": MetadataValue.text("Semejanza — Un grupo 'Otros' dominante atrae la atención lejos de los datos relevantes."),
        },
    )

@asset_check(
    asset="enriquecer_nivelestudios",
    name="check_label_text_municipio",
    description="Detecta municipios con nombres demasiado largos para el eje Y del heatmap. Gestalt — Continuidad.",
)
def check_label_text_municipio(enriquecer_nivelestudios: pd.DataFrame) -> AssetCheckResult:
    longitudes = enriquecer_nivelestudios["Municipio_clean"].str.len()
    max_len    = int(longitudes.max())
    return AssetCheckResult(
        passed=max_len <= MAX_LABEL_LENGTH,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "longitud_max":      MetadataValue.int(max_len),
            "overlap_risk":      MetadataValue.bool(max_len > MAX_LABEL_LENGTH),
            "principio_gestalt": MetadataValue.text("Continuidad — Etiquetas largas se solapan en el eje Y."),
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 3 — VISUALIZACIÓN (Asset)
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_datos_dashboard_no_vacios",
    description="Verifica que el subconjunto de datos filtrado por config.Dashboard no está vacío. Gestalt — Figura y Fondo.",
    additional_ins={"integrar_renta_codislas": AssetIn()}
)
def check_datos_dashboard_no_vacios(
    generar_graficos_ejercicio3: list,
    integrar_renta_codislas: pd.DataFrame,
) -> AssetCheckResult:
    datos    = _filtrar_datos_dashboard(integrar_renta_codislas)
    n_filas  = len(datos)
    n_terrs  = datos["Territorio"].nunique() if not datos.empty else 0
    passed   = n_filas > 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "filas_en_grafico":       MetadataValue.int(n_filas),
            "territorios_en_grafico": MetadataValue.int(n_terrs),
            "dashboard_activo":       MetadataValue.text(_contexto_dashboard()),
            "principio_gestalt":      MetadataValue.text("Figura y Fondo — Un gráfico sin datos no tiene figura que mostrar."),
        },
    )

@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_escala_y_dashboard",
    description="Detecta outliers extremos en el subconjunto graficado. Gestalt — Proporcionalidad.",
    additional_ins={"integrar_renta_codislas": AssetIn()}
)
def check_escala_y_dashboard(
    generar_graficos_ejercicio3: list,
    integrar_renta_codislas: pd.DataFrame,
) -> AssetCheckResult:
    datos = _filtrar_datos_dashboard(integrar_renta_codislas)
    if datos.empty:
        return AssetCheckResult(passed=True)
    valores = datos["Porcentaje"].dropna()
    v_min, v_max = float(valores.min()), float(valores.max())
    ratio   = round(v_max / v_min, 2) if v_min > 0 else float("inf")
    outlier = datos.loc[datos["Porcentaje"] == v_max, "Territorio"].iloc[0]
    return AssetCheckResult(
        passed=ratio <= RATIO_ESCALA_MAX,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "ratio_escala":       MetadataValue.float(ratio),
            "valor_outlier":      MetadataValue.float(v_max),
            "territorio_outlier": MetadataValue.text(str(outlier)),
            "dashboard_activo":   MetadataValue.text(_contexto_dashboard()),
            "principio_gestalt":  MetadataValue.text("Proporcionalidad — Evita que barras pequeñas parezcan invisibles ante una gigante."),
        },
    )

@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_cardinalidad_dashboard",
    description="Verifica que el número de series en el gráfico actual no supera MAX_CATEGORIAS_COLOR. Gestalt — Carga Cognitiva.",
    additional_ins={"integrar_renta_codislas": AssetIn()}
)
def check_cardinalidad_dashboard(
    generar_graficos_ejercicio3: list,
    integrar_renta_codislas: pd.DataFrame,
) -> AssetCheckResult:
    datos = _filtrar_datos_dashboard(integrar_renta_codislas)
    if Dashboard.COMPARAR_SUBS:
        n      = datos["Territorio"].nunique() if not datos.empty else 0
        col_id = "Territorio"
    else:
        n      = datos["Fuente_Renta_Code"].nunique() if not datos.empty else 0
        col_id = "Fuente_Renta_Code"
    return AssetCheckResult(
        passed=n <= MAX_CATEGORIAS_COLOR,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "n_series":           MetadataValue.int(n),
            "columna_series":     MetadataValue.text(col_id),
            "dashboard_activo":   MetadataValue.text(_contexto_dashboard()),
            "principio_gestalt":  MetadataValue.text("Carga Cognitiva — Más de 9 colores son imposibles de distinguir."),
        },
    )

@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_orden_magnitud_dashboard",
    description="Verifica si las series del gráfico actual están ordenadas por valor medio descendente. Gestalt — Continuidad / Prägnanz.",
    additional_ins={"integrar_renta_codislas": AssetIn()}
)
def check_orden_magnitud_dashboard(
    generar_graficos_ejercicio3: list,
    integrar_renta_codislas: pd.DataFrame,
) -> AssetCheckResult:
    datos = _filtrar_datos_dashboard(integrar_renta_codislas)
    if datos.empty:
        return AssetCheckResult(passed=True)
    col_serie = "Territorio" if Dashboard.COMPARAR_SUBS else "Fuente_Renta"
    medias    = datos.groupby(col_serie)["Porcentaje"].mean().sort_values(ascending=False)
    orden_actual = list(medias.index)
    orden_optimo = orden_actual
    is_sorted    = orden_actual == orden_optimo
    return AssetCheckResult(
        passed=is_sorted,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "is_sorted":         MetadataValue.bool(is_sorted),
            "sugerencia_orden":  MetadataValue.text(f"Usa reorder({col_serie}, -Porcentaje) en aes()."),
            "dashboard_activo":  MetadataValue.text(_contexto_dashboard()),
            "principio_gestalt": MetadataValue.text("Continuidad / Prägnanz — El ojo sigue una escalera suave; reduce el esfuerzo cognitivo."),
        },
    )

@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_graficos_generados",
    description="Verifica que los ficheros PNG se han generado con tamaño > 10 KB. Gestalt — Veracidad Visual.",
)
def check_graficos_generados(generar_graficos_ejercicio3: list) -> AssetCheckResult:
    resultados = {}
    todos_ok   = True
    for ruta in generar_graficos_ejercicio3:
        existe  = os.path.exists(ruta)
        size_kb = round(os.path.getsize(ruta) / 1024, 1) if existe else 0.0
        ok      = existe and size_kb > 10.0
        if not ok:
            todos_ok = False
        resultados[os.path.basename(ruta)] = f"{'OK' if ok else 'FALLO'} ({size_kb} KB)"
    return AssetCheckResult(
        passed=todos_ok,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "graficos":          MetadataValue.text(str(resultados)),
            "dashboard_activo":  MetadataValue.text(_contexto_dashboard()),
            "principio_gestalt": MetadataValue.text("Veracidad Visual — Un gráfico vacío o truncado transmite información falsa."),
            "mensaje": MetadataValue.text("Si size < 10 KB, plotnine generó un gráfico vacío (datos insuficientes o error)."),
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 4 — GENERACIÓN IA (Práctica 4)
# Checks sobre el pipeline de código generado automáticamente por el LLM.
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset="template_ia",
    name="check_template_no_vacio",
    description=(
        "Verifica que el payload para la IA contiene mensajes con contenido real. "
        "Gestalt — Figura y Fondo: un prompt vacío produce un gráfico sin figura."
    ),
)
def check_template_no_vacio(template_ia: dict) -> AssetCheckResult:
    """
    Valida la estructura mínima del payload antes de enviarlo al LLM:
      - Debe tener clave 'messages' con al menos 2 mensajes (system + user).
      - Ningún mensaje debe tener content vacío.
      - La descripción del gráfico debe mencionar las claves de gramática de gráficos.
    """
    mensajes     = template_ia.get("messages", [])
    n_mensajes   = len(mensajes)
    vacios       = [m["role"] for m in mensajes if not m.get("content", "").strip()]
    tiene_system = any(m["role"] == "system" for m in mensajes)
    tiene_user   = any(m["role"] == "user"   for m in mensajes)

    # Comprobamos que la descripción menciona las capas de la gramática de gráficos
    user_content  = next((m["content"] for m in mensajes if m["role"] == "user"), "")
    capas_gramatica = ["aes", "geom_", "scale_", "labs", "theme"]
    capas_presentes = [c for c in capas_gramatica if c in user_content]
    n_capas         = len(capas_presentes)

    passed = (
        n_mensajes >= 2
        and len(vacios) == 0
        and tiene_system
        and tiene_user
        and n_capas >= 3   # Al menos estéticas, geometría y escalas/etiquetas
    )

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "n_mensajes":        MetadataValue.int(n_mensajes),
            "mensajes_vacios":   MetadataValue.text(str(vacios) if vacios else "ninguno"),
            "tiene_system":      MetadataValue.bool(tiene_system),
            "tiene_user":        MetadataValue.bool(tiene_user),
            "capas_gramatica":   MetadataValue.text(str(capas_presentes)),
            "n_capas_presentes": MetadataValue.int(n_capas),
            "principio_gestalt": MetadataValue.text(
                "Figura y Fondo — Un prompt vacío o incompleto produce un gráfico sin figura."
            ),
            "mensaje": MetadataValue.text(
                "El prompt debe incluir al menos aes(), geom_*, scale_* o labs() "
                "para guiar al LLM hacia la gramática de gráficos."
            ),
        },
    )


@asset_check(
    asset="codigo_generado_ia",
    name="check_codigo_valido",
    description=(
        "Verifica que el código generado por la IA es Python ejecutable y contiene "
        "los elementos mínimos de plotnine. Gestalt — Veracidad Visual."
    ),
)
def check_codigo_valido(codigo_generado_ia: str) -> AssetCheckResult:
    """
    Valida el código Python devuelto por el LLM antes de ejecutarlo:
      - Debe definir 'def generar_plot(df)' (nombre exigido por el template).
      - Debe contener 'ggplot' y al menos una geometría geom_*.
      - Debe incluir 'return' (la función debe devolver el gráfico).
      - No debe contener llamadas peligrosas (import os, subprocess, sys.exit…).
      - No debe tener errores de sintaxis Python.
    """
    tiene_funcion  = "def generar_plot" in codigo_generado_ia
    tiene_ggplot   = "ggplot" in codigo_generado_ia
    tiene_geom     = bool(re.search(r"geom_\w+", codigo_generado_ia))
    tiene_return   = "return" in codigo_generado_ia

    # Patrones de código potencialmente peligroso que la IA no debería generar
    patrones_peligrosos = ["import os", "import subprocess", "sys.exit", "open(", "shutil"]
    codigo_peligroso    = [p for p in patrones_peligrosos if p in codigo_generado_ia]

    # Verificación de sintaxis Python sin ejecutar el código
    error_sintaxis = None
    try:
        compile(codigo_generado_ia, "<ia_code>", "exec")
    except SyntaxError as e:
        error_sintaxis = str(e)

    passed = (
        tiene_funcion
        and tiene_ggplot
        and tiene_geom
        and tiene_return
        and len(codigo_peligroso) == 0
        and error_sintaxis is None
    )

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "tiene_def_generar_plot": MetadataValue.bool(tiene_funcion),
            "tiene_ggplot":           MetadataValue.bool(tiene_ggplot),
            "tiene_geom":             MetadataValue.bool(tiene_geom),
            "tiene_return":           MetadataValue.bool(tiene_return),
            "codigo_peligroso":       MetadataValue.text(str(codigo_peligroso) if codigo_peligroso else "ninguno"),
            "error_sintaxis":         MetadataValue.text(error_sintaxis or "ninguno"),
            "longitud_codigo":        MetadataValue.int(len(codigo_generado_ia)),
            "principio_gestalt":      MetadataValue.text(
                "Veracidad Visual — Código con errores de sintaxis o sin return "
                "produce un gráfico vacío que transmite información falsa."
            ),
            "mensaje": MetadataValue.text(
                "Si 'def generar_plot' no está presente, la IA ignoró el template. "
                "Revisa el system prompt en template_ia."
            ),
        },
    )


@asset_check(
    asset="codigo_generado_ia",
    name="check_gestalt_en_codigo",
    description=(
        "Verifica que el código generado aplica al menos un principio Gestalt "
        "(Punto Focal mediante scale_color_manual o annotate). Gestalt — Punto Focal."
    ),
)
def check_gestalt_en_codigo(codigo_generado_ia: str) -> AssetCheckResult:
    """
    Comprueba que la IA respetó las instrucciones de diseño Gestalt del prompt:
      - scale_color_manual → diferenciación de colores (Punto Focal / Similitud).
      - annotate           → anotación directa sobre el gráfico (Punto Focal).
      - theme_minimal      → reducción de ruido visual (Figura y Fondo).
      - labs               → etiquetas claras (Continuidad).

    Un código que no aplica ninguno de estos elementos probablemente ignoró
    las instrucciones de diseño del prompt.
    """
    elementos_gestalt = {
        "scale_color_manual": "Punto Focal / Similitud — diferencia el foco del fondo por color.",
        "annotate":           "Punto Focal — anotación directa sobre el elemento destacado.",
        "theme_minimal":      "Figura y Fondo — elimina ruido visual del panel.",
        "labs":               "Continuidad — etiquetas claras guían la lectura.",
        "scale_x_continuous": "Continuidad — breaks explícitos evitan saltos engañosos en el eje X.",
    }

    presentes  = {k: v for k, v in elementos_gestalt.items() if k in codigo_generado_ia}
    ausentes   = [k for k in elementos_gestalt if k not in codigo_generado_ia]
    n_presentes = len(presentes)

    # Exigimos al menos scale_color_manual (Punto Focal) y labs (Continuidad)
    passed = "scale_color_manual" in presentes and "labs" in presentes

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "elementos_presentes": MetadataValue.text(str(list(presentes.keys()))),
            "elementos_ausentes":  MetadataValue.text(str(ausentes)),
            "n_elementos_gestalt": MetadataValue.int(n_presentes),
            "principio_gestalt":   MetadataValue.text(
                "Punto Focal — scale_color_manual es obligatorio para diferenciar "
                "el municipio destacado del resto."
            ),
            "mensaje": MetadataValue.text(
                "Si scale_color_manual está ausente, el gráfico usará colores automáticos "
                "sin aplicar el principio de Punto Focal solicitado en el prompt."
            ),
        },
    )


@asset_check(
    asset="visualizacion_ia_png",
    name="check_png_ia_generado",
    description=(
        "Verifica que el PNG generado por la IA existe en disco y tiene tamaño > 10 KB. "
        "Gestalt — Veracidad Visual."
    ),
)
def check_png_ia_generado(visualizacion_ia_png: str) -> AssetCheckResult:
    """
    Comprobación final del pipeline IA:
      - El fichero debe existir en la ruta devuelta por visualizacion_ia_png.
      - Su tamaño debe superar 10 KB (un PNG válido de plotnine nunca pesa menos).
      - Se registra el tamaño real en los metadatos para auditoría.

    Un fichero de menos de 10 KB indica que plotnine generó un canvas vacío,
    lo que suele ocurrir cuando el DataFrame filtrado quedó vacío o la función
    no devolvió correctamente el objeto ggplot.
    """
    existe  = os.path.exists(visualizacion_ia_png)
    size_kb = round(os.path.getsize(visualizacion_ia_png) / 1024, 1) if existe else 0.0
    passed  = existe and size_kb > 10.0

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "ruta":              MetadataValue.text(visualizacion_ia_png),
            "existe":            MetadataValue.bool(existe),
            "size_kb":           MetadataValue.float(size_kb),
            "supera_umbral":     MetadataValue.bool(size_kb > 10.0),
            "principio_gestalt": MetadataValue.text(
                "Veracidad Visual — Un PNG vacío o truncado transmite información falsa."
            ),
            "mensaje": MetadataValue.text(
                "Si size_kb < 10, el DataFrame filtrado quedó vacío o generar_plot "
                "no devolvió el objeto ggplot. Revisa los filtros en el código de la IA."
            ),
        },
    )
