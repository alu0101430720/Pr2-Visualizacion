"""
plots_assets_contratos_canarias.py

Nuevos assets que extienden plot_actividad_barras y plot_ocupacion_divergente
a toda Canarias usando los ficheros de contratos históricos (2019-2025 + mar 2026).

Assets:
  - plot_actividad_barras_canarias   — barras apiladas H/M por sector e isla
  - plot_ocupacion_divergente_canarias — divergente H-M por grupo CNO e isla

Añadir a definitions.py:
    import plots_assets_contratos_canarias
    all_assets = load_assets_from_modules([
        assets, plots_assets, plots_assets_contratos_canarias
    ])

Añadir a commitear_plots_a_github deps:
    "plot_actividad_barras_canarias",
    "plot_ocupacion_divergente_canarias",
"""

import os
import re
import glob
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from plotnine import *
from dagster import asset, AssetExecutionContext, MetadataValue
from assets import preprocesar_datos_p5
from plots_assets import get_processed_path, get_plot_dir, fmt_k
import config

warnings.filterwarnings("ignore")

# ── Helpers ───────────────────────────────────────────────────────────────────

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
    Extrae el grupo CNO (primer dígito) independientemente del formato:
      - 2019-2022: '5.5', '9.432'  → primer carácter antes del punto
      - 2023+:     '5120', '9310'  → primer carácter del código
    """
    s = str(val).strip().replace(".", "")
    return int(s[0]) if s and s[0].isdigit() else None


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

ISLAS_ORDEN  = ["El Hierro","La Gomera","La Palma","Tenerife",
                "Gran Canaria","Lanzarote","Fuerteventura"]
COLORS_SEX   = {"Hombres": "#4A90D9", "Mujeres": "#D94A8C"}


def _cargar_contratos_canarias(data_dir: str, año: int) -> pd.DataFrame | None:
    """
    Carga los ficheros de contratos de un año dado, normaliza columnas,
    infiere isla para años sin esa columna y aplica corrección de artículos.
    Devuelve None si no hay ficheros disponibles.
    """
    from checks_p5 import inferir_isla

    if año in (2019, 2020, 2021, 2022):
        paths   = [os.path.join(data_dir, f"contratos{año}.csv")]
        col_c   = "contratos"
    elif año == 2026:
        paths   = [get_processed_path("contratos_202603.csv")]
        col_c   = "Contratos"
    elif año == 2023:
        paths   = sorted(glob.glob(
            os.path.join(data_dir, "2023", "contratos_registrados_*.csv")))
        col_c   = "Contratos"
    elif año == 2024:
        paths   = sorted(glob.glob(
            os.path.join(data_dir, "2024", "contratos_registrados_*.csv")))
        col_c   = "Contratos"
    elif año == 2025:
        paths   = sorted(glob.glob(
            os.path.join(data_dir, "2025", "contratos_202*.csv")))
        col_c   = "Contratos"
    else:
        return None

    paths = [p for p in paths if os.path.exists(p)]
    if not paths:
        return None

    dfs = []
    for p in paths:
        df = pd.read_csv(p, sep=_detect_sep(p), dtype={col_c: float})
        df.columns = df.columns.str.strip()
        df = df.rename(columns={col_c: "c"})
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].str.strip()
        # Corregir artículos en Municipio antes de inferir isla
        if "Municipio" in df.columns:
            df["Municipio"] = df["Municipio"].apply(_fix_articulo)
        dfs.append(df[df["sexo"].isin(["Hombres", "Mujeres"])])

    df_año = pd.concat(dfs, ignore_index=True)
    df_año["año"] = año

    # Inferir isla si no existe la columna
    if "isla" not in df_año.columns:
        df_año["isla"] = df_año["Municipio"].apply(inferir_isla)
    else:
        df_año["isla"] = df_año["isla"].str.strip().str.title()

    # Grupo CNO
    if "CNO11" in df_año.columns:
        df_año["grupo_cno"] = df_año["CNO11"].apply(_cno_grupo).map(CNO_GRUPOS)

    return df_año


# ══════════════════════════════════════════════════════════════════════════════
# ASSET 1 — Barras apiladas: actividad × sexo × isla — Canarias, año configurable
# ══════════════════════════════════════════════════════════════════════════════

@asset(deps=[preprocesar_datos_p5], group_name="viz_estructura_laboral")
def plot_actividad_barras_canarias(context: AssetExecutionContext) -> None:
    """
    Extiende plot_actividad_barras a toda Canarias usando los ficheros de
    contratos históricos. Usa el último año anual completo disponible (2025).

    IDONEIDAD: variable nominal (sector) × nominal (sexo) × temporal (año).
    Barras apiladas H/M por sector, facet por isla. Permite comparar la
    composición de género del mercado laboral en cada isla simultáneamente.

    GESTALT:
      Similitud   — azul = hombres, rosa = mujeres, coherente con el proyecto.
      Proximidad  — barras del mismo sector agrupadas por año dentro de cada isla.
      Cierre      — facet por isla como unidad perceptiva completa.

    DISEÑO:
      Top 6 sectores por volumen (evita overplotting).
      Escala Y libre por isla (tamaños de mercado muy distintos entre islas).
      Sin panel "No consta" — categoría técnica sin valor narrativo.
    """
    data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
    AÑO      = 2025

    df = _cargar_contratos_canarias(data_dir, AÑO)
    if df is None:
        context.log.warning(f"No hay datos de contratos para {AÑO}. Asset omitido.")
        return

    df = df[df["isla"] != "Desconocida"]

    # Top 6 actividades por volumen total (excluir "No consta")
    df_act = df[~df["Actividad económica"].str.lower().str.contains(
        "no consta|sin clasificar", na=False)]
    top_act = (df_act.groupby("Actividad económica")["c"]
               .sum().nlargest(6).index.tolist())
    df_act  = df_act[df_act["Actividad económica"].isin(top_act)]

    # Abreviar nombres
    ABREV = {
        "Servicios de alojamiento":          "Alojamiento",
        "Servicios de comidas y bebidas":    "Hostelería",
        "Comercio al por menor":             "Comercio minorista",
        "Actividades sanitarias":            "Sanidad",
        "Construcción de edificios":         "Construcción",
        "Actividades de construcción especializada": "Construcción esp.",
        "Educación":                         "Educación",
        "Administración pública y defensa; seguridad social obligatoria": "Adm. pública",
    }
    df_act["actividad"] = df_act["Actividad económica"].map(
        lambda x: ABREV.get(x, x[:25]))

    agg = (df_act.groupby(["isla", "actividad", "sexo"], as_index=False)["c"]
           .sum())
    agg["isla"] = pd.Categorical(agg["isla"], categories=ISLAS_ORDEN, ordered=True)

    p = (
        ggplot(agg, aes(x="actividad", y="c", fill="sexo"))
        + geom_col(position="stack", width=0.7, alpha=0.9)
        + facet_wrap("~ isla", scales="free_y", ncol=4)
        + scale_fill_manual(
            values={"Hombres": "#4A90D9", "Mujeres": "#D94A8C"}, name="Sexo")
        + scale_y_continuous(labels=fmt_k)
        + coord_flip()
        + labs(
            title=f"Actividad económica por sector y género — Canarias {AÑO}",
            subtitle="Top 6 sectores por volumen de contratos · escala Y libre por isla",
            x=None, y="Nº contratos",
            caption="Fuente: OBECAN / SEPE",
        )
        + theme_minimal()
        + theme(
            figure_size=(16, 10),
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            strip_text=element_text(size=9, face="bold"),
            axis_text_y=element_text(size=8),
            panel_grid_major_x=element_line(color="#dddddd", size=0.4),
            panel_grid_major_y=element_blank(),
            legend_position="bottom",
        )
    )
    out = os.path.join(get_plot_dir(), "actividad_barras_canarias.png")
    p.save(out, width=16, height=10, dpi=150, verbose=False)
    context.add_output_metadata(
        {"plot": MetadataValue.md(f"![Actividad Canarias]({out})")})


# ══════════════════════════════════════════════════════════════════════════════
# ASSET 2 — Divergente: brecha H-M por grupo CNO e isla — Canarias
# ══════════════════════════════════════════════════════════════════════════════

@asset(deps=[preprocesar_datos_p5], group_name="viz_estructura_laboral")
def plot_ocupacion_divergente_canarias(context: AssetExecutionContext) -> None:
    """
    Extiende plot_ocupacion_divergente a toda Canarias usando grupos CNO-1
    (primer dígito del código de ocupación) para agrupar las 481 ocupaciones
    detalladas en 9 grandes grupos interpretables.

    IDONEIDAD: la diferencia H-M por grupo de ocupación es una magnitud real
    con unidad (número de contratos). El gráfico divergente con eje en 0 es
    la geometría canónica para mostrar qué grupos tienen mayoría masculina
    o femenina.

    GESTALT:
      Simetría    — eje en 0 como punto de paridad visual.
      Similitud   — azul = mayoría hombres, rosa = mayoría mujeres.
      Proximidad  — facet por isla para comparación interinsular.
      Continuidad — barras ordenadas por brecha dentro de cada facet.

    DISEÑO:
      Facet por isla (7 paneles), escala X libre (tamaños distintos).
      Grupos CNO ordenados por brecha global para coherencia visual entre islas.
      Sin "No consta" ni grupos con n < 100 contratos.
    """
    data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
    AÑO      = 2025

    df = _cargar_contratos_canarias(data_dir, AÑO)
    if df is None or "grupo_cno" not in df.columns:
        context.log.warning("No hay datos CNO disponibles. Asset omitido.")
        return

    df = df[df["isla"] != "Desconocida"]
    df = df.dropna(subset=["grupo_cno"])

    # Pivot H/M por isla × grupo
    pivot = (
        df.groupby(["isla", "grupo_cno", "sexo"])["c"]
        .sum().unstack("sexo").fillna(0).reset_index()
    )
    pivot.columns.name = None
    pivot["brecha"]    = pivot.get("Hombres", 0) - pivot.get("Mujeres", 0)
    pivot["total"]     = pivot.get("Hombres", 0) + pivot.get("Mujeres", 0)
    pivot["direccion"] = pivot["brecha"].apply(
        lambda x: "Mayoría Hombres" if x > 0 else "Mayoría Mujeres")

    # Filtrar grupos con masa insuficiente
    pivot = pivot[pivot["total"] >= 100]

    # Orden global de grupos (por brecha agregada de toda Canarias)
    orden_global = (
        pivot.groupby("grupo_cno")["brecha"].sum()
        .sort_values().index.tolist()
    )
    pivot["grupo_cno"] = pd.Categorical(
        pivot["grupo_cno"], categories=orden_global, ordered=True)
    pivot["isla"] = pd.Categorical(
        pivot["isla"], categories=ISLAS_ORDEN, ordered=True)

    p = (
        ggplot(pivot, aes(x="grupo_cno", y="brecha", fill="direccion"))
        + geom_col(width=0.65, alpha=0.9)
        + geom_hline(yintercept=0, linetype="dashed",
                     color="#333333", size=0.5)
        + facet_wrap("~ isla", scales="free_x", ncol=4)
        + scale_fill_manual(
            values={"Mayoría Hombres": "#4A90D9",
                    "Mayoría Mujeres": "#D94A8C"},
            name=None)
        + scale_y_continuous(labels=fmt_k)
        + coord_flip()
        + labs(
            title=f"Brecha de género por grupo ocupacional e isla — Canarias {AÑO}",
            subtitle="Diferencia contratos (Hombres − Mujeres) · grupos CNO-1 · escala X libre por isla",
            x=None, y="Diferencia (Hombres − Mujeres)",
            caption="Fuente: OBECAN / SEPE",
        )
        + theme_minimal()
        + theme(
            figure_size=(16, 10),
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            strip_text=element_text(size=9, face="bold"),
            axis_text_y=element_text(size=8),
            panel_grid_major_x=element_line(color="#dddddd", size=0.4),
            panel_grid_major_y=element_blank(),
            legend_position="bottom",
        )
    )
    out = os.path.join(get_plot_dir(), "ocupacion_divergente_canarias.png")
    p.save(out, width=16, height=10, dpi=150, verbose=False)
    context.add_output_metadata(
        {"plot": MetadataValue.md(f"![Ocupación Canarias]({out})")})


# ══════════════════════════════════════════════════════════════════════════════
# CHECKS
# ══════════════════════════════════════════════════════════════════════════════

from dagster import asset_check, AssetCheckResult, AssetCheckSeverity


@asset_check(
    asset=plot_actividad_barras_canarias,
    description=(
        "Verifica que los ficheros de contratos 2025 existen, tienen las columnas "
        "necesarias, cobertura de las 7 islas y actividades suficientes."
    ),
)
def check_actividad_barras_canarias(context):
    """
    Gestalt — Similitud: sin las 7 islas el facet presenta paneles vacíos que
    el lector interpreta como islas sin actividad económica, no como datos faltantes.
    Cierre: sin al menos 5 actividades no se puede construir un top-6 coherente.
    """
    data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
    passed   = True
    md       = "### Check: actividad_barras_canarias\n\n"

    df = _cargar_contratos_canarias(data_dir, 2025)

    if df is None:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            metadata={"check": MetadataValue.md("🔴 No hay ficheros de contratos 2025.")},
        )

    # Columnas requeridas
    for col in ["Actividad económica", "sexo", "c", "isla"]:
        ok = col in df.columns
        passed = passed and ok
        md += f"- Columna `{col}`: {'🟢' if ok else '🔴'}\n"

    # Cobertura de islas
    islas_ok  = set(df[df["isla"] != "Desconocida"]["isla"].unique())
    faltantes = set(ISLAS_ORDEN) - islas_ok
    ok = len(faltantes) == 0
    passed = passed and ok
    md += f"- 7 islas cubiertas: {'🟢' if ok else '🔴'} (faltan: {faltantes or '–'})\n"

    # Municipios sin isla
    sin_isla = df[df["isla"] == "Desconocida"]["c"].sum()
    pct_sin  = sin_isla / df["c"].sum() * 100
    ok = pct_sin < 5.0
    passed = passed and ok
    md += f"- Municipios sin isla < 5%: {'🟢' if ok else '🔴'} ({pct_sin:.1f}%)\n"

    # Actividades suficientes
    n_act = df["Actividad económica"].nunique()
    ok = n_act >= 6
    passed = passed and ok
    md += f"- Actividades únicas ≥ 6: {'🟢' if ok else '🔴'} ({n_act})\n"

    # Ambos sexos presentes
    sexos = set(df["sexo"].unique())
    ok = {"Hombres", "Mujeres"}.issubset(sexos)
    passed = passed and ok
    md += f"- Ambos sexos presentes: {'🟢' if ok else '🔴'}\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"check": MetadataValue.md(md)},
    )


@asset_check(
    asset=plot_ocupacion_divergente_canarias,
    description=(
        "Verifica que los ficheros de contratos 2025 tienen CNO11 válido, "
        "cobertura de islas y grupos CNO con masa suficiente."
    ),
)
def check_ocupacion_divergente_canarias(context):
    """
    Gestalt — Simetría: sin grupos con masa suficiente (n ≥ 100) el gráfico
    divergente muestra barras de un solo píxel que rompen la lectura simétrica.
    Figura/Fondo: un CNO11 malformado produce grupos None que aparecen como
    barra sin etiqueta, contaminando la figura.
    """
    data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
    passed   = True
    md       = "### Check: ocupacion_divergente_canarias\n\n"

    df = _cargar_contratos_canarias(data_dir, 2025)

    if df is None:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            metadata={"check": MetadataValue.md("🔴 No hay ficheros de contratos 2025.")},
        )

    # CNO11 presente
    ok = "CNO11" in df.columns
    passed = passed and ok
    md += f"- Columna `CNO11` presente: {'🟢' if ok else '🔴'}\n"

    if "grupo_cno" in df.columns:
        # Grupos CNO válidos
        n_sin_cno = int(df["grupo_cno"].isna().sum())
        pct_sin   = n_sin_cno / len(df) * 100
        ok = pct_sin < 10.0
        passed = passed and ok
        md += f"- CNO sin grupo < 10%: {'🟢' if ok else '🔴'} ({pct_sin:.1f}%)\n"

        # Grupos únicos (esperamos 9 del CNO-1)
        n_grupos = df["grupo_cno"].nunique()
        ok = n_grupos >= 7
        passed = passed and ok
        md += f"- Grupos CNO únicos ≥ 7: {'🟢' if ok else '🔴'} ({n_grupos})\n"

        # Masa mínima por grupo e isla
        pivot = (
            df[df["isla"] != "Desconocida"]
            .groupby(["isla", "grupo_cno"])["c"]
            .sum().reset_index()
        )
        grupos_escasos = pivot[pivot["c"] < 100]
        ok = len(grupos_escasos) < 5
        passed = passed and ok
        md += (
            f"- Grupos con n < 100: {'🟢' if ok else '⚠️'}"
            f" ({len(grupos_escasos)} combinaciones isla×grupo)\n"
        )

    # Cobertura islas
    islas_ok  = set(df[df["isla"] != "Desconocida"]["isla"].unique())
    faltantes = set(ISLAS_ORDEN) - islas_ok
    ok = len(faltantes) == 0
    passed = passed and ok
    md += f"- 7 islas cubiertas: {'🟢' if ok else '🔴'} (faltan: {faltantes or '–'})\n"

    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN,
        metadata={"check": MetadataValue.md(md)},
    )
