"""
plots_assets_contratos_canarias.py

Assets que extienden plot_actividad_barras y plot_ocupacion_divergente
a toda Canarias (o a la provincia/isla configurada en plot_config.yaml)
usando los ficheros de contratos históricos (2019-2025 + mar 2026).

Assets:
  - plot_actividad_barras_canarias
  - plot_ocupacion_divergente_canarias

Integración en definitions.py:
    import plots_assets_contratos_canarias
    all_assets = load_assets_from_modules([
        assets, plots_assets, plots_assets_contratos_canarias
    ])

Añadir a commitear_plots_a_github deps en assets.py:
    "plot_actividad_barras_canarias",
    "plot_ocupacion_divergente_canarias",
"""

import os, re, glob, warnings
import numpy as np
import pandas as pd
from plotnine import *
from dagster import (
    asset, asset_check, AssetExecutionContext,
    AssetCheckResult, AssetCheckSeverity, MetadataValue,
)
from assets import preprocesar_datos_p5
from plots_assets import get_processed_path, get_plot_dir, get_plot_config, fmt_k
import config

warnings.filterwarnings("ignore")

# ── Constantes ────────────────────────────────────────────────────────────────

ISLAS_ORDEN   = ["El Hierro","La Gomera","La Palma","Tenerife",
                 "Gran Canaria","Lanzarote","Fuerteventura"]
ISLAS_VALIDAS = set(ISLAS_ORDEN)

PROVINCIAS = {
    "SC Tenerife": {"Tenerife","La Palma","La Gomera","El Hierro"},
    "Las Palmas":  {"Gran Canaria","Lanzarote","Fuerteventura"},
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
        return ISLAS_ORDEN
    elif ambito in PROVINCIAS:
        return [i for i in ISLAS_ORDEN if i in PROVINCIAS[ambito]]
    else:
        return [ambito]


def _cargar_contratos(data_dir: str, año: int) -> pd.DataFrame | None:
    """
    Carga contratos de un año, normaliza columnas, infiere isla para
    ficheros 2019-2022 que no la traen, y calcula grupo CNO-1.
    """
    from checks_p5 import inferir_isla

    if año in (2019, 2020, 2021, 2022):
        paths = [os.path.join(data_dir, f"contratos{año}.csv")]
        col_c = "contratos"
    elif año == 2026:
        paths = [get_processed_path("contratos_202603.csv")]
        col_c = "Contratos"
    elif año == 2023:
        paths = sorted(glob.glob(
            os.path.join(data_dir, "2023", "contratos_registrados_*.csv")))
        col_c = "Contratos"
    elif año == 2024:
        paths = sorted(glob.glob(
            os.path.join(data_dir, "2024", "contratos_registrados_*.csv")))
        col_c = "Contratos"
    elif año == 2025:
        paths = sorted(glob.glob(
            os.path.join(data_dir, "2025", "contratos_202*.csv")))
        col_c = "Contratos"
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
        # Corregir artículos antes de inferir isla
        if "Municipio" in df.columns:
            df["Municipio"] = df["Municipio"].apply(_fix_articulo)
        dfs.append(df[df["sexo"].isin(["Hombres", "Mujeres"])])

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


# ══════════════════════════════════════════════════════════════════════════════
# ASSET 1 — Actividad económica × sexo
# ══════════════════════════════════════════════════════════════════════════════

@asset(deps=[preprocesar_datos_p5], group_name="viz_estructura_laboral")
def plot_actividad_barras_canarias(context: AssetExecutionContext) -> None:
    """
    Barras apiladas H/M por sector, filtradas según el ámbito configurado
    en plot_config.yaml (Canarias, provincia o isla individual).

    Cuando ambito = "Canarias" o provincia → facet por isla.
    Cuando ambito = isla individual        → barras directas sin facet.

    GESTALT:
      Similitud   — azul = hombres, rosa = mujeres, coherente con el proyecto.
      Proximidad  — barras del mismo sector agrupadas dentro de cada isla.
      Cierre      — facet por isla como unidad perceptiva completa.
      Continuidad — ordenación por volumen total (mayor arriba con coord_flip).

    DISEÑO:
      Top N sectores configurables desde el YAML (recomendado 5-8).
      Escala X libre por isla (tamaños de mercado distintos entre islas).
      Sin "No consta" ni similares — categorías técnicas sin valor narrativo.
    """
    cfg    = get_plot_config()["actividad_barras_canarias"]
    AÑO    = cfg.get("ano", 2025)
    AMBITO = cfg.get("ambito", "Canarias")
    TOP_N  = cfg.get("top_n_actividades", 6)

    data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
    df = _cargar_contratos(data_dir, AÑO)
    if df is None:
        context.log.warning(f"No hay datos de contratos para {AÑO}.")
        return

    df = _filtrar_ambito(df, AMBITO)
    context.log.info(
        f"Ámbito: {AMBITO} → {df['isla'].nunique()} isla(s), "
        f"{df['c'].sum():,.0f} contratos"
    )

    df = df[~df["Actividad económica"].str.lower().str.contains(
        "no consta|sin clasificar", na=False)]

    top_act = (df.groupby("Actividad económica")["c"]
               .sum().nlargest(TOP_N).index.tolist())
    df = df[df["Actividad económica"].isin(top_act)]

    ABREV = {
        "Servicios de alojamiento":   "Alojamiento",
        "Servicios de comidas y bebidas": "Hostelería",
        "Comercio al por menor, excepto de vehículos de motor y motocicletas":
            "Comercio minorista",
        "Actividades sanitarias":     "Sanidad",
        "Construcción de edificios":  "Construcción",
        "Actividades de construcción especializada": "Construcción esp.",
        "Educación":                  "Educación",
        "Administración pública y defensa; seguridad social obligatoria":
            "Adm. pública",
        "Servicios a edificios y actividades de jardinería":
            "Servicios a edificios",
    }
    df["actividad"] = df["Actividad económica"].map(
        lambda x: ABREV.get(x, x[:28]))

    titulo = (f"Actividad económica por sector y género — "
              f"{_titulo_ambito(AMBITO)}, {AÑO}")

    es_isla_unica = AMBITO in ISLAS_VALIDAS

    if es_isla_unica:
        agg = df.groupby(["actividad","sexo"], as_index=False)["c"].sum()
        p = (
            ggplot(agg, aes(x="reorder(actividad, c)", y="c", fill="sexo"))
            + geom_col(position="stack", width=0.7, alpha=0.9)
            + scale_fill_manual(
                values={"Hombres":"#4A90D9","Mujeres":"#D94A8C"}, name="Sexo")
            + scale_y_continuous(labels=fmt_k)
            + coord_flip()
            + labs(title=titulo,
                   subtitle=f"Top {TOP_N} sectores por volumen de contratos",
                   x=None, y="Nº contratos",
                   caption="Fuente: OBECAN / SEPE")
            + _tema_base((11, 6))
        )
        fig_w, fig_h = 11, 6
    else:
        islas_ambito = _islas_en_ambito(AMBITO)
        agg = df.groupby(["isla","actividad","sexo"], as_index=False)["c"].sum()
        agg["isla"] = pd.Categorical(
            agg["isla"], categories=islas_ambito, ordered=True)
        ncol = min(len(islas_ambito), 4)
        p = (
            ggplot(agg, aes(x="reorder(actividad, c)", y="c", fill="sexo"))
            + geom_col(position="stack", width=0.7, alpha=0.9)
            + facet_wrap("~ isla", scales="free_x", ncol=ncol)
            + scale_fill_manual(
                values={"Hombres":"#4A90D9","Mujeres":"#D94A8C"}, name="Sexo")
            + scale_y_continuous(labels=fmt_k)
            + coord_flip()
            + labs(title=titulo,
                   subtitle=f"Top {TOP_N} sectores · escala X libre por isla",
                   x=None, y="Nº contratos",
                   caption="Fuente: OBECAN / SEPE")
            + _tema_base((16, 10))
            + theme(strip_text=element_text(size=9, face="bold"))
        )
        fig_w, fig_h = 16, 10

    out = os.path.join(get_plot_dir(), "actividad_barras_canarias.png")
    p.save(out, width=fig_w, height=fig_h, dpi=150, verbose=False)
    context.add_output_metadata({
        "ambito": MetadataValue.text(AMBITO),
        "año":    MetadataValue.int(AÑO),
        "plot":   MetadataValue.md(f"![Actividad {AMBITO}]({out})"),
    })


# ══════════════════════════════════════════════════════════════════════════════
# ASSET 2 — Ocupación divergente por grupo CNO
# ══════════════════════════════════════════════════════════════════════════════

@asset(deps=[preprocesar_datos_p5], group_name="viz_estructura_laboral")
def plot_ocupacion_divergente_canarias(context: AssetExecutionContext) -> None:
    """
    Barras divergentes H-M por grupo CNO-1, filtradas según el ámbito
    configurado en plot_config.yaml.

    Cuando ambito = "Canarias" o provincia → facet por isla.
    Cuando ambito = isla individual        → barras directas sin facet.

    GESTALT:
      Simetría    — eje en 0 como punto de paridad visual.
      Similitud   — azul = mayoría hombres, rosa = mayoría mujeres.
      Proximidad  — facet por isla para comparación interinsular directa.
      Continuidad — grupos ordenados por brecha global para coherencia visual
                    entre islas (el lector no tiene que reaprender el orden).

    DISEÑO:
      9 grupos CNO-1 interpretables y coherentes con la clasificación INE/SEPE.
      Brecha expresada en % sobre el total de contratos de cada isla — así
      El Hierro y Tenerife son directamente comparables con escala compartida.
      Filtro min_contratos desde YAML para excluir grupos con n irrelevante.
    """
    cfg    = get_plot_config()["ocupacion_divergente_canarias"]
    AÑO    = cfg.get("ano", 2025)
    AMBITO = cfg.get("ambito", "Canarias")
    MIN_C  = cfg.get("min_contratos", 100)

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

    if es_isla_unica:
        pivot = (
            df.groupby(["grupo_cno","sexo"])["c"]
            .sum().unstack("sexo").fillna(0).reset_index()
        )
        pivot.columns.name = None
        pivot["total_isla"] = df["c"].sum()
        pivot["brecha"]     = (
            (pivot.get("Hombres",0) - pivot.get("Mujeres",0))
            / pivot["total_isla"] * 100
        )
        pivot["total"]      = pivot.get("Hombres",0) + pivot.get("Mujeres",0)
        pivot               = pivot[pivot["total"] >= MIN_C]
        pivot["direccion"]  = pivot["brecha"].apply(
            lambda x: "Mayoría Hombres" if x > 0 else "Mayoría Mujeres")

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
                   subtitle="Diferencia (H − M) como % del total de contratos · grupos CNO-1",
                   x=None, y="% sobre total contratos (Hombres − Mujeres)",
                   caption="Fuente: OBECAN / SEPE")
            + _tema_base((11, 6))
        )
        fig_w, fig_h = 11, 6
    else:
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
                   x=None, y="% sobre total contratos (Hombres − Mujeres)",
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


# ══════════════════════════════════════════════════════════════════════════════
# CHECKS
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset=plot_actividad_barras_canarias,
    description=(
        "Verifica ficheros del año configurado, cobertura del ámbito "
        "seleccionado y actividades suficientes para el top-N."
    ),
)
def check_actividad_barras_canarias(context):
    """
    Gestalt — Cierre [actividad_canarias]: sin las islas del ámbito el facet
    presenta paneles vacíos que el lector interpreta como islas sin actividad.
    Similitud: sin ambos sexos la paleta azul/rosa pierde coherencia.
    """
    cfg    = get_plot_config()["actividad_barras_canarias"]
    AÑO    = cfg.get("ano", 2025)
    AMBITO = cfg.get("ambito", "Canarias")
    TOP_N  = cfg.get("top_n_actividades", 6)

    data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
    passed   = True
    md       = f"### Check: actividad_barras_canarias\n\n"
    md      += (f"Configuración: `ano={AÑO}`, `ambito={AMBITO!r}`, "
                f"`top_n={TOP_N}`\n\n")

    df = _cargar_contratos(data_dir, AÑO)
    if df is None:
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.WARN,
            metadata={"check": MetadataValue.md(
                f"🔴 No hay ficheros de contratos para {AÑO}.")},
        )

    try:
        df_f = _filtrar_ambito(df, AMBITO)
        md += f"- Ámbito `{AMBITO}` reconocido: 🟢\n"
    except ValueError as e:
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.ERROR,
            metadata={"check": MetadataValue.md(f"🔴 {e}")},
        )

    islas_esp  = set(_islas_en_ambito(AMBITO))
    islas_pres = set(df_f[df_f["isla"].isin(ISLAS_VALIDAS)]["isla"].unique())
    faltantes  = islas_esp - islas_pres
    ok = len(faltantes) == 0
    passed = passed and ok
    md += f"- Islas del ámbito: {'🟢' if ok else '🔴'} (faltan: {faltantes or '–'})\n"

    sin_isla = df_f[~df_f["isla"].isin(ISLAS_VALIDAS)]["c"].sum()
    pct_sin  = sin_isla / df_f["c"].sum() * 100 if df_f["c"].sum() > 0 else 0
    ok = pct_sin < 5.0
    passed = passed and ok
    md += f"- Sin isla < 5%: {'🟢' if ok else '🔴'} ({pct_sin:.1f}%)\n"

    df_f2 = df_f[~df_f["Actividad económica"].str.lower().str.contains(
        "no consta|sin clasificar", na=False)]
    n_act = df_f2["Actividad económica"].nunique()
    ok    = n_act >= TOP_N
    passed = passed and ok
    md += f"- Actividades ≥ {TOP_N}: {'🟢' if ok else '🔴'} ({n_act})\n"

    ok = {"Hombres","Mujeres"}.issubset(set(df_f["sexo"].unique()))
    passed = passed and ok
    md += f"- Ambos sexos: {'🟢' if ok else '🔴'}\n"

    return AssetCheckResult(
        passed=bool(passed), severity=AssetCheckSeverity.WARN,
        metadata={"check": MetadataValue.md(md)},
    )


@asset_check(
    asset=plot_ocupacion_divergente_canarias,
    description=(
        "Verifica CNO11 válido, cobertura del ámbito y grupos con masa "
        "suficiente según min_contratos configurado."
    ),
)
def check_ocupacion_divergente_canarias(context):
    """
    Gestalt — Simetría [ocupacion_canarias]: sin grupos con n ≥ min_contratos
    el divergente muestra barras de un pixel que rompen la lectura simétrica.
    Figura/Fondo: CNO11 malformado produce grupos None sin etiqueta.
    """
    cfg    = get_plot_config()["ocupacion_divergente_canarias"]
    AÑO    = cfg.get("ano", 2025)
    AMBITO = cfg.get("ambito", "Canarias")
    MIN_C  = cfg.get("min_contratos", 100)

    data_dir = os.path.join(config.TARGET_DIR, config.DATA_P5_DIR)
    passed   = True
    md       = f"### Check: ocupacion_divergente_canarias\n\n"
    md      += (f"Configuración: `ano={AÑO}`, `ambito={AMBITO!r}`, "
                f"`min_contratos={MIN_C}`\n\n")

    df = _cargar_contratos(data_dir, AÑO)
    if df is None:
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.WARN,
            metadata={"check": MetadataValue.md(
                f"🔴 No hay ficheros de contratos para {AÑO}.")},
        )

    try:
        df_f = _filtrar_ambito(df, AMBITO)
        md += f"- Ámbito `{AMBITO}` reconocido: 🟢\n"
    except ValueError as e:
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.ERROR,
            metadata={"check": MetadataValue.md(f"🔴 {e}")},
        )

    ok = "CNO11" in df_f.columns
    passed = passed and ok
    md += f"- Columna `CNO11`: {'🟢' if ok else '🔴'}\n"

    if "grupo_cno" in df_f.columns:
        pct_sin = df_f["grupo_cno"].isna().mean() * 100
        ok = pct_sin < 10.0
        passed = passed and ok
        md += f"- Sin grupo CNO < 10%: {'🟢' if ok else '🔴'} ({pct_sin:.1f}%)\n"

        n_grupos = df_f["grupo_cno"].nunique()
        ok = n_grupos >= 7
        passed = passed and ok
        md += f"- Grupos CNO ≥ 7: {'🟢' if ok else '🔴'} ({n_grupos})\n"

    pivot = (
        df_f[df_f["isla"].isin(ISLAS_VALIDAS)]
        .groupby(["isla","grupo_cno"])["c"].sum().reset_index()
    )
    escasos = pivot[pivot["c"] < MIN_C]
    ok = len(escasos) < 5
    passed = passed and ok
    md += (f"- Grupos con n < {MIN_C}: "
           f"{'🟢' if ok else '⚠️'} ({len(escasos)} combis isla×grupo)\n")

    islas_esp  = set(_islas_en_ambito(AMBITO))
    islas_pres = set(df_f[df_f["isla"].isin(ISLAS_VALIDAS)]["isla"].unique())
    faltantes  = islas_esp - islas_pres
    ok = len(faltantes) == 0
    passed = passed and ok
    md += f"- Islas del ámbito: {'🟢' if ok else '🔴'} (faltan: {faltantes or '–'})\n"

    return AssetCheckResult(
        passed=bool(passed), severity=AssetCheckSeverity.WARN,
        metadata={"check": MetadataValue.md(md)},
    )