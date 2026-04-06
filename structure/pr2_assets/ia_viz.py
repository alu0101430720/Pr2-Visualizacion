"""
assets/ia_viz.py — Pipeline de generación de gráficos mediante IA (Práctica 4).

Cambios respecto a la versión anterior:
  - El LLM solo genera el bloque ggplot(...), no la función completa.
    La función se construye en Python; el LLM no puede romper la preparación
    de datos ni el nombre de la función.
  - _corregir_codigo se reduce a 3 parches mínimos (comillas + scale_manual).
  - Las dos llamadas al LLM se lanzan en paralelo con ThreadPoolExecutor.
  - _llamar_ia acepta un retry=1 para reintentar una vez ante fallo de red.
  - Se añade _validar_bloque() más ligero que _validar_codigo().
"""

import os
import re
import warnings
import concurrent.futures

import requests
import pandas as pd
from dagster import asset, Output, OpExecutionContext, MetadataValue

from config import DIR_GRAFICOS, GIT_BRANCH, REPO_DIR, Dashboard, MAPA_EDUCACION, repo_url
from utils import commit_and_push
from pr2_assets.git_ops import get_github_token


# ── Constantes ─────────────────────────────────────────────────────────────────

IA_URL   = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
IA_MODEL = "ollama/deepseek-coder:6.7b-instruct-q4_K_M"
IA_TOKEN = "sk-1234"

COLOR_FOCAL  = "#D95F02"
COLOR_NEUTRO = "#CCCCCC"

SYSTEM_PROMPT = (
    "Eres un experto en Plotnine (gramática de gráficos para Python). "
    "Devuelve EXCLUSIVAMENTE el bloque Python que empieza con 'ggplot(' y termina con ')'. "
    "Sin imports, sin funciones, sin markdown, sin explicaciones. "
    "Usa solo las variables que ya existen en el entorno: df, y las indicadas."
)


def _paleta_focal(categorias: list, focal: str) -> dict:
    return {c: (COLOR_FOCAL if c == focal else COLOR_NEUTRO) for c in categorias}


# ── Limpieza mínima del bloque devuelto por el LLM ────────────────────────────

def _limpiar_bloque(texto: str) -> str:
    """
    Extrae solo el bloque ggplot(...) de la respuesta.
    1. Elimina markdown ```python ... ```.
    2. Se queda desde la primera 'ggplot(' hasta el final del bloque
       balanceando paréntesis — así funciona aunque el LLM añada prosa después.
    """
    # Quitar markdown
    texto = re.sub(r"```(?:python)?", "", texto).replace("```", "").strip()

    start = texto.find("ggplot(")
    if start == -1:
        raise ValueError(f"El LLM no devolvió un bloque ggplot. Respuesta:\n{texto}")

    # Balancear paréntesis para extraer el bloque completo
    depth, i = 0, start
    for i, ch in enumerate(texto[start:], start):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                break

    return texto[start : i + 1].strip()


def _corregir_bloque(bloque: str) -> str:
    """
    Parches mínimos sobre el bloque ggplot ya aislado.
    Al no generar la función completa, la mayoría de errores anteriores
    desaparecen. Solo mantenemos los 3 más frecuentes.
    """
    # 1. Comillas tipográficas → ASCII
    bloque = (bloque
              .replace("\u2018", "'").replace("\u2019", "'")
              .replace("\u201c", '"').replace("\u201d", '"'))

    # 2. scale_*_manual({...}) sin keyword 'values'
    bloque = re.sub(r"(scale_\w+_manual)\(\{", r"\1(values={", bloque)

    # 3. scale_y_continuous(labels=[lista]) → scale_y_continuous()
    bloque = re.sub(r"scale_y_continuous\(labels=\[[^\]]*\]\)", "scale_y_continuous()", bloque)

    return bloque


def _llamar_ia(payload: dict, context: OpExecutionContext, retries: int = 1) -> str:
    """POST al LLM con reintentos. Devuelve el bloque ggplot extraído."""
    ultimo_error = None
    for intento in range(1 + retries):
        if intento:
            context.log.warning(f"Reintentando llamada IA (intento {intento + 1})...")
        try:
            resp = requests.post(
                IA_URL,
                headers={"Content-Type": "application/json",
                         "Authorization": f"Bearer {IA_TOKEN}"},
                json=payload,
                timeout=120,
            )
            resp.raise_for_status()
            raw = resp.json()["choices"][0]["message"]["content"]
            context.log.info(f"Respuesta IA: {len(raw)} caracteres.")
            bloque = _limpiar_bloque(raw)
            return _corregir_bloque(bloque)
        except (requests.exceptions.RequestException, ValueError) as e:
            ultimo_error = e
            context.log.warning(f"Error en llamada IA: {e}")

    raise RuntimeError(f"Fallo definitivo llamando al LLM: {ultimo_error}") from ultimo_error


# ── Ejecución del bloque en entorno controlado ────────────────────────────────

def _ejecutar_bloque(
    bloque: str,
    df: pd.DataFrame,
    extra_vars: dict,
    context: OpExecutionContext = None,
):
    """
    Envuelve el bloque ggplot en una función mínima y la ejecuta.
    'extra_vars' inyecta variables que el bloque puede necesitar
    (p.ej. colores, orden de categorías) sin que el LLM las tenga que generar.
    """
    import plotnine

    entorno = {k: v for k, v in plotnine.__dict__.items() if not k.startswith("_")}
    entorno["plotnine"] = plotnine
    entorno["pd"] = pd
    entorno["df"] = df
    entorno.update(extra_vars)

    # Envolver en función para capturar el resultado
    fn_code = "def _plot_fn():\n" + "\n".join(f"    {l}" for l in bloque.splitlines()) + "\n    return plot\n"

    # El bloque debe terminar asignando a 'plot'; si el LLM devolvió
    # la expresión directamente (sin asignación), la envolvemos nosotros.
    if not re.search(r"^\s*plot\s*=", bloque, re.MULTILINE):
        fn_code = "def _plot_fn():\n    plot = (\n" + "\n".join(f"        {l}" for l in bloque.splitlines()) + "\n    )\n    return plot\n"

    try:
        exec(fn_code, entorno)  # noqa: S102
    except SyntaxError as e:
        numerado = "\n".join(f"{i+1:>3}: {l}" for i, l in enumerate(fn_code.splitlines()))
        raise RuntimeError(f"SyntaxError en bloque LLM: {e}\n{numerado}") from e

    return entorno["_plot_fn"]()


# ══════════════════════════════════════════════════════════════════════════════
# Assets
# ══════════════════════════════════════════════════════════════════════════════

@asset
def template_ia_renta(
    context: OpExecutionContext,
    integrar_renta_codislas: pd.DataFrame,
) -> dict:
    territorio = Dashboard.TERRITORIO

    df_filt = integrar_renta_codislas[
        (integrar_renta_codislas["ISLA_clean"] == territorio) &
        (integrar_renta_codislas["Fuente_Renta_Code"] == "SUELDOS_SALARIOS") &
        (integrar_renta_codislas["Territorio"] != territorio)
    ]
    municipio_top = (
        df_filt.groupby("Territorio")["Porcentaje"].mean().idxmax()
        if not df_filt.empty else "Municipio Destacado"
    )

    label_resto   = "Otros municipios"
    colores_focal = {municipio_top: COLOR_FOCAL, label_resto: COLOR_NEUTRO}
    # Los colores se resuelven aquí en Python — el LLM solo copia el dict literal.

    descripcion = f"""El dataframe 'df' ya tiene estas columnas relevantes:
  Año (int), Porcentaje (float), Territorio (str), es_focal (str).

La columna 'es_focal' contiene '{municipio_top}' para el municipio destacado
y '{label_resto}' para el resto. El df ya está ordenado para pintar el foco encima.

Genera el bloque ggplot que produzca:
  - geom_line(size=1.2) + geom_point(size=3, fill='white', stroke=1)
  - aes: x='Año', y='Porcentaje', color='es_focal', group='Territorio'
  - scale_color_manual(values={colores_focal})
  - scale_x_continuous(breaks=list(range(2015, 2025, 2)))
  - expand_limits(y=0)
  - labs(title='Evolución de Salarios — {territorio}',
         subtitle='Fuente: ISTAC · Distribución de Renta en Canarias',
         x='Año', y='Porcentaje (%)', color='Municipio')
  - theme_minimal() + theme(figure_size=(12, 8),
                            legend_position='bottom',
                            legend_title=element_text(fontweight='bold'))

Asigna el resultado a la variable 'plot'.
"""
    context.log.info(f"Template renta · focal='{municipio_top}'")
    return {
        "municipio_top": municipio_top,
        "label_resto":   label_resto,
        "payload": {
            "model": IA_MODEL, "temperature": 0.1, "stream": False,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": descripcion},
            ],
        },
    }


@asset
def template_ia_social(
    context: OpExecutionContext,
    enriquecer_nivelestudios: pd.DataFrame,
) -> dict:
    territorio   = Dashboard.TERRITORIO
    col_estudios = next(
        (c for c in enriquecer_nivelestudios.columns
         if "estudio" in c.lower() or "nivel" in c.lower()),
        "Nivel de estudios en curso",
    )
    orden = ['Sin Estudios/Otros', 'Básicos', 'Medios', 'Superiores']

    descripcion = f"""El dataframe 'df' ya tiene estas columnas relevantes:
  Periodo (int), n (float), Categoria (Categorical ordenada: {orden}).

Genera el bloque ggplot que produzca:
  - geom_area(position='fill', alpha=0.85, color='white')
  - aes: x='Periodo', y='n', fill='Categoria'
  - scale_fill_brewer(type='qual', palette='Set2')
  - scale_x_continuous(breaks=list(range(2019, 2026, 2)))
  - labs(title='Distribución del Nivel de Estudios — {territorio}',
         subtitle='Fuente: ISTAC · Encuesta de Nivel y Condiciones de Vida',
         x='Año', y='Proporción', fill='Nivel educativo')
  - theme_minimal() + theme(figure_size=(12, 5), legend_position='right')

Asigna el resultado a la variable 'plot'.
"""
    context.log.info(f"Template social · col='{col_estudios}'")
    return {
        "col_estudios": col_estudios,
        "payload": {
            "model": IA_MODEL, "temperature": 0.1, "stream": False,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": descripcion},
            ],
        },
    }


@asset
def visualizacion_ia_png(
    context: OpExecutionContext,
    template_ia_renta:  dict,
    template_ia_social: dict,
    integrar_renta_codislas:  pd.DataFrame,
    enriquecer_nivelestudios: pd.DataFrame,
) -> Output:
    """
    Llama al LLM en paralelo para ambos gráficos, prepara los datos
    aquí en Python (no el LLM) y ejecuta los bloques generados.
    """
    warnings.filterwarnings("ignore")
    os.makedirs(DIR_GRAFICOS, exist_ok=True)
    territorio     = Dashboard.TERRITORIO
    territorio_key = territorio.lower().replace(" ", "_")

    # ── Preparación de datos (Python, no el LLM) ──────────────────────────────

    municipio_top = template_ia_renta["municipio_top"]
    label_resto   = template_ia_renta["label_resto"]

    df_renta = (
        integrar_renta_codislas
        .loc[
            (integrar_renta_codislas["ISLA_clean"] == territorio) &
            (integrar_renta_codislas["Fuente_Renta_Code"] == "SUELDOS_SALARIOS") &
            (integrar_renta_codislas["Territorio"] != territorio)
        ]
        .copy()
    )
    df_renta["es_focal"] = df_renta["Territorio"].apply(
        lambda x: municipio_top if x == municipio_top else label_resto
    )
    df_renta = df_renta.sort_values("es_focal", ascending=False)

    mapa_categorias = {k: v for k, v in MAPA_EDUCACION.items() if isinstance(k, str)}
    orden_educativo = ['Sin Estudios/Otros', 'Básicos', 'Medios', 'Superiores']
    col_estudios    = template_ia_social["col_estudios"]

    df_social = enriquecer_nivelestudios.copy()
    if "Sexo" in df_social.columns:
        df_social = df_social[df_social["Sexo"] == "Total"]
    if "ISLA_clean" in df_social.columns:
        df_social = df_social[df_social["ISLA_clean"] == territorio]
    df_social["Categoria"] = df_social[col_estudios].map(mapa_categorias).fillna("Sin Estudios/Otros")
    df_social["Categoria"] = pd.Categorical(df_social["Categoria"], categories=orden_educativo, ordered=True)
    df_social["Total"]     = pd.to_numeric(df_social["Total"], errors="coerce").fillna(0)
    df_social = (
        df_social.groupby(["Periodo", "Categoria"])["Total"]
        .sum().reset_index().rename(columns={"Total": "n"})
    )

    # ── Llamadas al LLM en paralelo ───────────────────────────────────────────

    context.log.info("Llamando al LLM en paralelo para ambos gráficos...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        fut_renta  = pool.submit(_llamar_ia, template_ia_renta["payload"],  context)
        fut_social = pool.submit(_llamar_ia, template_ia_social["payload"], context)
        bloque_renta  = fut_renta.result()
        bloque_social = fut_social.result()

    context.log.info(f"Bloque renta ({len(bloque_renta)} chars): {bloque_renta[:120]}…")
    context.log.info(f"Bloque social ({len(bloque_social)} chars): {bloque_social[:120]}…")

    # ── Ejecución y guardado ──────────────────────────────────────────────────

    rutas = []
    for bloque, df, nombre in [
        (bloque_renta,  df_renta,  f"visualizacion_ia_renta_{territorio_key}.png"),
        (bloque_social, df_social, f"visualizacion_ia_social_{territorio_key}.png"),
    ]:
        grafico = _ejecutar_bloque(bloque, df, extra_vars={}, context=context)
        ruta    = os.path.join(DIR_GRAFICOS, nombre)
        grafico.save(ruta, width=12, height=7, dpi=150)
        context.log.info(f"Guardado: {ruta}")
        rutas.append(ruta)

    sizes = {os.path.basename(r): round(os.path.getsize(r) / 1024, 1) for r in rutas}
    return Output(
        value=rutas,
        metadata={
            "rutas":           MetadataValue.text(str(rutas)),
            "sizes_kb":        MetadataValue.text(str(sizes)),
            "territorio":      MetadataValue.text(territorio),
            "bloque_renta":    MetadataValue.md(f"```python\n{bloque_renta}\n```"),
            "bloque_social":   MetadataValue.md(f"```python\n{bloque_social}\n```"),
        },
    )


@asset
def commit_visualizacion_ia(
    context: OpExecutionContext,
    visualizacion_ia_png: list,
) -> None:
    commit_and_push(
        repo_dir=REPO_DIR,
        remote_url=repo_url(get_github_token()),
        branch=GIT_BRANCH,
        files=visualizacion_ia_png,
        message="practica4: gráficos IA — " + ", ".join(
            os.path.basename(f) for f in visualizacion_ia_png
        ),
        ctx=context,
    )