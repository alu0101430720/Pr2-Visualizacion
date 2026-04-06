"""
assets/ia_viz.py — Pipeline de generación de gráficos mediante IA (Práctica 4).

Assets en orden de ejecución:
  integrar_renta_codislas ──► template_ia_renta     ──► codigo_generado_ia_renta
  enriquecer_nivelestudios ─►                                     │
                              template_ia_social    ──► codigo_generado_ia_social
                                                                  │
                                                       visualizacion_ia_png  (genera ambos PNG)
                                                                  │
                                                       commit_visualizacion_ia

Gráficos generados:
  1. visualizacion_ia_renta_<territorio>.png   — evolución de salarios por municipio
     con Punto Focal (municipio con mayor media resaltado en rojo).
  2. visualizacion_ia_social_<territorio>.png  — cruce renta × nivel de estudios
     (área apilada por categoría de estudios, facetada por isla si procede).
"""

import os
import re
import warnings

import requests
import pandas as pd
from dagster import asset, Output, OpExecutionContext, MetadataValue

from config import DIR_GRAFICOS, GIT_BRANCH, REPO_DIR, Dashboard, MAPA_EDUCACION, repo_url
from utils import commit_and_push
from pr2_assets.git_ops import get_github_token


# ── Constantes del servicio IA ─────────────────────────────────────────────────

IA_URL   = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
IA_MODEL = "ollama/llama3.1:8b"
IA_TOKEN = "sk-1234"


# ── Helpers privados ───────────────────────────────────────────────────────────

def _limpiar_codigo(texto: str) -> str:
    """
    Extrae únicamente el bloque de código Python de la respuesta de la IA.
    Estrategia en dos pasos:
      1. Si hay bloque markdown ```python … ```, extraer su contenido.
      2. Si no, buscar la primera línea 'def ' y devolver desde ahí,
         descartando prosa previa y líneas de comentario Markdown (###, -).
    """
    match = re.search(r"```(?:python)?\s*(.*?)```", texto, re.DOTALL)
    if match:
        return match.group(1).strip()

    lineas = texto.strip().splitlines()
    inicio = next(
        (i for i, l in enumerate(lineas) if l.strip().startswith("def ")),
        None,
    )
    if inicio is not None:
        lineas_validas = [
            l for l in lineas[inicio:]
            if not l.strip().startswith("###") and not l.strip().startswith("- ")
        ]
        return "\n".join(lineas_validas).strip()

    return texto.strip()


def _validar_codigo(codigo: str, nombre_funcion: str = "generar_plot") -> None:
    """Lanza ValueError si el código no contiene la función esperada ni ggplot."""
    if f"def {nombre_funcion}" not in codigo:
        raise ValueError(
            f"El código generado no contiene 'def {nombre_funcion}'. "
            "La IA no respetó el template.\n" + codigo
        )
    if "ggplot" not in codigo:
        raise ValueError("El código generado no contiene 'ggplot'.")


def _llamar_ia(payload: dict, context: OpExecutionContext) -> str:
    """Realiza la petición al servicio LLM y devuelve el código limpio y validado."""
    context.log.info(f"Llamando al servicio IA: {IA_URL}")
    try:
        response = requests.post(
            IA_URL,
            headers={
                "Content-Type":  "application/json",
                "Authorization": f"Bearer {IA_TOKEN}",
            },
            json=payload,
            timeout=120,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error al contactar el servicio IA: {e}") from e

    raw = response.json()["choices"][0]["message"]["content"]
    context.log.info(f"Respuesta IA recibida · {len(raw)} caracteres.")
    return _limpiar_codigo(raw)


def _ejecutar_codigo(codigo: str, df: pd.DataFrame, nombre_funcion: str = "generar_plot"):
    """Ejecuta el código en un entorno controlado con plotnine y pandas inyectados."""
    import plotnine
    entorno = {}
    entorno.update({k: v for k, v in plotnine.__dict__.items() if not k.startswith("_")})
    entorno["plotnine"] = plotnine
    entorno["pd"]       = pd

    try:
        exec(codigo, entorno)  # noqa: S102
    except Exception as e:
        raise RuntimeError(f"Error al ejecutar el código de la IA: {e}") from e

    if nombre_funcion not in entorno:
        raise RuntimeError(
            f"La función '{nombre_funcion}' no quedó definida tras exec(). "
            "El nombre no coincide con el template."
        )
    return entorno[nombre_funcion](df)


# ── Assets ─────────────────────────────────────────────────────────────────────

@asset
def template_ia_renta(
    context: OpExecutionContext,
    integrar_renta_codislas: pd.DataFrame,
) -> dict:
    """
    Construye el payload para el gráfico 1: evolución de salarios por municipio.

    Gramática de gráficos aplicada:
      Datos    → df filtrado por isla y fuente SUELDOS_SALARIOS.
      Estéticas → x=Año, y=Porcentaje, color/group=Territorio.
      Geometría → geom_line + geom_point.
      Escalas   → scale_color_manual con Punto Focal (municipio top en rojo).
      Etiquetas → título, subtítulo, ejes, leyenda.
      Gestalt   → Punto Focal: municipio con mayor media resaltado, resto en gris.
    """
    columnas   = ", ".join(integrar_renta_codislas.columns)
    territorio = Dashboard.TERRITORIO

    template_tecnico = """
def generar_plot(df):
    # plot = (ggplot(df, aes(...)) + geom_... + ...)
    # return plot
"""
    system_content = (
        "Eres un experto en la gramática de gráficos y Plotnine. "
        "Tu tarea es traducir descripciones en lenguaje natural a código Python ejecutable. "
        f"Usa siempre este template: {template_tecnico}. "
        "Devuelve EXCLUSIVAMENTE el código Python, sin explicaciones ni markdown. "
        "La función debe llamarse exactamente 'generar_plot' y recibir un DataFrame 'df'."
    )

    descripcion = f"""
Dataset: df con columnas [{columnas}].

Filtros a aplicar DENTRO de la función:
  - Filtrar ISLA_clean == '{territorio}'.
  - Filtrar Fuente_Renta_Code == 'SUELDOS_SALARIOS'.
  - Excluir filas donde Territorio == '{territorio}' (solo municipios, no el total).

Estéticas (aes):
  - x: 'Año' (numérica, eje temporal).
  - y: 'Porcentaje' (numérica, valor de renta).
  - color y group: 'Territorio' (categórica, una línea por municipio).

Geometría:
  - geom_line(size=0.8).
  - geom_point(size=1.5).

Escalas:
  - scale_color_manual: calcular dentro de la función el municipio con mayor media
    de Porcentaje y asignarle '#E63946'. El resto reciben '#CCCCCC'.
    Esto aplica el principio Gestalt de Punto Focal.
  - scale_x_continuous con breaks cada 2 años.

Etiquetas (labs):
  - title: 'Evolución de Salarios por Municipio — {territorio}'.
  - subtitle: 'Fuente: ISTAC · Distribución de Renta en Canarias'.
  - x: 'Año', y: 'Porcentaje (%)', color: 'Municipio'.

Tema: theme_minimal(), título en negrita tamaño 13.

Principio Gestalt — Punto Focal:
  Municipio con mayor media en rojo oscuro, resto en gris claro.
"""
    payload = {
        "model": IA_MODEL, "temperature": 0.1, "stream": False,
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user",   "content": f"Completa el template:\n{descripcion}"},
        ],
    }
    context.log.info(f"Template renta construido · territorio='{territorio}'")
    return payload


@asset
def template_ia_social(
    context: OpExecutionContext,
    enriquecer_nivelestudios: pd.DataFrame,
    integrar_renta_codislas: pd.DataFrame,
) -> dict:
    """
    Construye el payload para el gráfico 2: cruce renta × nivel de estudios.

    Gramática de gráficos aplicada:
      Datos     → nivelestudios enriquecido, agrupado por Periodo + Categoria.
      Estéticas → x=Periodo, y=Total, fill=Categoria (área apilada).
      Geometría → geom_area(position='fill') — proporciones normalizadas a 100%.
      Escalas   → scale_fill_manual con paleta semántica por nivel educativo.
      Etiquetas → título, subtítulo, ejes, leyenda.
      Gestalt   → Similitud: cada color = un nivel educativo constante en todo el gráfico.
    """
    territorio  = Dashboard.TERRITORIO
    col_estudios = next(
        (c for c in enriquecer_nivelestudios.columns
         if "estudio" in c.lower() or "nivel" in c.lower()),
        None,
    )
    columnas = ", ".join(enriquecer_nivelestudios.columns)

    template_tecnico = """
def generar_plot_social(df):
    # plot = (ggplot(df, aes(...)) + geom_... + ...)
    # return plot
"""
    system_content = (
        "Eres un experto en la gramática de gráficos y Plotnine. "
        "Tu tarea es traducir descripciones en lenguaje natural a código Python ejecutable. "
        f"Usa siempre este template: {template_tecnico}. "
        "Devuelve EXCLUSIVAMENTE el código Python, sin explicaciones ni markdown. "
        "La función debe llamarse exactamente 'generar_plot_social' y recibir un DataFrame 'df'."
    )

    mapa_str = str(MAPA_EDUCACION)
    descripcion = f"""
Dataset: df con columnas [{columnas}].
{'Columna de nivel de estudios detectada: ' + col_estudios if col_estudios else 'Busca una columna que contenga la palabra estudio o nivel.'}

Pasos de preparación DENTRO de la función (antes de graficar):
  1. Filtrar ISLA_clean == '{territorio}' si la columna existe.
  2. Si existe columna 'Sexo', filtrar Sexo == 'Total' para no duplicar filas.
  3. Mapear los valores de la columna de nivel de estudios usando este diccionario
     para reducir a 4 categorías: {mapa_str}.
     Guardar el resultado en una columna nueva llamada 'Categoria'.
  4. Convertir 'Total' a numérico con pd.to_numeric(..., errors='coerce').fillna(0).
  5. Agrupar por ['Periodo', 'Categoria'] y sumar 'Total'.
     Renombrar la suma a 'n'.

Estéticas (aes):
  - x: 'Periodo' (numérica, eje temporal).
  - y: 'n' (numérica, frecuencia absoluta).
  - fill: 'Categoria' (categórica, nivel educativo).

Geometría:
  - geom_area(position='fill') — áreas apiladas normalizadas al 100%.

Escalas:
  - scale_fill_manual con estos colores fijos por categoría:
      'Básicos': '#E63946', 'Medios': '#457B9D',
      'Superiores': '#2A9D8F', 'Sin Estudios/Otros': '#CCCCCC'.
  - scale_y_continuous con labels en formato porcentaje (usa lambda x: f'{{x*100:.0f}}%').
  - scale_x_continuous con breaks cada 2 años.

Etiquetas (labs):
  - title: 'Distribución del Nivel de Estudios — {territorio}'.
  - subtitle: 'Fuente: ISTAC · Encuesta de Nivel y Condiciones de Vida'.
  - x: 'Año', y: 'Proporción (%)', fill: 'Nivel educativo'.

Tema: theme_minimal(), título en negrita tamaño 13.

Principio Gestalt — Similitud:
  Cada color representa siempre el mismo nivel educativo en todo el gráfico.
  Los colores cálidos (rojo) señalan los niveles básicos como alerta visual.
"""
    payload = {
        "model": IA_MODEL, "temperature": 0.1, "stream": False,
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user",   "content": f"Completa el template:\n{descripcion}"},
        ],
    }
    context.log.info(f"Template social construido · territorio='{territorio}'")
    return payload


@asset
def codigo_generado_ia_renta(
    context: OpExecutionContext,
    template_ia_renta: dict,
) -> Output:
    """Llama al LLM con el template de renta y devuelve código Python validado."""
    codigo = _llamar_ia(template_ia_renta, context)
    _validar_codigo(codigo, "generar_plot")
    context.log.info("Código renta validado.")
    return Output(
        value=codigo,
        metadata={
            "longitud_codigo":     MetadataValue.int(len(codigo)),
            "contiene_ggplot":     MetadataValue.bool("ggplot" in codigo),
            "contiene_geom_line":  MetadataValue.bool("geom_line" in codigo),
            "contiene_scale_color":MetadataValue.bool("scale_color_manual" in codigo),
            "modelo_usado":        MetadataValue.text(IA_MODEL),
            "codigo_completo":     MetadataValue.md(f"```python\n{codigo}\n```"),
        },
    )


@asset
def codigo_generado_ia_social(
    context: OpExecutionContext,
    template_ia_social: dict,
) -> Output:
    """Llama al LLM con el template social y devuelve código Python validado."""
    codigo = _llamar_ia(template_ia_social, context)
    _validar_codigo(codigo, "generar_plot_social")
    context.log.info("Código social validado.")
    return Output(
        value=codigo,
        metadata={
            "longitud_codigo":    MetadataValue.int(len(codigo)),
            "contiene_ggplot":    MetadataValue.bool("ggplot" in codigo),
            "contiene_geom_area": MetadataValue.bool("geom_area" in codigo),
            "contiene_scale_fill":MetadataValue.bool("scale_fill_manual" in codigo),
            "modelo_usado":       MetadataValue.text(IA_MODEL),
            "codigo_completo":    MetadataValue.md(f"```python\n{codigo}\n```"),
        },
    )


@asset
def visualizacion_ia_png(
    context: OpExecutionContext,
    codigo_generado_ia_renta: str,
    codigo_generado_ia_social: str,
    integrar_renta_codislas: pd.DataFrame,
    enriquecer_nivelestudios: pd.DataFrame,
) -> Output:
    """
    Ejecuta ambos códigos generados por la IA y guarda los dos PNG en DIR_GRAFICOS.

    Gráfico 1 (renta):   visualizacion_ia_renta_<territorio>.png
    Gráfico 2 (social):  visualizacion_ia_social_<territorio>.png

    Entorno de ejecución: plotnine completo + pandas inyectados via exec().
    """
    warnings.filterwarnings("ignore")
    os.makedirs(DIR_GRAFICOS, exist_ok=True)

    territorio = Dashboard.TERRITORIO.lower().replace(" ", "_")
    rutas = []

    # ── Gráfico 1: renta por municipio ────────────────────────────────────────
    context.log.info("Ejecutando código renta...")
    grafico_renta = _ejecutar_codigo(
        codigo_generado_ia_renta, integrar_renta_codislas, "generar_plot"
    )
    ruta_renta = os.path.join(DIR_GRAFICOS, f"visualizacion_ia_renta_{territorio}.png")
    grafico_renta.save(ruta_renta, width=12, height=7, dpi=150)
    context.log.info(f"Gráfico renta guardado: {ruta_renta}")
    rutas.append(ruta_renta)

    # ── Gráfico 2: nivel de estudios ─────────────────────────────────────────
    context.log.info("Ejecutando código social...")
    grafico_social = _ejecutar_codigo(
        codigo_generado_ia_social, enriquecer_nivelestudios, "generar_plot_social"
    )
    ruta_social = os.path.join(DIR_GRAFICOS, f"visualizacion_ia_social_{territorio}.png")
    grafico_social.save(ruta_social, width=12, height=7, dpi=150)
    context.log.info(f"Gráfico social guardado: {ruta_social}")
    rutas.append(ruta_social)

    sizes = {os.path.basename(r): round(os.path.getsize(r) / 1024, 1) for r in rutas}

    return Output(
        value=rutas,
        metadata={
            "rutas":      MetadataValue.text(str(rutas)),
            "sizes_kb":   MetadataValue.text(str(sizes)),
            "territorio": MetadataValue.text(Dashboard.TERRITORIO),
            "mensaje":    MetadataValue.text("Ambos gráficos generados por IA correctamente."),
        },
    )


@asset
def commit_visualizacion_ia(
    context: OpExecutionContext,
    visualizacion_ia_png: list,
) -> None:
    """
    Hace git add + commit + push de los dos PNG generados por la IA.
    Tras el push estarán disponibles en GitHub Pages.
    """
    commit_and_push(
        repo_dir=REPO_DIR,
        remote_url=repo_url(get_github_token()),
        branch=GIT_BRANCH,
        files=visualizacion_ia_png,
        message=(
            "practica4: gráficos IA — "
            + ", ".join(os.path.basename(f) for f in visualizacion_ia_png)
        ),
        ctx=context,
    )