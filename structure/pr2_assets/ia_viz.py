"""
assets/ia_viz.py — Pipeline de generación de gráficos mediante IA (Práctica 4).

Assets en orden de ejecución:
  integrar_renta_codislas ──► template_ia ──► codigo_generado_ia
                                                      │
                                             visualizacion_ia_png
                                                      │
                                            commit_visualizacion_ia

Responsabilidades:
  - template_ia:            construye el prompt siguiendo la gramática de gráficos
                            de Wickham, parametrizado con las variables reales del dataset.
  - codigo_generado_ia:     llama al servicio LLM, limpia la respuesta y valida
                            que el resultado es código Python ejecutable.
  - visualizacion_ia_png:   ejecuta el código en un entorno controlado con exec()
                            e inyecta plotnine + pandas, igual que el ejemplo de la práctica.
  - commit_visualizacion_ia: sube el PNG resultante a GitHub reutilizando commit_and_push.
"""

import os
import re
import warnings

import requests
import pandas as pd
from dagster import asset, Output, OpExecutionContext, MetadataValue

from config import DIR_GRAFICOS, GIT_BRANCH, REPO_DIR, Dashboard, repo_url
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

    Estrategia en dos pasos (igual que el ejemplo de la profesora pero corregido):
      1. Si hay bloque markdown ```python … ```, extraer su contenido.
      2. Si no, buscar la primera línea 'def ' y devolver desde ahí hasta el final,
         descartando texto explicativo previo y líneas de comentario Markdown (###, -).
    """
    # Paso 1: bloque markdown explícito
    match = re.search(r"```(?:python)?\s*(.*?)```", texto, re.DOTALL)
    if match:
        return match.group(1).strip()

    # Paso 2: aislar la función ignorando prosa y viñetas
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


def _validar_codigo(codigo: str) -> None:
    """
    Lanza ValueError si el código no cumple los requisitos mínimos:
      - Debe definir la función 'generar_plot' (nombre exigido por el template).
      - Debe contener una llamada a ggplot (garantiza que es código plotnine).
    """
    if "def generar_plot" not in codigo:
        raise ValueError(
            "El código generado no contiene 'def generar_plot'. "
            "La IA no respetó el template. Respuesta recibida:\n" + codigo
        )
    if "ggplot" not in codigo:
        raise ValueError(
            "El código generado no contiene 'ggplot'. "
            "La IA no generó una visualización plotnine válida."
        )


# ── Assets ─────────────────────────────────────────────────────────────────────

@asset
def template_ia(
    context: OpExecutionContext,
    integrar_renta_codislas: pd.DataFrame,
) -> dict:
    """
    Construye el payload JSON para el servicio LLM.

    La descripción sigue la gramática de gráficos de Wickham/Wilkinson:
      Datos → Estéticas → Geometría → Escalas → Etiquetas → Tema → Gestalt.

    El territorio y la fuente se leen desde config.Dashboard para que este
    asset sea siempre coherente con el resto del pipeline.

    Devuelve un diccionario listo para pasarse a requests.post(..., json=payload).
    """
    columnas   = ", ".join(integrar_renta_codislas.columns)
    territorio = Dashboard.TERRITORIO

    # Estructura obligatoria que la IA debe respetar
    template_tecnico = """
def generar_plot(df):
    # El código debe seguir esta estructura:
    # plot = (ggplot(df, aes(...)) + geom_... + ...)
    # return plot
"""

    system_content = (
        "Eres un experto en la gramática de gráficos y Plotnine. "
        "Tu tarea es traducir descripciones en lenguaje natural a código Python ejecutable. "
        f"Usa siempre este template: {template_tecnico}. "
        "Devuelve EXCLUSIVAMENTE el código Python, sin explicaciones ni bloques markdown. "
        "La función debe llamarse exactamente 'generar_plot' y recibir un DataFrame 'df'."
    )

    # Descripción estructurada siguiendo la gramática de gráficos de Wickham:
    # Cada sección corresponde a una capa de la gramática (datos, estéticas,
    # geometría, escalas, etiquetas) más el principio Gestalt a aplicar.
    descripcion_grafico = f"""
Dataset: df con columnas [{columnas}].

Filtros a aplicar DENTRO de la función antes de graficar:
  - Filtrar ISLA_clean == '{territorio}'.
  - Filtrar Fuente_Renta_Code == 'SUELDOS_SALARIOS'.
  - Excluir filas donde Territorio == '{territorio}' (mostrar solo municipios, no el total de isla).

Estéticas (aes) — mapeo variable → canal visual:
  - Eje X: 'Año' (numérica, dimensión temporal).
  - Eje Y: 'Porcentaje' (numérica, valor de renta).
  - Color y grupo: 'Territorio' (categórica, una línea por municipio).

Geometría:
  - geom_line(size=0.8) para trazar la evolución temporal de cada municipio.
  - geom_point(size=1.5) sobre las mismas estéticas para marcar los valores anuales.

Escalas:
  - scale_color_manual: calcular el municipio con mayor media de Porcentaje dentro
    de la función y asignarle el color '#E63946' (rojo énfasis). El resto reciben
    '#CCCCCC' (gris neutro). Esto implementa el principio Gestalt de Punto Focal.
  - scale_x_continuous con breaks cada 2 años.

Etiquetas (labs):
  - title: 'Evolución de Salarios por Municipio — {territorio}'.
  - subtitle: 'Fuente: ISTAC · Distribución de Renta en Canarias'.
  - x: 'Año'.
  - y: 'Porcentaje (%)'.
  - color: 'Municipio'.

Tema: theme_minimal() con el título en negrita (face='bold') y tamaño 13.

Principio Gestalt aplicado — Punto Focal:
  El municipio con mayor media se resalta en rojo oscuro; el resto en gris claro.
  Esto dirige la atención del lector sin sobrecargar la leyenda de color.
"""

    payload = {
        "model":       IA_MODEL,
        "temperature": 0.1,   # Muy baja: código reproducible, sin creatividad
        "stream":      False,
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user",   "content": f"Basándote en esta descripción, completa el template:\n{descripcion_grafico}"},
        ],
    }

    context.log.info(
        f"Template IA construido · territorio='{territorio}' · "
        f"columnas disponibles: {columnas}"
    )
    return payload


@asset
def codigo_generado_ia(
    context: OpExecutionContext,
    template_ia: dict,
) -> Output:
    """
    Envía el payload al servicio LLM y devuelve código Python limpio y validado.

    Pasos internos:
      1. POST al endpoint del servicio con timeout de 120 s.
      2. Extrae el contenido de choices[0].message.content.
      3. Limpia la respuesta (elimina markdown, prosa, viñetas).
      4. Valida que el código contiene 'def generar_plot' y 'ggplot'.

    Devuelve Output con el código como value y metadatos de auditoría.
    """
    context.log.info(f"Llamando al servicio IA: {IA_URL}")

    try:
        response = requests.post(
            IA_URL,
            headers={
                "Content-Type":  "application/json",
                "Authorization": f"Bearer {IA_TOKEN}",
            },
            json=template_ia,
            timeout=120,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error al contactar el servicio IA: {e}") from e

    data    = response.json()
    raw     = data["choices"][0]["message"]["content"]
    context.log.info(f"Respuesta IA recibida · {len(raw)} caracteres.")

    codigo = _limpiar_codigo(raw)
    context.log.info("Código limpiado · markdown y texto explicativo eliminados.")

    _validar_codigo(codigo)
    context.log.info("Código validado · contiene 'def generar_plot' y 'ggplot'.")

    return Output(
        value=codigo,
        metadata={
            "longitud_codigo":     MetadataValue.int(len(codigo)),
            "contiene_ggplot":     MetadataValue.bool("ggplot" in codigo),
            "contiene_geom_line":  MetadataValue.bool("geom_line" in codigo),
            "contiene_geom_point": MetadataValue.bool("geom_point" in codigo),
            "modelo_usado":        MetadataValue.text(IA_MODEL),
            "codigo_completo":     MetadataValue.md(f"```python\n{codigo}\n```"),
        },
    )


@asset
def visualizacion_ia_png(
    context: OpExecutionContext,
    codigo_generado_ia: str,
    integrar_renta_codislas: pd.DataFrame,
) -> Output:
    """
    Ejecuta el código generado por la IA con exec() en un entorno controlado
    y guarda el gráfico resultante como PNG en DIR_GRAFICOS.

    Entorno de ejecución inyectado (mismo patrón que el ejemplo de la práctica):
      - Todas las funciones públicas de plotnine (ggplot, aes, geom_line…).
      - plotnine como módulo completo.
      - pandas como 'pd'.

    La clave del diccionario debe coincidir con el nombre de la función que la IA
    generó en el template: 'generar_plot'.
    """
    import plotnine

    warnings.filterwarnings("ignore")
    os.makedirs(DIR_GRAFICOS, exist_ok=True)

    # Construimos el entorno con todo lo que el código de la IA necesitará
    entorno_ejecucion = {}
    entorno_ejecucion.update(
        {k: v for k, v in plotnine.__dict__.items() if not k.startswith("_")}
    )
    entorno_ejecucion["plotnine"] = plotnine
    entorno_ejecucion["pd"]       = pd

    try:
        exec(codigo_generado_ia, entorno_ejecucion)  # noqa: S102
    except Exception as e:
        raise RuntimeError(
            f"Error al ejecutar el código generado por la IA: {e}"
        ) from e

    if "generar_plot" not in entorno_ejecucion:
        raise RuntimeError(
            "La función 'generar_plot' no quedó definida tras exec(). "
            "El nombre de la función en el código no coincide con el template."
        )

    # Invocamos la función almacenada en el diccionario del entorno
    grafico = entorno_ejecucion["generar_plot"](integrar_renta_codislas)

    territorio = Dashboard.TERRITORIO.lower().replace(" ", "_")
    ruta       = os.path.join(DIR_GRAFICOS, f"visualizacion_ia_{territorio}.png")
    grafico.save(ruta, width=12, height=7, dpi=150)
    context.log.info(f"Gráfico IA guardado en: {ruta}")

    size_kb = round(os.path.getsize(ruta) / 1024, 1)

    return Output(
        value=ruta,
        metadata={
            "ruta":       MetadataValue.text(ruta),
            "size_kb":    MetadataValue.float(size_kb),
            "territorio": MetadataValue.text(Dashboard.TERRITORIO),
            "mensaje":    MetadataValue.text("Gráfico generado por IA y guardado correctamente."),
        },
    )


@asset
def commit_visualizacion_ia(
    context: OpExecutionContext,
    visualizacion_ia_png: str,
) -> None:
    """
    Hace git add + commit + push del PNG generado por la IA.

    Tras el push, si GitHub Pages está activo, el gráfico estará disponible en:
      https://<REPO_OWNER>.github.io/<REPO_NAME>/graficos/<nombre>.png

    El token se lee desde la variable de entorno GITHUB_TOKEN, nunca se
    recibe como parámetro para evitar que Dagster lo persista en disco.
    """
    commit_and_push(
        repo_dir=REPO_DIR,
        remote_url=repo_url(get_github_token()),
        branch=GIT_BRANCH,
        files=[visualizacion_ia_png],
        message=f"practica4: gráfico IA — {os.path.basename(visualizacion_ia_png)}",
        ctx=context,
    )
