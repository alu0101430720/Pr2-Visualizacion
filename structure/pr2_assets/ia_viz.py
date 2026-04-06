"""

assets/ia_viz.py — Pipeline de generación de gráficos mediante IA (Práctica 4).



Flujo de assets:

  integrar_renta_codislas ──► template_ia_renta  ──► codigo_generado_ia_renta ──┐

  enriquecer_nivelestudios ──► template_ia_social ──► codigo_generado_ia_social ─┤

                                                                                  ▼

                                                                      visualizacion_ia_png

                                                                                  │

                                                                      commit_visualizacion_ia



Estrategia:

  - template_ia_*:          construye el prompt siguiendo la gramática de Wickham.

                            Los colores del Punto Focal se resuelven en Python con _paleta_focal()

                            ANTES de enviarlo a la IA, para que el LLM solo copie

                            un dict ya construido sin inventar nada.

  - codigo_generado_ia_*:   llama al LLM, limpia la respuesta y valida sintaxis.

  - _corregir_codigo():     parchea errores sintácticos frecuentes del LLM antes

                            de ejecutar con exec().

  - visualizacion_ia_png:   ejecuta el código generado en un entorno controlado

                            con plotnine + pandas inyectados.

  - commit_visualizacion_ia: sube los PNG a GitHub Pages.

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

IA_MODEL = "ollama/deepseek-coder:6.7b-instruct-q4_K_M"

IA_TOKEN = "sk-1234"





# ── Colores para Punto Focal ──────────────────────────────────────────────────

# Para los gráficos de renta usamos Punto Focal (Gestalt):

# un color de énfasis para el municipio destacado, gris neutro para el resto.

# El resto de paletas (Set2, Dark2, Blues…) las gestiona plotnine nativamente

# con scale_fill_brewer / scale_color_brewer — no hace falta redefinirlas.



COLOR_FOCAL  = "#D95F02"  # naranja oscuro — énfasis cálido sin alarmar

COLOR_NEUTRO = "#CCCCCC"  # gris neutro — fondo / no-foco





def _paleta_focal(categorias: list, focal: str) -> dict:

    """

    Devuelve {categoria: color} para aplicar Punto Focal (Gestalt).

    La categoría 'focal' recibe COLOR_FOCAL; el resto, COLOR_NEUTRO.

    Para cualquier otra necesidad de color usar directamente

    scale_fill_brewer() o scale_color_brewer() de plotnine.

    """

    return {c: (COLOR_FOCAL if c == focal else COLOR_NEUTRO) for c in categorias}





# ── Limpieza y corrección del código generado por el LLM ──────────────────────



def _limpiar_codigo(texto: str) -> str:

    """

    Extrae el bloque de código Python de la respuesta del LLM.

    1. Busca bloque markdown ```python…```.

    2. Si no hay, busca la primera línea 'def ' y descarta prosa anterior.

    """

    match = re.search(r"```(?:python)?\s*(.*?)```", texto, re.DOTALL)

    if match:

        return match.group(1).strip()



    lineas = texto.strip().splitlines()

    inicio = next(

        (i for i, l in enumerate(lineas) if l.strip().startswith("def ")), None

    )

    if inicio is not None:

        return "\n".join(

            l for l in lineas[inicio:]

            if not l.strip().startswith("###") and not l.strip().startswith("- ")

        ).strip()



    return texto.strip()





def _corregir_codigo(codigo: str, context: OpExecutionContext = None) -> str:

    """

    Parchea errores sintácticos frecuentes que los LLMs pequeños cometen

    al generar código plotnine. Se aplica ANTES de exec().



    Correcciones:

      1. Comillas tipográficas → ASCII.

      2. scale_*_manual([dict-like]) → scale_*_manual(values={...}).

      3. scale_*_manual({...}) sin keyword → scale_*_manual(values={...}).

      4. scale_y_continuous(labels=[lista]) → scale_y_continuous().

      5. Clave fusionada 'Municipio:#RRGGBB':'#RRGGBB' → 'Municipio':'#RRGGBB'.

      6. scale_*_manual(values={set}) → scale_*_manual(values={dict reconstruido}).

    """

    original = codigo



    # 1. Comillas tipográficas

    codigo = (codigo

              .replace("\u2018", "'").replace("\u2019", "'")

              .replace("\u201c", '"').replace("\u201d", '"'))



    # 2. scale_*_manual([...]) → values={...}

    def _list_to_dict(m):

        return f"{m.group(1)}(values={{{m.group(2)}}})"

    codigo = re.sub(r"(scale_\w+_manual)\(\[([^\]]+)\]\)", _list_to_dict, codigo)



    # 3. scale_*_manual({...}) sin keyword values

    codigo = re.sub(r"(scale_\w+_manual)\(\{", r"\1(values={", codigo)



    # 4. scale_y_continuous(labels=[lista]) → scale_y_continuous()

    codigo = re.sub(r"scale_y_continuous\(labels=\[[^\]]*\]\)", "scale_y_continuous()", codigo)



    # 5. Clave fusionada 'Nombre:#RRGGBB': 'valor' → 'Nombre': '#RRGGBB'

    def _fix_fused(m):

        q1, nombre, hexcol, q2 = m.group(1), m.group(2).strip(), m.group(3), m.group(4)

        return q1 + nombre + q1 + ": " + q2 + "#" + hexcol + q2



    _p = (r"(['\"])([^'\"]+):#([0-9A-Fa-f]{3,6})"

          + r"\1" + r"\s*:\s*" + r"(['\"])[^'\"]*" + r"\4")

    codigo = re.sub(_p, _fix_fused, codigo)



    # 6. scale_*_manual(values={set sin ':' }) → reconstruir con paleta Dark2

    def _fix_set(m):

        func  = m.group(1)

        items = [i.strip().strip("'\"") for i in m.group(2).split(",") if i.strip()]

        pal   = ["#1B9E77", "#D95F02", "#7570B3", "#E7298A",

                 "#66A61E", "#E6AB02", "#A6761D", "#666666"]

        pairs = ", ".join(f"'{it}': '{pal[i % len(pal)]}'" for i, it in enumerate(items))

        return f"{func}(values={{{pairs}}})"

    codigo = re.sub(r"(scale_\w+_manual)\(values=\{([^}:]+)\}\)", _fix_set, codigo)



    # 7. Lambda anidado con parentesis sin cerrar.

    # El LLM genera .apply(lambda x: 'A' if ... else('B' if ...))

    # en varias lineas olvidando cerrar el parentesis de .apply().

    # compile() lo detecta y se reemplaza toda la asignacion de 'Categoria'

    # por _categorizar(), funcion auxiliar inyectada al inicio de la funcion.

    try:

        compile(codigo, '<check>', 'exec')

    except SyntaxError as e:

        if 'never closed' in str(e.msg) or 'was not closed' in str(e.msg):

            pat = r"\.apply\(lambda[^)]*\n(?:[ \t]+[^\n]*\n)*?(?=\s*\n\s*df\[)"

            codigo = re.sub(

                pat,

                ".apply(_categorizar)",

                codigo,

                flags=re.DOTALL,

            )

            if '_categorizar' in codigo and 'def _categorizar' not in codigo:

                fn_lines = [

                    '    def _categorizar(x):',

                    "        x = str(x).lower()",

                    "        if 'primaria' in x or 'primera etapa' in x:",

                    "            return 'Basicos'",

                    "        elif 'segunda etapa' in x:",

                    "            return 'Medios'",

                    "        elif 'superior' in x:",

                    "            return 'Superiores'",

                    "        else:",

                    "            return 'Sin Estudios/Otros'",

                ]

                fn_str = '\n'.join(fn_lines) + '\n'

                codigo = re.sub(

                    r'(def generar_plot_social\(df\):[ \t]*\n)',

                    lambda m: m.group(0) + fn_str,

                    codigo,

                )

    if context and codigo != original:

        context.log.info("_corregir_codigo aplicó correcciones al código del LLM.")



    return codigo



def _validar_codigo(codigo: str, nombre_funcion: str) -> None:

    """Lanza ValueError si el código no contiene la función esperada o ggplot."""

    if f"def {nombre_funcion}" not in codigo:

        raise ValueError(

            f"El LLM no generó 'def {nombre_funcion}'. "

            "No respetó el template.\n" + codigo

        )

    if "ggplot" not in codigo:

        raise ValueError("El código no contiene 'ggplot' — no es código plotnine válido.")





def _llamar_ia(payload: dict, context: OpExecutionContext) -> str:

    """POST al servicio LLM. Devuelve el bloque de código extraído y limpiado."""

    context.log.info(f"Llamando al servicio IA ({IA_MODEL})...")

    try:

        resp = requests.post(

            IA_URL,

            headers={"Content-Type": "application/json",

                     "Authorization": f"Bearer {IA_TOKEN}"},

            json=payload,

            timeout=120,

        )

        resp.raise_for_status()

    except requests.exceptions.RequestException as e:

        raise RuntimeError(f"Error al contactar el servicio IA: {e}") from e



    raw = resp.json()["choices"][0]["message"]["content"]

    context.log.info(f"Respuesta IA: {len(raw)} caracteres.")

    return _limpiar_codigo(raw)





def _ejecutar_codigo(

    codigo: str,

    df: pd.DataFrame,

    nombre_funcion: str,

    context: OpExecutionContext = None,

):

    """

    Ejecuta el código con exec() en un entorno controlado.

    Inyecta plotnine completo + pandas.

    Aplica _corregir_codigo antes de ejecutar.

    Si falla, muestra el código con números de línea en los logs.

    """

    import plotnine



    codigo = _corregir_codigo(codigo, context)



    entorno = {k: v for k, v in plotnine.__dict__.items() if not k.startswith("_")}

    entorno["plotnine"] = plotnine

    entorno["pd"] = pd



    try:

        exec(codigo, entorno)  # noqa: S102

    except Exception as e:

        numerado = "\n".join(f"{i+1:>3}: {l}"

                             for i, l in enumerate(codigo.splitlines()))

        raise RuntimeError(

            f"Error ejecutando código del LLM: {e}\n--- código ---\n{numerado}"

        ) from e



    if nombre_funcion not in entorno:

        raise RuntimeError(

            f"La función '{nombre_funcion}' no quedó definida tras exec(). "

            "El LLM usó un nombre distinto al del template."

        )

    return entorno[nombre_funcion](df)





# ══════════════════════════════════════════════════════════════════════════════

# Assets

# ══════════════════════════════════════════════════════════════════════════════



@asset

def template_ia_renta(

    context: OpExecutionContext,

    integrar_renta_codislas: pd.DataFrame,

) -> dict:

    territorio = Dashboard.TERRITORIO

    columnas   = ", ".join(integrar_renta_codislas.columns)



    # Calcular municipio focal

    df_filt = integrar_renta_codislas[

        (integrar_renta_codislas["ISLA_clean"] == territorio) &

        (integrar_renta_codislas["Fuente_Renta_Code"] == "SUELDOS_SALARIOS") &

        (integrar_renta_codislas["Territorio"] != territorio)

    ]

    municipio_top = (

        df_filt.groupby("Territorio")["Porcentaje"].mean().idxmax()

        if not df_filt.empty else "Municipio Destacado"

    )

   

    label_resto = "Otros municipios"

    colores_focal = {municipio_top: COLOR_FOCAL, label_resto: COLOR_NEUTRO}



    template = (

        "def generar_plot(df):\n"

        "    # plot = (ggplot(df, aes(...)) + geom_... + ...)\n"

        "    # return plot\n"

    )

    system = (

        "Eres un experto en la gramática de gráficos y Plotnine. "

        "Traduce la descripción a código Python ejecutable siguiendo el template. "

        "Devuelve EXCLUSIVAMENTE el código Python, sin markdown ni explicaciones. "

        f"La función debe llamarse exactamente 'generar_plot' y recibir 'df'.\nTemplate:\n{template}"

    )

   

    descripcion = f"""Dataset: df con columnas [{columnas}].



Pasos dentro de la función:

  1. df = df[df['ISLA_clean'] == '{territorio}']

  2. df = df[df['Fuente_Renta_Code'] == 'SUELDOS_SALARIOS']

  3. df = df[df['Territorio'] != '{territorio}']

  4. Crear columna 'es_focal':

     df['es_focal'] = df['Territorio'].apply(

         lambda x: '{municipio_top}' if x == '{municipio_top}' else '{label_resto}')

  5. Ordenar el dataframe para que el foco se pinte al final (encima):

     df = df.sort_values('es_focal', ascending=False)



Estéticas (aes): x='Año', y='Porcentaje', color='es_focal', group='Territorio'



Geometría (ESTILO ESTRICTO):

  - geom_line(size=1.2)

  - geom_point(size=3, fill='white', stroke=1)



Escala de color y límites:

  colores = {colores_focal}

  scale_color_manual(values=colores)

  scale_x_continuous(breaks=list(range(2015, 2025, 2)))

  expand_limits(y=0)



labs(title='Evolución de Salarios por Municipio — {territorio}',

     subtitle='Fuente: ISTAC · Distribución de Renta en Canarias',

     x='Año', y='Porcentaje (%)', color='Municipio')



Tema (ESTILO ESTRICTO):

  theme_minimal() + theme(figure_size=(12, 8), legend_position='bottom', legend_title=element_text(fontweight='bold'))

"""



    context.log.info(f"Template renta · territorio='{territorio}' · focal='{municipio_top}'")

    return {

        "model": IA_MODEL, "temperature": 0.1, "stream": False,

        "messages": [

            {"role": "system", "content": system},

            {"role": "user",   "content": "Completa el template:\n" + descripcion},

        ],

    }



@asset

def template_ia_social(

    context: OpExecutionContext,

    enriquecer_nivelestudios: pd.DataFrame,

    integrar_renta_codislas: pd.DataFrame,

) -> dict:

    territorio   = Dashboard.TERRITORIO

    col_estudios = next(

        (c for c in enriquecer_nivelestudios.columns

         if "estudio" in c.lower() or "nivel" in c.lower()),

        "Nivel de estudios en curso",

    )



    mapa_categorias = {k: v for k, v in MAPA_EDUCACION.items() if isinstance(k, str)}



    # Template con el cascarón de la función completa

    template_con_datos = f"""def generar_plot_social(df):

    import pandas as pd

    if 'Sexo' in df.columns:

        df = df[df['Sexo'] == 'Total'].copy()

    if 'ISLA_clean' in df.columns:

        df = df[df['ISLA_clean'] == '{territorio}'].copy()

   

    _mapa = {mapa_categorias}

    df['Categoria'] = df['{col_estudios}'].map(_mapa).fillna('Sin Estudios/Otros')

   

    orden_educativo = ['Sin Estudios/Otros', 'Básicos', 'Medios', 'Superiores']

    df['Categoria'] = pd.Categorical(df['Categoria'], categories=orden_educativo, ordered=True)

   

    df['Total'] = pd.to_numeric(df['Total'], errors='coerce').fillna(0)

    df = df.groupby(['Periodo', 'Categoria'])['Total'].sum().reset_index()

    df = df.rename(columns={{'Total': 'n'}})

   

    # INSERTA AQUI EL BLOQUE GGPLOT (ASIGNALO A LA VARIABLE 'plot')

    plot = None

   

    return plot

"""



    # Hacemos especial énfasis en devolver la función COMPLETA

    system = (

        "Eres un experto en Plotnine. "

        "Tu tarea es tomar el template de código proporcionado y completarlo. "

        "IMPORTANTE: Debes devolver la función COMPLETA. Empieza con `def generar_plot_social(df):`, "

        "copia toda la lógica de preparación de datos intacta y sustituye 'plot = None' por el bloque de código ggplot. "

        "Devuelve EXCLUSIVAMENTE código Python válido, sin markdown ni texto extra."

    )

   

    descripcion = f"""Template base:

{template_con_datos}



Instrucción:

Reemplaza la línea 'plot = None' con el siguiente bloque exacto de ggplot, manteniendo el resto de la función intacta:



    plot = (

        ggplot(df, aes(x='Periodo', y='n', fill='Categoria'))

        + geom_area(position='fill', alpha=0.85, color='white')

        + scale_fill_brewer(type='qual', palette='Set2')

        + scale_x_continuous(breaks=list(range(2019, 2026, 2)))

        + labs(title='Distribución del Nivel de Estudios — {territorio}',

               subtitle='Fuente: ISTAC · Encuesta de Nivel y Condiciones de Vida',

               x='Año', y='Proporción', fill='Nivel educativo')

        + theme_minimal()

        + theme(figure_size=(12, 5), legend_position='right')

    )

"""

    context.log.info(f"Template social · territorio='{territorio}'")

    return {

        "model": IA_MODEL, "temperature": 0.1, "stream": False,

        "messages": [

            {"role": "system", "content": system},

            {"role": "user",   "content": descripcion},

        ],

    }

@asset

def codigo_generado_ia_renta(

    context: OpExecutionContext,

    template_ia_renta: dict,

) -> Output:

    """Llama al LLM con el template de renta, limpia y valida el código devuelto."""

    codigo = _llamar_ia(template_ia_renta, context)

    _validar_codigo(codigo, "generar_plot")

    context.log.info("Código renta validado.")

    return Output(

        value=codigo,

        metadata={

            "longitud":            MetadataValue.int(len(codigo)),

            "tiene_ggplot":        MetadataValue.bool("ggplot" in codigo),

            "tiene_geom_line":     MetadataValue.bool("geom_line" in codigo),

            "tiene_scale_color":   MetadataValue.bool("scale_color" in codigo),

            "tiene_es_focal":      MetadataValue.bool("es_focal" in codigo),

            "modelo":              MetadataValue.text(IA_MODEL),

            "codigo":              MetadataValue.md(f"```python\n{codigo}\n```"),

        },

    )





@asset

def codigo_generado_ia_social(

    context: OpExecutionContext,

    template_ia_social: dict,

) -> Output:

    """Llama al LLM con el template social, limpia y valida el código devuelto."""

    codigo = _llamar_ia(template_ia_social, context)

    _validar_codigo(codigo, "generar_plot_social")

    context.log.info("Código social validado.")

    return Output(

        value=codigo,

        metadata={

            "longitud":            MetadataValue.int(len(codigo)),

            "tiene_ggplot":        MetadataValue.bool("ggplot" in codigo),

            "tiene_geom_area":     MetadataValue.bool("geom_area" in codigo),

            "tiene_scale_fill":    MetadataValue.bool("scale_fill" in codigo),

            "tiene_brewer":        MetadataValue.bool("brewer" in codigo.lower()),

            "modelo":              MetadataValue.text(IA_MODEL),

            "codigo":              MetadataValue.md(f"```python\n{codigo}\n```"),

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

    Ejecuta los dos códigos generados por la IA y guarda los PNG en DIR_GRAFICOS.



      visualizacion_ia_renta_<territorio>.png

      visualizacion_ia_social_<territorio>.png

    """

    warnings.filterwarnings("ignore")

    os.makedirs(DIR_GRAFICOS, exist_ok=True)

    territorio = Dashboard.TERRITORIO.lower().replace(" ", "_")

    rutas = []



    # Gráfico 1 — renta por municipio

    context.log.info("Ejecutando código renta generado por IA...")

    g_renta   = _ejecutar_codigo(

        codigo_generado_ia_renta, integrar_renta_codislas,

        "generar_plot", context,

    )

    ruta_renta = os.path.join(DIR_GRAFICOS, f"visualizacion_ia_renta_{territorio}.png")

    g_renta.save(ruta_renta, width=12, height=7, dpi=150)

    context.log.info(f"Guardado: {ruta_renta}")

    rutas.append(ruta_renta)



    # Gráfico 2 — nivel de estudios

    context.log.info("Ejecutando código social generado por IA...")

    g_social  = _ejecutar_codigo(

        codigo_generado_ia_social, enriquecer_nivelestudios,

        "generar_plot_social", context,

    )

    ruta_social = os.path.join(DIR_GRAFICOS, f"visualizacion_ia_social_{territorio}.png")

    g_social.save(ruta_social, width=12, height=7, dpi=150)

    context.log.info(f"Guardado: {ruta_social}")

    rutas.append(ruta_social)



    sizes = {os.path.basename(r): round(os.path.getsize(r) / 1024, 1) for r in rutas}

    return Output(

        value=rutas,

        metadata={

            "rutas":      MetadataValue.text(str(rutas)),

            "sizes_kb":   MetadataValue.text(str(sizes)),

            "territorio": MetadataValue.text(Dashboard.TERRITORIO),

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