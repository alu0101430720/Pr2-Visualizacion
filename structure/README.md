# Pr2-Visualizacion · Pipeline Dagster

## Estructura del proyecto

```
Pr2-Visualizacion/
│
├── config.py                 # Constantes globales (rutas, ramas, umbrales Gestalt)
├── definitions.py            # Entrypoint de Dagster: Registro de assets, jobs, checks y sensores
│
├── pr2_assets/               # Lógica de orquestación y transformación (Assets Dagster)
│   ├── __init__.py
│   ├── git_ops.py            # Clonado, configuración y pull del repositorio de datos
│   ├── renta.py              # Ingesta y limpieza del dataset de renta
│   ├── codislas.py           # Ingesta de catálogo territorial y cruce (merge) con renta
│   ├── nivelestudios.py      # Tratamiento del dataset de educación y generación de gráficos base
│   ├── mapas.py              # Extracción TopoJSON, mapas de coropletas (renta y paro)
│   ├── ia_viz.py             # Generación dinámica de código Python vía LLM y ejecución segura
│   └── checks.py             # Batería centralizada de validaciones de calidad (Asset Checks)
│
├── charts/                   # Funciones puras de visualización (Plotnine/ggplot)
│   ├── renta_charts.py       # Gráficos de evolución de renta
│   └── social_charts.py      # Gráficos de distribución de nivel de estudios
│
└── utils/
    └── git.py                # Wrappers para automatizar git commit & push
```

> NOTE: Para hacer uso de la lógica del flujo, desde el repositorio hasta los parámetros para graficar, debe usarse el fichero config.py

## DAG de assets

```
github_token ──┬──► clone_repository ──► configure_git ──► pull_repository
               │                                           │
               │         ┌─────────────────────────────────┴──────────┬────────────────────────┐
               │         │                                            │                        │
               │    ingestar_renta                           ingestar_codislas                 │
               │         │                                            │                        │
               │    limpiar_renta                            limpiar_codislas                  │
               │         │       \                          /         │                        │
               │    guardar_renta  integrar_renta_codislas  ──┐  guardar_codislas              │
               │    _limpia         /            \            │       _limpia                  │
               │                   /              \           │                                │
               │    guardar_renta_integrada        \          └──► extraer_indicadores         │
               │             │                      \                 _istac (JSON)            │
               │             │                       \                       │                 │
               │      mapa_rentas_python       ingestar_nivelestudios  mapa_paro               │
               │             │                       │                 _municipios        prompt_ia
               │             │                 limpiar_nivelestudios         │                 │
               │             │                       │                       │        generar_codigo_ia
               │             │                 enriquecer_nivelestudios      │                 │
               │             │                      /        \               │        visualizacion_ia
               │             │       guardar_nivelestudios  generar_graficos │                 │
               │             │            _limpio            _ejercicio3     │                 │
               │             │                      \        /               │                 │
               └──►   commit_mapa_python       commit_ejercicio3      commit_mapa_paro   commit_ia
```

## DAG checks
| Etapa | Nombre del Check | Descripción Técnica | Cómo programarlo (Lógica) | Relación con el Diseño / Gestalt | Información para Metadata |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Carga (Raw)** | `check_nulos_criticos_renta` | Detecta ausencia de datos en variables críticas (Territorio, Tiempo, Medidas y Valores). | `df[cols].isna().any(axis=1).sum() == 0` | Figura y Fondo: Los huecos inesperados rompen la forma de la visualización. | `porcentaje_filas_incompletas`, `filas_afectadas`, `detalle_por_columna` |
| **Carga (Raw)** | `check_nulos_criticos_codislas` | Detecta nulos en ISLA y NOMBRE del catálogo de territorios. | `df[['ISLA', 'NOMBRE']].isna().sum() == 0` | Figura y Fondo: Los huecos inesperados rompen la forma de la visualización. | `nulos_ISLA`, `nulos_NOMBRE` |
| **Carga (Raw)** | `check_nulos_criticos_nivelestudios` | Detecta nulos en las columnas clave: Periodo, Sexo, Total. | `df[['Periodo', 'Sexo', 'Total']].isna().sum() == 0` | Figura y Fondo: Los huecos inesperados rompen la forma de la visualización. | `nulos_Periodo`, `nulos_Sexo`, `nulos_Total` |
| **Transf. (Curated)** | `check_duplicados_limpiar_renta` | Verifica que la limpieza no introdujo ni mantuvo filas duplicadas en la renta. | `df.duplicated().sum() == 0` | Figura y Fondo: Los duplicados inflan artificialmente las series. | `filas_duplicadas` |
| **Transf. (Curated)** | `check_formato_title_limpiar_renta` | Verifica que "Territorio" está en formato Title Case. | `df.str.strip() == df.str.strip().str.title()` | Similitud: Evita que una misma categoría se pinte con dos colores distintos al tener distintas mayúsculas. | `n_incorrectos`, `ejemplos` |
| **Transf. (Curated)** | `check_nombre_invertido_limpiar_renta` | Detecta valores con formato 'Apellido, Artículo' (ej. "Gomera, La"). | `~df.str.contains(",", na=False)` | Similitud: Evita que una misma categoría se pinte con dos colores distintos. | `n_invertidos`, `ejemplos` |
| **Transf. (Curated)** | `check_cardinalidad_fuente_renta` | Limita el número de fuentes de renta graficadas. | `df['Fuente_Renta_Code'].nunique() <= MAX_CATEGORIAS` | Carga Cognitiva / Similitud: Más de 9 colores son imposibles de distinguir. | `n_categorias`, `limite_recomendado` |
| **Transf. (Curated)** | `check_continuidad_serie_temporal_renta` | Verifica que no falten años intercalados en la serie temporal. | `max(df.Año) - min(df.Año) + 1 == len(df.Año.unique())` | Continuidad: Evita que una línea una puntos lejanos creando una pendiente falsa. | `fechas_faltantes`, `rango_temporal` |
| **Transf. (Curated)** | `check_label_text_territorio` | Detecta etiquetas de territorio demasiado largas para los ejes. | `df['Territorio'].str.len().max() <= MAX_LABEL` | Continuidad: Etiquetas largas se solapan y rompen la legibilidad. | `longest_label`, `longitud_max`, `overlap_risk` |
| **Transf. (Curated)** | `check_cardinalidad_islas` | Verifica que el número de islas únicas no supere el límite de colores. | `df['ISLA_clean'].nunique() <= MAX_CATEGORIAS` | Carga Cognitiva / Similitud: Más de 9 colores son imposibles de distinguir. | `n_categorias` |
| **Transf. (Curated)** | `check_formato_title_limpiar_codislas` | Verifica que ISLA_clean y Territorio estén en Title Case. | `_get_title_case_errors() == []` | Similitud: Evita que una misma categoría se pinte con dos colores distintos al tener distintas mayúsculas. | `ejemplos_ISLA_clean`, `ejemplos_Territorio` |
| **Transf. (Curated)** | `check_nombre_invertido_limpiar_codislas` | Verifica que ISLA_clean y Territorio no contienen formato inverso por coma. | `_get_inverted_name_errors() == []` | Similitud: Evita que una misma categoría se pinte con dos colores distintos. | `ejemplos_ISLA_clean`, `ejemplos_Territorio` |
| **Transf. (Curated)** | `check_duplicados_limpiar_codislas` | Verifica que no haya municipios duplicados en el catálogo. | `df.duplicated().sum() == 0` | Figura y Fondo: Un municipio duplicado aparecería dos veces en el facet del gráfico. | `filas_duplicadas`, `ejemplos` |
| **Transf. (Curated)** | ~~`check_integridad_join_renta_codislas`~~ *(comentado)* | Verifica que todos los municipios cruzados tengan una isla asignada. | `df_municipios['ISLA'].isna().sum() == 0` | Figura y Fondo: Municipios sin isla asignada no aparecerán en el facet correcto. | `municipios_huerfanos`, `ejemplos_huerfanos`, `excluidos_del_check` |
| **Transf. (Curated)** | `check_continuidad_serie_temporal_nivelestudios` | Verifica que no haya saltos de años en el dataset de nivel de estudios. | `max(df.Año) - min(df.Año) + 1 == len(df.Año.unique())` | Continuidad: Evita que un área una puntos lejanos creando un volumen visual engañoso. | `fechas_faltantes` |
| **Transf. (Curated)** | `check_cardinalidad_nivel_estudios` | Limita los distintos niveles de estudios a un máximo de colores. | `df['Nivel_estudios'].nunique() <= MAX_CAT` | Carga Cognitiva / Similitud: Más de 9 colores son imposibles de distinguir. | `n_categorias`, `sugerencia_agrupacion` |
| **Transf. (Curated)** | `check_dominance_otros_nivelestudios` | Verifica que el grupo "Sin Estudios/Otros" no domine la visualización. | `pct_otros <= DOMINANCE_OTROS_MAX` | Semejanza: Un grupo 'Otros' dominante atrae la atención lejos de los datos relevantes. | `pct_of_total`, `umbral_maximo` |
| **Transf. (Curated)** | `check_label_text_municipio` | Detecta municipios con nombres largos, en específico para heatmaps (eje Y). | `df['Municipio'].str.len().max() <= MAX_LABEL` | Continuidad: Etiquetas largas se solapan en el eje Y rompiendo la legibilidad. | `longitud_max`, `overlap_risk` |
| **Visualiz. (Asset)** | `check_datos_dashboard_no_vacios` | Verifica que los datos filtrados por la configuración del dashboard existan. | `len(_filtrar_datos_dashboard(df)) > 0` | Figura y Fondo: Un gráfico sin datos no tiene figura que mostrar. | `filas_en_grafico`, `territorios_en_grafico`, `dashboard_activo` |
| **Visualiz. (Asset)** | `check_escala_y_dashboard` | Detecta outliers que comprimen los demás datos en el gráfico. | `(max / min) <= RATIO_ESCALA_MAX` | Proporcionalidad: Evita que barras pequeñas parezcan invisibles ante una gigante. | `ratio_escala`, `valor_outlier`, `territorio_outlier` |
| **Visualiz. (Asset)** | `check_cardinalidad_dashboard` | Verifica el número de series en el subset final. | `df_subset[col].nunique() <= MAX_CATEGORIAS` | Carga Cognitiva / Similitud: Más de 9 colores son imposibles de distinguir. | `n_series`, `columna_series`, `dashboard_activo` |
| **Visualiz. (Asset)** | `check_orden_magnitud_dashboard` | Verifica que las series estén ordenadas descendentemente por valor. | `orden_actual == orden_optimo` | Continuidad / Prägnanz: El ojo sigue una línea suave (escalera) en lugar de saltos erráticos, reduciendo el esfuerzo cognitivo. | `is_sorted`, `sugerencia_orden`, `dashboard_activo` |
| **Visualiz. (Asset)** | `check_graficos_generados` | Verifica que los archivos `.png` se han guardado físicamente y no están vacíos. | `os.path.exists(f) and size > 10.0 KB` | Veracidad Visual: Un gráfico vacío o truncado transmite información falsa. | `graficos`, `dashboard_activo` |
| **Visualiz. (Asset)** | `check_mapa_png_valido` | Verifica que el PNG del mapa de rentas existe en disco y supera 30 KB. | `os.path.exists(f) and size > 30.0 KB` | Veracidad Visual: Un archivo pequeño indica un mapa vacío o sin polígonos coloreados. | `size_kb` |
| **Visualiz. (Asset)** | `check_integridad_istac` | Verifica que el CSV de indicadores ISTAC contiene los 88 municipios y que la suma paro\_t ≈ paro\_m + paro\_f. | `n_municipios == 88 and diff_paro < 20.0` | Veracidad Visual: Datos faltantes o incoherentes producen un mapa con huecos o cortes. | `municipios_detectados`, `desviacion_suma_paro`, `status_conteo`, `status_suma` |
| **Visualiz. (Asset)** | `check_rango_tasas` | Valida que las tasas de paro estén dentro del rango válido [0, 100]. | `df[(df['tpar_t'] < 0) \| (df['tpar_t'] > 100)]` len == 0 | Veracidad Visual: Escalas fuera de 0–100% confunden al usuario. | `municipios_con_error`, `valor_max_encontrado` |
| **Visualiz. (Asset)** | `check_mapa_generado_correctamente` | Verifica que el PNG del mapa de paro municipal existe y supera 20 KB. | `os.path.exists(f) and size > 20.0 KB` | Veracidad Visual: Un archivo pequeño indica que se grabó un lienzo en blanco. | `path`, `tamano_kb` |
| **Generación IA** | `check_template_no_vacio` | Verifica que el payload IA contiene los dos prompts (renta + estudios) con contenido real y al menos 3 capas de gramática de gráficos. | `len(mensajes) >= 2 and capas_presentes >= 3` | Figura y Fondo: Un prompt vacío produce un gráfico sin figura. | `payload_renta`, `payload_estudios` |
| **Generación IA** | `check_codigo_valido` | Verifica que los dos códigos generados son Python ejecutable con plotnine: definen la función, usan `ggplot`, `geom_*`, `return` y no contienen patrones peligrosos. | `compile(codigo) OK and not patrones_peligrosos` | Veracidad Visual: Código con errores de sintaxis produce un gráfico vacío. | `codigo_renta`, `codigo_estudios` |
| **Generación IA** | `check_gestalt_en_codigo` | Verifica que el código IA aplica principios Gestalt: `scale_color_manual` en renta y `scale_fill_brewer` en estudios, junto con `labs` y `theme_minimal`. | `all(elem in codigo for elem in elementos_requeridos)` | Punto Focal (renta) / Similitud (estudios): Sin `scale_*_manual` el gráfico usa colores automáticos sin criterio Gestalt. | `codigo_renta`, `codigo_estudios` |
| **Generación IA** | `check_png_ia_generado` | Verifica que los dos PNG generados por la IA existen en `DIR_GRAFICOS` y superan 10 KB. | `os.path.exists(f) and size > 10.0 KB` | Veracidad Visual: Un PNG vacío o ausente transmite información falsa. | `resultados`, `n_graficos`, `directorio` |
