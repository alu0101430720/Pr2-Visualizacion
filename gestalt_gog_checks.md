# Guía de Calidad Visual: Principios de Gestalt y Gramática de Gráficos en los Checks del Proyecto

Este documento describe detalladamente la base teórica y técnica de cada una de las comprobaciones de calidad de datos (`asset_checks`) implementadas en el módulo `checks_p5.py`. Cada comprobación está diseñada bajo la premisa de garantizar que los gráficos finales sean **visualmente veraces, consistentes e intuitivos**, respaldándose en las leyes de **Gestalt** y en los componentes de la **Gramática de Gráficos**.

---

## 1. Relación de Conceptos Teóricos

### A. Leyes de Gestalt (Percepción Visual)
*   **Figura/Fondo**: Capacidad del ojo para separar el foco de atención (figura) del espacio circundante (fondo). Un outlier extremo, un título solapado o un hueco inesperado (NaN) perturba esta separación.
*   **Similitud**: Tendencia a agrupar elementos visuales con características comunes (color, forma, tamaño). Mantener paletas de colores coherentes por sexo o isla y tipografías consistentes consolida esta ley.
*   **Proximidad**: Tendencia a agrupar elementos que están espacialmente cerca. En gráficos facetados o de barras agrupadas, la distancia entre elementos dicta su relación conceptual.
*   **Continuidad**: Preferencia del ojo por seguir una trayectoria suave. Un salto temporal faltante (e.g., saltar de 2020 a 2022 sin 2021) introduce pendientes falsas y fractura la trayectoria.
*   **Cierre**: El cerebro tiende a completar formas e información faltante. Un mapa con secciones vacías debido a joins fallidos o un municipio ausente produce una sensación de "lienzo roto".
*   **Proporcionalidad y Veracidad**: Relación directa entre la magnitud física del canal visual (e.g., longitud de barra, área) y la magnitud matemática de los datos para evitar distorsiones cognitivas.

### B. Gramática de Gráficos (GoG - Grammar of Graphics)
*   **Datos (Data)**: La estructura de la tabla fuente (columnas, tipos, consistencia tipográfica).
*   **Mapeo Estético (Aesthetics - aes)**: Vinculación de variables a canales visuales (posición X/Y, color, relleno, tamaño).
*   **Geometrías (Geoms)**: La representación física del dato (`geom_bar`, `geom_line`, `geom_point`, `geom_sf`).
*   **Estadísticas (Stats)**: Transformaciones matemáticas sobre el dato antes de graficar (sumas, medias, proporciones agregadas).
*   **Escalas (Scales)**: Mapeo del dominio matemático al rango visual (`scale_x_continuous`, `scale_fill_manual`, `TwoSlopeNorm`).
*   **Facetas (Facets)**: Small multiples o subgráficos basados en variables categóricas (`facet_wrap`).
*   **Tema (Theme)**: Elementos no-datos del gráfico (títulos, rejilla, leyendas, márgenes, envoltura de textos).

---

## 2. Catálogo de Checks y su Base de Diseño

### 1. Ingesta y Limpieza de Datos

#### `check_ausencia_nulos`
*   **Gestalt**: *Figura/Fondo*. Los vacíos inesperados (NaN) en variables continuas o categóricas rompen la forma de la distribución, atrayendo la atención del usuario hacia el vacío (ruido) en lugar de hacia la figura (el dato).
*   **Gramática de Gráficos**: *Mapeo Estético (aes)*. El mapeo de nulos en canales de posición (X/Y) o color altera los límites del dominio y ensucia la escala del gráfico.

#### `check_duplicados`
*   **Gestalt**: *Similitud*. Los registros duplicados inflan de forma artificial el peso visual de geometrías específicas, distorsionando la relación de proporción respecto a categorías similares.
*   **Gramática de Gráficos**: *Estadística (Stats)*. El cálculo estadístico interno (`stat="identity"` o sumatorias) computa valores inflados, alterando la altura visual del `geom`.

#### `check_formato_nombres_municipio`
*   **Gestalt**: *Similitud*. Diferencias en capitalización o espaciado (e.g., "puerto de la cruz" vs "Puerto de La Cruz") hacen que el usuario perciba visualmente entidades duplicadas e inconexas.
*   **Gramática de Gráficos**: *Datos (Data) / Escalas*. Evita la proliferación de categorías duplicadas en el dominio discreto de los ejes de posición.

#### `check_consistencia_municipios_cruzados`
*   **Gestalt**: *Similitud*. Asegura la consistencia en la rotulación tipográfica de los municipios a lo largo de todos los gráficos del proyecto.
*   **Gramática de Gráficos**: *Datos (Data)*. Garantiza joins seguros entre conjuntos de datos diversos a nivel de clave primaria.

---

### 2. Cobertura Temporal, Territorial y Estructura

#### `check_temporal_y_sexo`
*   **Gestalt**: *Continuidad y Similitud*. La falta de años intermedios rompe la trayectoria temporal; valores de género espúreos fracturan la paleta cromática unificada por sexo.
*   **Gramática de Gráficos**: *Escalas (Scales)*. Garantiza que la escala temporal en el eje X y la escala discreta de color manual (`scale_fill_manual`) permanezcan estables y predecibles.

#### `check_conteo_municipios`
*   **Gestalt**: *Cierre*. Si falta algún municipio de la isla analizada, se rompe la percepción mental del territorio geográfico completo (el todo).
*   **Gramática de Gráficos**: *Datos (Data) / Escalas*. Asegura un dominio de escala categórica completo para los ejes o mapas.

#### `check_tsv_procesados` & `check_tipo_territorio`
*   **Gestalt**: *Cierre*. Asegura que la transformación y clasificación de los ámbitos geográficos esté disponible en su totalidad.
*   **Gramática de Gráficos**: *Datos (Data)*. Prepara segmentaciones de datos coherentes para mapeos eficientes a nivel de facetas (`facets`) o subconjuntos.

#### `check_dedup_santa_cruz`
*   **Gestalt**: *Veracidad Visual*. Impide duplicados invisibles que se solapan en la misma coordenada geográfica del mapa.
*   **Gramática de Gráficos**: *Estadísticas (Stats)*. Previene sumas duplicadas erróneas en el cálculo estadístico previo al mapeo.

#### `check_cobertura_gini` & `check_cobertura_rentas`
*   **Gestalt**: *Continuidad / Cierre*. Salvaguarda la totalidad de registros históricos para evitar gráficas de líneas inconexas o rotas.
*   **Gramática de Gráficos**: *Datos (Data) / Geometrías*. Asegura la densidad suficiente de puntos en la capa geométrica (`geom_line` o `geom_point`).

#### `check_consistencia_municipios_gini` & `check_suficientes_anios_historico`
*   **Gestalt**: *Continuidad*. Valida que la serie temporal cuente con un horizonte suficiente (e.g., ≥ 9 años) para dibujar trayectorias creíbles de largo plazo.
*   **Gramática de Gráficos**: *Escalas / Facetas*. Asegura un dominio temporal lo suficientemente amplio y consistente para comparativas facetadas.

---

### 3. Escalas, Contraste y Limpieza del Canvas

#### `check_rangos_valores`
*   **Gestalt**: *Figura/Fondo*. Valores absurdos u outliers colapsan la escala de color o el eje del gráfico, enviando el resto de datos representativos a un fondo homogéneo ilegible.
*   **Gramática de Gráficos**: *Límites de Escala (Scales Limits)*. Asegura que el dominio matemático de entrada se adecúe perfectamente al rango visual establecido.

#### `check_suma_componentes_distribucion`
*   **Gestalt**: *Similitud / Cierre*. Garantiza que las partes de una distribución (e.g., deciles de renta) conformen armónicamente el 100% de la barra apilada.
*   **Gramática de Gráficos**: *Estadística (Stats)*. El cálculo de acumulación del gráfico de barras porcentual debe cerrar exactamente a 1.0.

#### `check_cobertura_join_geojson`
*   **Gestalt**: *Cierre y Figura/Fondo*. Si un join espacial falla, el municipio aparece vacío en el mapa (color gris o blanco de fondo), rompiendo la continuidad visual y dando aspecto de gráfico incompleto.
*   **Gramática de Gráficos**: *Geometría (Geoms)*. Asegura el acoplamiento óptimo de la capa geoespacial (`geom_sf`) con los datos tabulares.

#### `check_cardinalidad_categorias`
*   **Gestalt**: *Similitud / Carga cognitiva*. Un gráfico con más de 9 categorías nominales rompe la ley de similitud cromática: el cerebro es incapaz de discriminar con rapidez más de 9 colores diferentes.
*   **Gramática de Gráficos**: *Escala de Color*. Restringe el número máximo de niveles permitidos en la codificación del canal de color nominal.

#### `check_longitud_etiquetas`
*   **Gestalt**: *Continuidad / Proximidad*. Nombres o etiquetas excesivamente largas colisionan, se solapan y rompen el flujo natural de lectura de los ejes categóricos.
*   **Gramática de Gráficos**: *Tema (Theme)*. Controla los textos de los ejes (`theme(axis_text_y)`) favoreciendo la abreviación o envoltura (`textwrap`).

#### `check_dominancia_componente`
*   **Gestalt**: *Figura/Fondo*. Si un componente supera el 80% del total, ejerce una dominancia visual extrema. Esto requiere un ajuste del contraste o una escala divergente equilibrada.
*   **Gramática de Gráficos**: *Escalas (Scales)*. Calibra los centros de normalización divergente (e.g., `mcolors.TwoSlopeNorm` centrado en 0.5).

#### `check_escala_outliers`
*   **Gestalt**: *Proporcionalidad*. Valida que las diferencias de ingresos o rentas extremas no colapsen la escala visual, recurriendo de ser necesario a transformaciones (e.g. logarítmicas) o acotamientos.
*   **Gramática de Gráficos**: *Límites y Transformaciones de Escala*. Ajuste dinámico de los límites (`xlim`/`ylim`) y ticks.

#### `check_ratio_hm_estabilidad` & `check_balance_sexos_por_municipio`
*   **Gestalt**: *Similitud / Veracidad*. Previene anomalías visuales en municipios con datos sesgados o de un solo sexo que distorsionen los ratios de brecha y rompan la paleta manual simétrica.
*   **Gramática de Gráficos**: *Escalas / Mapeo*. Protege la coherencia del canal de color y relleno mapeado al sexo.

---

### 4. Calidad del Gráfico Físico y Layout

#### `check_output_plots`
*   **Gestalt**: *Veracidad Visual*. Verifica la existencia de cada PNG en disco y su peso mínimo (≥50 KB) para descartar visualizaciones vacías ("lienzos en blanco").
*   **Gramática de Gráficos**: *Tema (Theme) / Salida*. Valida el correcto renderizado de todas las capas estéticas al archivo físico final.

#### `check_longitud_titulos_plots`
*   **Gestalt**: *Figura/Fondo*. Un título excesivamente largo y sin saltos de línea se sale de la figura o se solapa con el gráfico principal, rompiendo la composición.
*   **Gramática de Gráficos**: *Tema (Theme)*. Verifica la propiedad `title` del tema estético para forzar envoltura si excede el límite razonable de caracteres.

#### `check_min_filas_por_panel`
*   **Gestalt**: *Proximidad*. Un panel vacío en una cuadrícula facetada se percibe cognitivamente como un error o una ausencia de datos, rompiendo la simetría de la grilla.
*   **Gramática de Gráficos**: *Facetas (Facets)*. Asegura que cada faceta generada en `facet_wrap` contenga suficientes datos para ser dibujada.

#### `check_coherencia_paleta_config` & `check_ano_configurable_existe`
*   **Gestalt**: *Similitud*. Asegura la consistencia global de la identidad cromática y del eje temporal en base al YAML central de configuraciones.
*   **Gramática de Gráficos**: *Escalas / Tema*. Valida que las escalas del gráfico lean colores armoniosos e integrados.

---

### 5. Datos Específicos de Visualización

#### `check_datos_actividad_barras`
*   **Gestalt**: *Similitud*. Valida la existencia estricta de "Hombres" y "Mujeres" en los datos para evitar que la escala de color manual binaria falle y asigne colores aleatorios rotos.
*   **Gramática de Gráficos**: *Escala de Color / Relleno*. Consistencia en el canal estético de `fill`.

#### `check_datos_brecha_salarial` & `check_datos_mapa_brecha`
*   **Gestalt**: *Cierre y Figura/Fondo*. Exigen que existan valores tanto positivos como negativos en las brechas para garantizar que las geometrías de desviación y las escalas divergentes de dos tonos tengan sentido narrativo (rango completo visible).
*   **Gramática de Gráficos**: *Escalas / Geometrías*. Mapeo a escalas divergentes y geometrías de lollipop/mapa.

#### `check_datos_gini_evolucion`
*   **Gestalt**: *Continuidad*. Impide la generación de líneas de tendencia quebradas o discontinuas en la evolución temporal de desigualdad.
*   **Gramática de Gráficos**: *Geometría (Geom)*. Asegura un `geom_line` continuo en el tiempo para cada isla.

#### `check_datos_historico_contratos`
*   **Gestalt**: *Continuidad / Proximidad*. Verifica la integridad del mosaico de gráficos 2x2. Necesita al menos 4 series de datos completas para evitar paneles vacíos e inconexos.
*   **Gramática de Gráficos**: *Facetas (Facets)*. Valida el correcto cuadre y balance de los ejes y variables facetadas.

#### `check_datos_segregacion_sectorial` (Cleveland Dot Plot)
*   **Gestalt**:
    *   *Similitud*: Colores consistentes y unificados por isla a través de la visualización.
    *   *Proximidad*: Puntos de la misma actividad económica contenidos exactamente en el mismo panel horizontal guiando la lectura.
    *   *Cierre*: Exclusión de celdas con menos de 30 contratos para evitar ratios de segregación inestables que desvirtúen la escala visual.
*   **Gramática de Gráficos**: *Canal de Posición X*. Emplea el canal visual más preciso para comparaciones de proporciones (posición en eje común, Cleveland 1984) mapeando el porcentaje de hombres y controlando la dispersión mediante geometrías lineales (`geom_segment` o similar).

#### `check_datos_covid_sueldos` & `check_datos_covid_prestaciones`
*   **Gestalt**: *Continuidad y Figura/Fondo*. Verifican la presencia de los años críticos de la pandemia (2020) y la validez de los sueldos en esos periodos para que las áreas de sombreado especial en color gris (anotaciones de fondo) encajen con la caída del dato (figura en primer plano).
*   **Gramática de Gráficos**: *Tema / Capas Anotadas*. Validación de rectángulos de fondo (`geom_rect` o similar) y anotaciones de texto.

#### `check_datos_brecha_temporal_edad`
*   **Gestalt**: *Proximidad*. Valida que las variables de sexo y grupo de edad estén completas para que la agrupación de barras H/M una al lado de la otra sea regular y coherente espacialmente.
*   **Gramática de Gráficos**: *Geometría (Geom)*. Asegura un `geom_col(position="dodge")` simétrico y uniforme.

#### `check_ocupacion_divergente_canarias`
*   **Gestalt**: *Figura/Fondo*. Garantiza que el gráfico divergente del CNO-1 canario cuente con el ámbito geográfico y las variables necesarias para que el eje central de paridad de género actúe como ancla de fondo equilibrada.
*   **Gramática de Gráficos**: *Coordenadas Invertidas (`coord_flip`)*. Valida el correcto despliegue de las barras de desviación a ambos lados del cero.
