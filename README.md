# Proyecto Final — Visualización de la Estructura Laboral y Salarial de Canarias

Este repositorio contiene la implementación del pipeline de ingesta, preprocesamiento, control de calidad y generación de visualizaciones de alto impacto para analizar la estructura laboral, salarial y la brecha de género en la Comunidad Autónoma de Canarias.

* **Alumno**: Carlos Yanes Pérez
* **Tecnologías**: Dagster (Pipeline & Quality Orchestration), Pandas, Matplotlib, Plotnine (ggplot), Geopandas.
* **Enfoque de Diseño**: Justificación rigurosa en base a las **Leyes de la Gestalt** y los fundamentos de la **Gramática de Gráficos (GoG)**.

---

## 🚀 Cómo Ejecutar el Proyecto

### 1. Requisitos e Instalación
Asegúrate de contar con Python 3.10+ y el entorno virtual activo. Para instalar dependencias (en la carpeta raíz del proyecto):
```bash
pip install -r requirements.txt
```

### 2. Ingesta, Preprocesamiento y Generación de Visualizaciones (Dagster)
Para materializar el pipeline completo o assets específicos, sitúate en `assets_proyecto5` y ejecuta el comando de Dagster:

```bash
# Materializar todo el pipeline (Ingesta -> Preprocesado -> Plots -> Publicación en GitHub)
..\..\.venv\Scripts\dagster asset materialize --select * -m definitions

# Materializar únicamente el nuevo gráfico de tendencia temporal de segregación
..\..\.venv\Scripts\dagster asset materialize --select plot_segregacion_sectorial_temporal -m definitions
```

### 3. Ejecución de la Suite de Controles de Calidad (Checks)
Los checks de calidad verifican la coherencia lógica de los datos, la estabilidad estadística y la veracidad de la composición visual antes de confirmar los gráficos:
```bash
# Ejecutar todos los checks asociados al preprocesamiento y plots
..\..\.venv\Scripts\dagster asset check run --select * -m definitions
```

---

## 📊 Portafolio Visual y Catálogo de Gráficos

El pipeline genera automáticamente las visualizaciones en formato de alta resolución (150 DPI) dentro de la carpeta `data-P5/plots/`:

### 1. Estructura de Contratos por Ocupación y Actividad
*   **`ocupacion_divergente_canarias.png`**: Estructura divergente del CNO-1 en el ámbito configurado (Canarias/islas). Muestra la brecha (H − M) respecto al total normalizado para evaluar visualmente la asimetría por ocupación (Gestalt: *Figura/Fondo*).
*   **`actividad_barras.png`**: Gráfico de barras apiladas al 100% que ilustra el porcentaje de contratos por sexo y sector a lo largo de los periodos.
*   **`brecha_temporal_parcial_edad.png`**: Distribución de tipologías de contrato (Indefinido, Temporal Completo, Temporal Parcial y Conversión) cruzados por franja de edad y sexo.

### 2. Segregación Sectorial de Género
*   **`segregacion_sectorial_dotplot.png`**: Cleveland Dotplot del ratio de feminización en las principales actividades económicas. Soporta configuración a nivel regional (7 islas) o municipal para una isla concreta (ej. municipios de Tenerife con color canónico verde `#1B9E77` y jitter determinista).
*   **`segregacion_sectorial_temporal.png`** (🆕): Serie temporal del ratio $H/(H+M)$ por sector de actividad en Canarias (2019-2026), dividida por una línea de referencia de la Reforma Laboral de 2022 y áreas de dominancia teñidas en azul (masculina) y rosa (femenina).

### 3. Evolución Histórica de Tipos de Contrato (Small Multiples 2×2)
*   **`historico_indefinido.png`**
*   **`historico_temp_completo.png`**
*   **`historico_temp_parcial.png`**
*   **`historico_conversion.png`**
    *Permiten estudiar el impacto del marco regulatorio de 2022 y la progresiva estabilización contractual en las diferentes franjas de edad.*

### 4. Desigualdad y Análisis de Impacto COVID en Islas Turísticas
*   **`gini_evolucion_islas.png`**: Evolución histórica del Índice de Gini y distribución P80/P20 de renta por isla de 2015 a 2023.
*   **`covid_sueldos_islas.png`**: Caída y recuperación de la masa de sueldos y salarios en islas turísticas (Tenerife, Lanzarote y Fuerteventura) con sombreado del área COVID (2020-2021).
*   **`covid_prestaciones_islas.png`**: Incremento masivo de prestaciones por desempleo durante la pandemia en el sector turístico.

### 5. Análisis Espacial e Índices Ponderados
*   **`mapa_brecha_salarial.png`**: Mapa coroplético interactivo de la provincia con escala divergente ponderada por volumen de sueldos e indexada con nombres de municipios canónicos.
*   **`brecha_salarial_lollipop.png`**: Lollipop de deltas en la brecha salarial municipal para el periodo seleccionado.

---

## 🛠️ Controles de Calidad Integrados (`asset_checks`)

La suite automatizada incluye comprobaciones rigurosas divididas en 6 grandes bloques:
1.  **Limpieza de Ingesta**: `check_ausencia_nulos`, `check_duplicados`, `check_formato_nombres_municipio`.
2.  **Consistencia de Cruces**: `check_cobertura_join_geojson`, `check_consistencia_municipios_cruzados`, `check_consistencia_municipios_gini`.
3.  **Integridad Temporal**: `check_temporal_y_sexo`, `check_suficientes_anios_historico`, `check_cobertura_gini`, `check_cobertura_rentas`.
4.  **Estabilidad Estadística**: `check_ratio_hm_estabilidad` (evita volatilidad en n pequeños), `check_min_filas_por_panel` (evita paneles de facetas vacíos), `check_datos_segregacion_sectorial_temporal` (🆕 valida suficiencia temporal y de datos de género).
5.  **Composición Visual y Tema**: `check_cardinalidad_categorias` (evita más de 9 colores), `check_longitud_etiquetas` (evita solapes en texto), `check_longitud_titulos_plots`, `check_coherencia_paleta_config`.
6.  **Verificación de Entrega**: `check_output_plots` (valida la existencia, frescura y tamaño de todos los gráficos PNG generados).
