"""
config.py — Constantes y configuración compartida del proyecto Pr2-Visualizacion.

Para cambiar qué gráfico se genera y se valida, edita ÚNICAMENTE la sección
DASHBOARD. Los asset checks de visualización leerán automáticamente estos valores,
por lo que el check siempre validará el gráfico que se está generando en ese momento.
"""

# ── Repositorio Git ────────────────────────────────────────────────────────────
REPO_DIR   = "C:/Users/carlo/OneDrive/Escritorio/Pr4-Visualizacion/Pr2-Visualizacion"
REPO_OWNER = "alu0101430720"
REPO_NAME  = "Pr2-Visualizacion"
GIT_BRANCH = "practica3_refactorizado"
GIT_EMAIL  = "alu0101430720@ull.edu.es"
GIT_NAME   = "Carlos Yanes"

def repo_url(token: str) -> str:
    return f"https://{token}@github.com/{REPO_OWNER}/{REPO_NAME}.git"

# ── Rutas de datasets ──────────────────────────────────────────────────────────
RAW_RENTA         = f"{REPO_DIR}/distribucion-renta-canarias.csv"
RAW_CODISLAS      = f"{REPO_DIR}/codislas.csv"
RAW_NIVELESTUDIOS = f"{REPO_DIR}/nivelestudios.xlsx"
# RAW_RENTA         = f"{REPO_DIR}/datasets-check/distribucion-renta-canarias-checks.csv"
# RAW_CODISLAS      = f"{REPO_DIR}/datasets-check/codislas-checks.csv"
# RAW_NIVELESTUDIOS = f"{REPO_DIR}/datasets-check/nivelestudios-checks.xlsx"

DIR_CLEAN   = f"{REPO_DIR}/datasets-clean"
DIR_GRAFICOS = f"{REPO_DIR}/graficos"


# ── Fuentes de renta ───────────────────────────────────────────────────────────
class Fuentes:
    SALARIOS           = "SUELDOS_SALARIOS"
    PENSIONES          = "PENSIONES"
    DESEMPLEO          = "PRESTACIONES_DESEMPLEO"
    OTROS_INGRESOS     = "OTROS_INGRESOS"
    OTRAS_PRESTACIONES = "OTRAS_PRESTACIONES"
    TODOS = [SALARIOS, PENSIONES, DESEMPLEO, OTROS_INGRESOS, OTRAS_PRESTACIONES]


# ── Geografía ──────────────────────────────────────────────────────────────────
ISLAS_LAS_PALMAS = ["Gran Canaria", "Lanzarote", "Fuerteventura"]
ISLAS_SCT        = ["Tenerife", "La Gomera", "La Palma", "El Hierro"]
TODAS_ISLAS      = ISLAS_LAS_PALMAS + ISLAS_SCT


# ── Mapas de categorías ────────────────────────────────────────────────────────
MAPA_EDUCACION = {
    "Educación primaria e inferior": "Básicos",
    "Primera etapa de Educación Secundaria y similar": "Básicos",
    (
        "Segunda etapa de Educación Secundaria, con orientación profesional "
        "(con y sin continuidad en la educación superior); "
        "Educación postsecundaria no superior"
    ): "Medios",
    "Segunda etapa de educación secundaria, con orientación general": "Medios",
    "Educación superior": "Superiores",
    "No cursa estudios": "Sin Estudios/Otros",
    "Cursa estudios pero no hay información sobre los mismos": "Sin Estudios/Otros",
}


# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD — Parámetros del gráfico ejercicio 3
#
# Edita aquí para cambiar el gráfico que se genera Y se valida.
# Los asset checks de visualización leen estos valores automáticamente,
# garantizando que siempre validan exactamente el gráfico producido.
# ══════════════════════════════════════════════════════════════════════════════

class Dashboard:
    TERRITORIO       = "El Hierro"          # Territorio a visualizar
    FUENTE           = Fuentes.SALARIOS    # Fuente de renta a mostrar
    DIM_SOCIAL       = "Nacionalidad"      # "Estudios" | "Sexo" | "Nacionalidad"
    COMPARAR_SUBS    = True                # True → cada línea es un subterritorio
    DESGLOSAR_MUNIS  = True                # True → municipios, False → isla agregada
    MOSTRAR_LEYENDA  = True
    EJE_Y_CERO       = False               # Si False, check_eje_cero fallará en WARN


# ── Umbrales de calidad visual ─────────────────────────────────────────────────
# Modifica estos valores para ajustar la sensibilidad de los checks.

MAX_CATEGORIAS_COLOR  = 9     # más de 9 colores son indistinguibles
MAX_LABEL_LENGTH      = 40    # caracteres máximos en una etiqueta de eje
RATIO_ESCALA_MAX      = 20.0  # ratio max/min tolerable en eje Y antes de distorsión
DOMINANCE_OTROS_MAX   = 0.40  # fracción máxima tolerable para categoría "Otros"

