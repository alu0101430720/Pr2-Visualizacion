import os
import glob
import pandas as pd
from dagster import asset_check, AssetCheckResult, MetadataValue
import warnings
from assets import preprocesar_datos_p5

# Mapeo de Municipios de Canarias por Isla
MUNICIPIOS_POR_ISLA = {
    "Tenerife": {
        "Adeje", "Arafo", "Arico", "Arona", "Buenavista del Norte", "Candelaria",
        "Fasnia", "Garachico", "Granadilla de Abona", "La Guancha", "Guía de Isora",
        "Güímar", "Icod de los Vinos", "La Matanza de Acentejo", "La Orotava",
        "Puerto de la Cruz", "Puerto de La Cruz", "Los Realejos", "El Rosario", "San Cristóbal de La Laguna",
        "San Juan de la Rambla", "San Miguel de Abona", "Santa Cruz de Tenerife",
        "Santa Úrsula", "Santiago del Teide", "El Sauzal", "Los Silos", "Tacoronte",
        "El Tanque", "Tegueste", "La Victoria de Acentejo", "Vilaflor", "Vilaflor de Chasna"
    },
    "Gran Canaria": {
        "Agaete", "Agüimes", "Artenara", "Arucas", "Firgas", "Gáldar", "Ingenio",
        "Mogán", "Moya", "Las Palmas de Gran Canaria", "San Bartolomé de Tirajana",
        "La Aldea de San Nicolás", "Santa Brígida", "Santa Lucía de Tirajana",
        "Santa María de Guía de Gran Canaria", "Tejeda", "Telde", "Teror", "Valleseco",
        "Valsequillo de Gran Canaria", "Vega de San Mateo"
    },
    "La Palma": {
        "Barlovento", "Breña Alta", "Breña Baja", "Fuencaliente de la Palma", "Fuencaliente de La Palma", "Garafía",
        "Los Llanos de Aridane", "El Paso", "Puntagorda", "Puntallana", "San Andrés y Sauces",
        "Santa Cruz de la Palma", "Santa Cruz de La Palma", "Tazacorte", "Tijarafe", "Villa de Mazo"
    },
    "Lanzarote": {
        "Arrecife", "Haría", "San Bartolomé", "Teguise", "Tías", "Tinajo", "Yaiza"
    },
    "Fuerteventura": {
        "Antigua", "Betancuria", "La Oliva", "Pájara", "Puerto del Rosario", "Tuineje"
    },
    "La Gomera": {
        "Agulo", "Alajeró", "Hermigua", "San Sebastián de la Gomera", "San Sebastián de La Gomera", "Valle Gran Rey", "Vallehermoso"
    },
    "El Hierro": {
        "La Frontera", "Frontera", "El Pinar de El Hierro", "Pinar de El Hierro, El", "Valverde"
    }
}

ESPERADOS_ISLAS = {
    "Tenerife": 31,
    "Gran Canaria": 21,
    "La Palma": 14,
    "Lanzarote": 7,
    "Fuerteventura": 6,
    "La Gomera": 6,
    "El Hierro": 3
}

def inferir_isla(municipio):
    # Formateo básico para evitar discordancias simples como comas
    municipio_limpio = municipio.replace(",", "").strip()
    for isla, munis in MUNICIPIOS_POR_ISLA.items():
        if municipio_limpio in munis:
            return isla
        # Búsqueda suave si no encaja perfecto
        for m in munis:
            if m.lower() in municipio_limpio.lower() or municipio_limpio.lower() in m.lower():
                return isla
    return "Desconocida"

@asset_check(asset=preprocesar_datos_p5, description="Comprueba que no existen valores nulos en el dataset.")
def check_ausencia_nulos(context, preprocesar_datos_p5: str):
    csv_files = glob.glob(os.path.join(preprocesar_datos_p5, "*.csv"))
    
    total_nulos = 0
    report_md = "### Reporte de Nulos Gestalt\n\n| Dataset | Total Nulos |\n|---------|-------------|\n"
    
    for file in csv_files:
        df = pd.read_csv(file)
        n_nulos = df.isna().sum().sum()
        total_nulos += n_nulos
        
        # Agrupación visual por dataset
        status = "🔴 Alerta" if n_nulos > 0 else "🟢 Limpio"
        report_md += f"| `{os.path.basename(file)}` | {n_nulos} ({status}) |\n"
        
    return AssetCheckResult(
        passed=bool(total_nulos == 0),
        metadata={
            "Resumen_Nulos": MetadataValue.md(report_md)
        }
    )

@asset_check(asset=preprocesar_datos_p5, description="Verifica la exactitud del conteo de municipios por Isla, filtrando por provincia según el nombre.")
def check_conteo_municipios(context, preprocesar_datos_p5: str):
    csv_files = glob.glob(os.path.join(preprocesar_datos_p5, "*.csv"))
    if not csv_files:
        return AssetCheckResult(passed=False, metadata={"Error": MetadataValue.md("No se encontraron CSVs.")})
    
    passed = True
    report_md = "### Balance Geográfico de Cierre Gestalt\n\nNuestra regla de cierre exige una cantidad estricta de municipios en el dataset, segmentada por islas aplicables.\n\n"
    
    ISLAS_SC = {"Tenerife", "La Palma", "La Gomera", "El Hierro"}
    
    for file in csv_files:
        fname = os.path.basename(file)
        report_md += f"\n#### Dataset: `{fname}`\n"
        df = pd.read_csv(file)
        
        if "municipio" not in df.columns:
            passed = False
            report_md += "🔴 **Error:** Columna 'municipio' faltante en este dataset.\n"
            continue
            
        conteo_por_isla = {isla: set() for isla in ESPERADOS_ISLAS.keys()}
        municipios_desconocidos = set()
        
        for muni in df["municipio"].dropna().unique():
            isla = inferir_isla(muni)
            if isla != "Desconocida":
                conteo_por_isla[isla].add(muni)
            else:
                municipios_desconocidos.add(muni)
                
        # Si el dataset es exclusivamente de SC de Tenerife
        is_sc_only = "-sc-" in fname.lower()
        
        report_md += "| Isla | Encontrados | Esperados | Estado |\n|---|---|---|---|\n"
        
        for isla, expected in ESPERADOS_ISLAS.items():
            if is_sc_only and isla not in ISLAS_SC:
                continue
                
            found = len(conteo_por_isla[isla])
            if found != expected:
                passed = False
                status = f"❌ Faltan/Sobran ({found - expected})"
            else:
                status = "✅ Exacto"
                
            report_md += f"| **{isla}** | {found} | {expected} | {status} |\n"
            
        if municipios_desconocidos:
            report_md += f"\n**⚠️ Principio de Inconsistencia:** Municipios atípicos: {', '.join(municipios_desconocidos)}\n"
            
    return AssetCheckResult(
        passed=passed,
        metadata={"Balance_Islas": MetadataValue.md(report_md)}
    )

@asset_check(asset=preprocesar_datos_p5, description="Asegura continuidad en el Periodo Temporal y dicotomía exacta en Sexo.")
def check_temporal_y_sexo(context, preprocesar_datos_p5: str):
    csv_files = glob.glob(os.path.join(preprocesar_datos_p5, "*.csv"))
    
    report_md = "### Continuidad Gestalt\n"
    passed = True
    
    for file in csv_files:
        df = pd.read_csv(file)
        fname = os.path.basename(file)
        report_md += f"\n#### Dataset: `{fname}`\n"
        
        # Check Periodo
        if "Periodo" in df.columns:
            periodos = sorted(df["Periodo"].dropna().unique())
            if len(periodos) > 1:
                # Comprobar saltos matemáticos en años ej: [2021, 2022, 2023]
                saltos = [periodos[i+1] - periodos[i] for i in range(len(periodos)-1)]
                if any(s > 1 for s in saltos):
                    passed = False
                    report_md += "- **Periodo**: 🔴 Se ha quebrado la ley de continuidad. Años detectados con salto: " + str(periodos) + "\n"
                else:
                    report_md += "- **Periodo**: 🟢 Contínuo y estructurado. Años: " + str(periodos) + "\n"
                    
        # Check Sexo
        if "Sexo" in df.columns:
            sexos_existentes = set(df["Sexo"].dropna().unique())
            esperados = {"Hombres", "Mujeres"}
            # Comprobar si hay elementos aberrantes ("Ambos", "Desconocido", etc)
            extra = sexos_existentes - esperados
            if extra:
                passed = False
                report_md += f"- **Sexo**: 🔴 Se quebró la Ley de Semejanza. Categorías extra detectadas: {extra}\n"
            else:
                report_md += f"- **Sexo**: 🟢 Cumple la ley de proximidad estructural estricta ({sexos_existentes}).\n"
                
    return AssetCheckResult(
        passed=passed,
        metadata={"Reporte_Estructural": MetadataValue.md(report_md)}
    )
