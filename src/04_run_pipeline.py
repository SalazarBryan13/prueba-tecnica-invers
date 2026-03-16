import os
import sys
import importlib

# Agregar la carpeta 'src' al path para poder importar los scripts como módulos
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(script_dir)

from dotenv import load_dotenv

# Cargar variables desde el archivo .env en la raíz
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

limpieza = importlib.import_module("02_limpieza")
modelado = importlib.import_module("03_modelado")

def run_end_to_end_pipeline(db_uri: str):
    print("="*60)
    print("INICIANDO PIPELINE DE DATOS")
    print("="*60)
    
    # ---------------------------------------------------------
    # FASE 2: LIMPIEZA & STAGING 
    # ---------------------------------------------------------
    print("\n[PASO 1] Ejecutando Fase 2 - Limpieza de Datos y Carga a Staging...")
    try:
        input_path = os.path.join(script_dir, "..", "data", "healthcare_dataset.csv")
        output_path = os.path.join(script_dir, "..", "data", "healthcare_dataset_cleaned.csv")
        # Definir la Base de Datos compartida para ambas fases
        
        # Llama a la limpieza inyectando la URL de base de datos para habilitar el guardado a Staging
        df_cleaned = limpieza.run_cleaning(input_path, output_path, db_uri=db_uri)
        print(f" Limpieza exitosa. {len(df_cleaned)} registros listos para modelar.")
    except Exception as e:
        print(f" Error en la Fase 2 (Limpieza): {e}")
        return

    # ---------------------------------------------------------
    # FASE 3: MODELADO Y CARGA  A POSTGRESQL
    # ---------------------------------------------------------
    print("\n[PASO 2] Ejecutando Fase 3 - Modelado ELT In-Database...")
    try:
        # Extraemos las rutas dinámicas necesarias para el script de modelado

        schema_file = os.path.join(script_dir, "schema.sql")
        elt_file = os.path.join(script_dir, "etl_insert.sql")
        
        # Ejecutar transformación: SQL DDL -> SQL ELT
        modelado.run_elt_pipeline(db_uri, schema_file, elt_file)
        
        print(f" Modelado y Carga ELT exitosa. Base de datos lista en: {db_uri}")
    except Exception as e:
        print(f" Error en la Fase 3 (Modelado): {e}")
        return
        
    print("\n" + "="*60)
    print("PIPELINE COMPLETADO EXITOSAMENTE.")
    print("="*60)

if __name__ == "__main__":
    run_end_to_end_pipeline(db_uri)
