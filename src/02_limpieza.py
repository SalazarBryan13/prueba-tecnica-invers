import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, make_url

def clean_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza los nombres de pacientes y doctores eliminando prefijos/sufijos y aplicando Title Case.
    
    Args:
        df (pd.DataFrame): DataFrame crudo que contiene las columnas 'Name' y 'Doctor'.
        
    Returns:
        pd.DataFrame: DataFrame con las columnas de nombres limpias.
    """
    df = df.copy()
    
    # Expresiones regulares para limpiar (ignorando mayúsculas/minúsculas)
    prefix_pattern = r'(?i)^(Mr\.|Mrs\.|Ms\.|Dr\.)\s+'
    suffix_pattern = r'(?i)\s+\b(Jr\.|Sr\.|II|III|IV|DDS|MD|PhD|DVM)\b'
    
    for col in ['Name', 'Doctor']:
        # 1. Eliminar prefijos
        df[col] = df[col].str.replace(prefix_pattern, '', regex=True)
        # 2. Eliminar sufijos
        df[col] = df[col].str.replace(suffix_pattern, '', regex=True)
        # 3. Quitar espacios extra y estandarizar a Title Case
        df[col] = df[col].str.strip().str.title()
        
    return df

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina registros exactamente duplicados del dataset.
        
    Args:
        df (pd.DataFrame): DataFrame con filas potencialmente duplicadas.
        
    Returns:
        pd.DataFrame: DataFrame sin duplicados.
    """
    df = df.drop_duplicates(keep='first')
    return df

def handle_negative_billing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Corrige los montos de facturación negativos utilizando valores absolutos.
    
    Asume que los valores negativos son errores de digitación (guiones) en lugar 
    de reembolsos legítimos, dada la baja frecuencia de ocurrencia.
    
    Args:
        df (pd.DataFrame): DataFrame que contiene 'Billing Amount'.
        
    Returns:
        pd.DataFrame: DataFrame con montos de facturación positivos.
    """
    df = df.copy()
    df['Billing Amount'] = df['Billing Amount'].abs()
    return df

def clean_hospital_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza los nombres de hospitales eliminando comas y palabras que generan ruido como "and".
    
    Args:
        df (pd.DataFrame): DataFrame que contiene la columna 'Hospital'.
        
    Returns:
        pd.DataFrame: DataFrame con los nombres de hospitales limpios.
    """
    df = df.copy()
    # Quita cualquier coma
    df['Hospital'] = df['Hospital'].str.replace(r',', '', regex=True) 
    
    # Quita and al inicio o al final
    df['Hospital'] = df['Hospital'].str.replace(r'(?i)(^and\s+|\s+and$)', '', regex=True)
    
    # Limpia espacios
    df['Hospital'] = df['Hospital'].str.replace(r'\s+', ' ', regex=True).str.strip() 
    return df



def cast_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte las columnas de fecha a objetos datetime estándar para el procesamiento posterior.
    
    Args:
        df (pd.DataFrame): DataFrame con cadenas de fecha.
        
    Returns:
        pd.DataFrame: DataFrame con columnas datetime.
    """
    df = df.copy()
    df['Date of Admission'] = pd.to_datetime(df['Date of Admission'])
    df['Discharge Date'] = pd.to_datetime(df['Discharge Date'])
    return df

def create_database(db_uri: str) -> None:
    """
    Crea la base de datos objetivo de PostgreSQL si aún no existe.
    
    Args:
        db_uri (str): URI de conexión de la base de datos objetivo.
    """
    parsed_url = make_url(db_uri)
    db_name = parsed_url.database
    default_uri = parsed_url.set(database='postgres')
    
    engine = create_engine(default_uri, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        res = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'"))
        if not res.scalar():
            print(f"[*] Base de datos '{db_name}' no encontrada. Creándola automáticamente...")
            conn.execute(text(f"CREATE DATABASE {db_name}"))
            print("[*] Base de datos creada exitosamente.")

def run_cleaning(input_path: str, output_path: str, db_uri: str = None) -> pd.DataFrame:
    """
    Ejecuta el pipeline de limpieza y carga a Staging.
    
    Args:
        input_path (str): Ruta al dataset CSV crudo.
        output_path (str): Ruta donde se guardará el  CSV limpio.
        db_uri (str): URI de la BD para la carga en Staging.
        
    Returns:
        pd.DataFrame: El DataFrame completamente limpio.
    """
    
    print("="*60)
    print("[INFO] Iniciando Pipeline de Limpieza de Datos")
    print("="*60)

    # 1. Cargar datos crudos
    print(f"\n[PASO 1] Ingestando datos crudos desde: {input_path}")
    df_raw = pd.read_csv(input_path, low_memory=False)
    print(f"Registros iniciales cargados: {len(df_raw):,}")

    # 2. Estandarizar nombres
    print("[PASO 2] Estandarizando nombres de pacientes y doctores...")
    df_nombres_limpios = clean_names(df_raw)
    
    # 3. Eliminar duplicados
    print("[PASO 3] Eliminando duplicados exactos...")
    df_sin_duplicados = remove_duplicates(df_nombres_limpios)

    # 4. Corregir facturación negativa
    print("[PASO 4] Corrigiendo montos de facturación negativos...")
    df_facturacion = handle_negative_billing(df_sin_duplicados)

    # 5. Estandarizar nombres de hospitales
    print("[PASO 5] Estandarizando nombres de hospitales...")
    df_hospitales = clean_hospital_names(df_facturacion)

    # 6. Convertir columnas de fecha a objetos datetime
    print("[PASO 6] Convirtiendo columnas de fecha a objetos datetime...")
    df_limpio = cast_data_types(df_hospitales)

    # 7. Guardar  CSV limpio 
    print(f"\n[PASO 7] Generando CSV limpio local en: {output_path}")
    df_limpio.to_csv(output_path, index=False)
    
    # 8. Cargar datos limpios al Área Staging de la Base de Datos
    if db_uri:
        print(f"\n[PASO 8] Cargando datos limpios al Área Staging de la Base de Datos...")
        create_database(db_uri)
        engine = create_engine(db_uri)
        df_limpio.to_sql('stg_healthcare', con=engine, if_exists='replace', index=False)
        print("La tabla de staging ha sido poblada exitosamente.")
    
    print("\n" + "="*60)
    print("Fase de limpieza completada")
    print("="*60 + "\n")
    
    return df_limpio

