from sqlalchemy import create_engine, text

def run_elt_pipeline(db_uri: str, schema_sql_path: str, elt_sql_path: str):
    """Ejecuta el pipeline para crear el Modelo Estrella."""
    
    engine = create_engine(db_uri)
    print(f"Conectado a la base de datos '{engine.url.database}'...")
    
    # 1. Crear las tablas del Modelo Estrella
    with open(schema_sql_path, 'r', encoding='utf-8') as file:
        schema_script = file.read()
    with engine.begin() as conn:
        conn.execute(text(schema_script))
    
    # 2. Insertar datos en las tablas del Modelo Estrella 
    with open(elt_sql_path, 'r', encoding='utf-8') as file:
        elt_script = file.read()
    with engine.begin() as conn:
        conn.execute(text(elt_script))    
    print("MODELADO ESTRELLA COMPLETADO")
