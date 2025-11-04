import pandas as pd
import os
from etl.transform_dims import create_all_dims
from etl.transform_facts import create_all_facts

# Rutas de los directorios (ajustado a 'RAW')
RAW_PATH = 'RAW'
WAREHOUSE_PATH = 'warehouse'

def main():
    print("Iniciando Proceso ETL (Extract, Transform, Load) para EcoBottle...")
    
    # 1. Asegurar directorios de salida
    os.makedirs(os.path.join(WAREHOUSE_PATH, 'dim'), exist_ok=True)
    os.makedirs(os.path.join(WAREHOUSE_PATH, 'fact'), exist_ok=True)
    
    # 2. Extracción: Cargar todos los CSVs
    raw_data = load_raw_data()
    
    # 3. Transformación y Carga de DIMENSIONES
    print("--> Creando Tablas de Dimensiones...")
    dimensions = create_all_dims(raw_data) # Devuelve las dimensiones para usarlas en Hechos
    
    # 4. Transformación y Carga de HECHOS
    print("--> Creando Tablas de Hechos...")
    create_all_facts(raw_data, dimensions)
    
    print("Proceso ETL Finalizado. Data Warehouse listo en el directorio 'warehouse/'.")

def load_raw_data():
    """Carga todos los archivos CSV del directorio RAW."""
    raw_data = {}
    print(f"Leyendo datos desde: {RAW_PATH}")
    for filename in os.listdir(RAW_PATH):
        if filename.endswith('.csv'):
            table_name = filename.replace('.csv', '')
            file_path = os.path.join(RAW_PATH, filename)
            
            # Usamos low_memory=False para mejor manejo de tipos grandes
            raw_data[table_name] = pd.read_csv(file_path, low_memory=False)
            
    return raw_data

if __name__ == '__main__':
    main()