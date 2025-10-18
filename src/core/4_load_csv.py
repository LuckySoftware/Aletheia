"""
load_csv_to_database.py (Versión Robusta)

Carga archivos CSV, asegurando que todas las filas contengan datos
numéricamente válidos antes de insertarlos en la base de datos.
"""
import os
import logging
from glob import glob
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] - %(message)s')

PROJECT_ROOT = "/app"
ENV_PATH = os.path.join(PROJECT_ROOT, '.env')
load_dotenv(dotenv_path=ENV_PATH)

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}
CSV_DIRECTORY = os.getenv("CSV_DIRECTORY")
CSV_SEPARATOR = ';'
CSV_ENCODING = 'windows-1252'

def main():
    logging.info("Iniciando script de carga de CSV...")
    if not all(DB_CONFIG.values()):
        logging.error("FATAL: Faltan variables de base de datos en .env.")
        return
    if not CSV_DIRECTORY or not os.path.isdir(CSV_DIRECTORY):
        logging.error(f"FATAL: La ruta CSV_DIRECTORY no es válida: '{CSV_DIRECTORY}'")
        return

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            logging.info(f"Conexión exitosa a la base de datos '{DB_CONFIG['dbname']}'.")
            with conn.cursor() as cur:
                cur.execute("SELECT column_name FROM information_schema.columns WHERE lower(table_name) = 'raw_data' ORDER BY ordinal_position;")
                db_cols = [row[0] for row in cur.fetchall()]
                target_columns = [c for c in db_cols if c not in {'id', 'status', 'created_at', 'processed_at'}]

            if not target_columns:
                logging.error("FATAL: No se pudieron detectar las columnas de 'Raw_Data'.")
                return

            csv_files = glob(os.path.join(CSV_DIRECTORY, "*.csv"))
            if not csv_files:
                logging.warning(f"No se encontraron archivos .csv en '{CSV_DIRECTORY}'.")
                return

            logging.info(f"Se encontraron {len(csv_files)} archivos para procesar.")
            for file_path in csv_files:
                try:
                    logging.info(f"--- Procesando archivo: {os.path.basename(file_path)} ---")
                    df = pd.read_csv(file_path, sep=CSV_SEPARATOR, encoding=CSV_ENCODING, skiprows=1, header=None)

                    if len(df.columns) != len(target_columns):
                        logging.error(f"Se omitió '{os.path.basename(file_path)}': número de columnas incorrecto (Esperado: {len(target_columns)}, Encontrado: {len(df.columns)}).")
                        continue

                    df.columns = target_columns
                    initial_rows = len(df)
                    
                    # Limpiar de datos
                    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                    df.dropna(subset=['timestamp'], inplace=True)

                    numeric_cols = [col for col in target_columns if col != 'timestamp']
                    for col in numeric_cols:
                        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.', regex=False), errors='coerce')
                    
                    #df.dropna(subset=numeric_cols, inplace=True) -- vieja -- eliminar filas con cualquier NaN

                      # 1. Reemplazar explícitamente los ceros (0) por un valor nulo (NA)
                    df[numeric_cols] = df[numeric_cols].replace(0, pd.NA)
                      
                      # 2. Ahora, eliminar todas las filas que tengan cualquier valor nulo (incluyendo los ceros)
                    df.dropna(subset=numeric_cols, inplace=True)
                    
                    cleaned_rows = len(df)
                    logging.info(f"Limpieza de datos: {initial_rows} filas iniciales -> {cleaned_rows} filas válidas.")

                    if df.empty:
                        logging.warning("El archivo no contenía datos válidos después de la limpieza.")
                        continue

                    # Inserción de datos limpios
                    with conn.cursor() as cur:
                        cols_str = '", "'.join(target_columns)
                        insert_query = f'INSERT INTO "raw_data" ("{cols_str}") VALUES %s'
                        data_tuples = list(df.itertuples(index=False, name=None))
                        execute_values(cur, insert_query, data_tuples)
                    conn.commit()
                    logging.info(f"ÉXITO: Se insertaron {len(df)} filas desde '{os.path.basename(file_path)}'.")

                except Exception as e:
                    logging.error(f"ERROR procesando '{os.path.basename(file_path)}': {e}", exc_info=True)
                    conn.rollback()

    except Exception as e:
        logging.critical(f"ERROR CRÍTICO: {e}", exc_info=True)

if __name__ == "__main__":
    main()