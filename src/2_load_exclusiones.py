import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import sys
import os
import pandas as pd
from dotenv import load_dotenv
import logging
from datetime import datetime

# --- CONFIGURACIÓN DE LOGGING ---
def setup_logging():
    """Configura el sistema de logging para registrar únicamente en la consola."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout) # Handler para escribir a la consola
        ]
    )
    logging.info("Logging configurado para salida por terminal.")

# --- CONFIGURACIÓN DE RUTAS Y ENTORNO ---

# 1. Obtiene la ruta del directorio donde está este script (.../src)
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
# 2. Sube un nivel para obtener la ruta raíz del proyecto
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
# 3. Construye la ruta completa y segura al archivo .env
ENV_PATH = os.path.join(PROJECT_ROOT, '.env')

# Llama a la función para configurar el logging al inicio
setup_logging()

# Carga las variables de entorno desde la ruta específica
if os.path.exists(ENV_PATH):
    load_dotenv(dotenv_path=ENV_PATH)
    logging.info("Archivo .env cargado correctamente.")
else:
    logging.error(f"No se encontró el archivo .env en la ruta esperada: {ENV_PATH}")
    sys.exit(1)

# --- CONFIGURACIÓN DE LA CONEXIÓN A LA BASE DE DATOS ---
DB_CONFIG = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT")
}

# Verificación de que todas las variables de entorno para la DB están presentes
if not all(DB_CONFIG.values()):
    logging.error("Faltan variables de entorno para la DB. Revisa tu archivo .env.")
    sys.exit(1)

def find_excel_files(root_path):
    """
    Busca recursivamente todos los archivos .xlsx en la ruta especificada.
    """
    if not os.path.isdir(root_path):
        logging.error(f"La ruta de exclusiones '{root_path}' no es un directorio válido.")
        return []

    excel_files = []
    logging.info(f"Buscando archivos .xlsx en el directorio: {root_path}")
    for dirpath, _, filenames in os.walk(root_path):
        for f in filenames:
            if f.endswith('.xlsx'):
                full_path = os.path.join(dirpath, f)
                excel_files.append(full_path)
    
    logging.info(f"Se encontraron {len(excel_files)} archivos para procesar.")
    return excel_files

def prepare_data_for_db(file_path):
    """
    Lee un archivo Excel y prepara los datos para la inserción de forma robusta.
    """
    try:
        df = pd.read_excel(file_path)

        # 1. Renombrar columnas a un formato estándar (minúsculas) inmediatamente.
        df.rename(columns={
            'Potencia_Pico_kW': 'potencia_pico_kw'
        }, inplace=True)

        # 2. Asegurar el tipo de dato correcto y manejar valores vacíos.
        df['fecha_hora'] = pd.to_datetime(df['fecha_hora'])
        df['potencia_pico_kw'] = pd.to_numeric(df['potencia_pico_kw'], errors='coerce')
        df['exclusion'] = pd.to_numeric(df['exclusion'], errors='coerce')
        df['motivo'] = df['motivo'].astype(object).where(pd.notnull(df['motivo']), None)

        # 3. Derivar columnas de tiempo a partir de 'fecha_hora'.
        df['anio'] = df['fecha_hora'].dt.year
        df['mes'] = df['fecha_hora'].dt.month
        df['dia'] = df['fecha_hora'].dt.day
        df['hora'] = df['fecha_hora'].dt.hour
        df['minuto'] = df['fecha_hora'].dt.minute
        df['segundo'] = df['fecha_hora'].dt.second

        # 4. Definir la lista final de columnas en el orden correcto para la DB.
        final_columns = [
            "fecha_hora", "potencia_pico_kw", "exclusion", "motivo", 
            "anio", "mes", "dia", "hora", "minuto", "segundo"
        ]
        
        # 5. Seleccionar solo las columnas necesarias.
        df_final = df[final_columns]

        # 6. Convertir el DataFrame a una lista de tuplas, manejando los NaN/NA de Pandas.
        records_with_na = [tuple(r) for r in df_final.to_numpy()]
        records = [tuple(None if pd.isna(v) else v for v in r) for r in records_with_na]
        
        return records

    except FileNotFoundError:
        logging.warning(f"No se encontró el archivo en la ruta especificada: {file_path}")
        return None
    except Exception:
        logging.exception(f"Error inesperado al procesar el archivo '{os.path.basename(file_path)}'.")
        return None

def main():
    """
    Función principal que orquesta todo el proceso.
    """
    logging.info("--- INICIANDO PROCESO DE CARGA DE EXCLUSIONES ---")
    exclusions_folder = os.environ.get("EXCLUSIONES_FOLDER_PATH")
    if not exclusions_folder:
        logging.error("La variable 'EXCLUSIONES_FOLDER_PATH' no está definida en tu archivo .env.")
        sys.exit(1)
    
    if not os.path.isabs(exclusions_folder):
        exclusions_folder = os.path.join(PROJECT_ROOT, exclusions_folder)

    files_to_process = find_excel_files(exclusions_folder)
    if not files_to_process:
        logging.info("No hay archivos para procesar. Finalizando el script.")
        return

    total_rows_processed = 0
    
    conn = None
    try:
        logging.info("Estableciendo conexión con la base de datos...")
        conn = psycopg2.connect(
            f"dbname={DB_CONFIG['dbname']} user={DB_CONFIG['user']} password={DB_CONFIG['password']} "
            f"host={DB_CONFIG['host']} port={DB_CONFIG['port']}"
        )
        logging.info("Conexión a la base de datos exitosa.")

        with conn.cursor() as cur:
            for file_path in files_to_process:
                filename = os.path.basename(file_path)
                logging.info(f"Procesando archivo: {filename}")
                
                prepared_records = prepare_data_for_db(file_path)
                
                if prepared_records:
                    total_rows_processed += len(prepared_records)
                    
                    table_name = "excluded_data"
                    # Lista de columnas actualizada para la consulta SQL
                    columns = [
                        "fecha_hora", "potencia_pico_kw", "exclusion", "motivo", 
                        "anio", "mes", "dia", "hora", "minuto", "segundo"
                    ]
                    
                    # Query actualizada sin las columnas eliminadas
                    query = sql.SQL("""
                        INSERT INTO {table} ({fields}) VALUES %s
                        ON CONFLICT (fecha_hora) DO UPDATE SET
                        potencia_pico_kw = EXCLUDED.potencia_pico_kw,
                        exclusion = EXCLUDED.exclusion,
                        motivo = EXCLUDED.motivo,
                        anio = EXCLUDED.anio,
                        mes = EXCLUDED.mes,
                        dia = EXCLUDED.dia,
                        hora = EXCLUDED.hora,
                        minuto = EXCLUDED.minuto,
                        segundo = EXCLUDED.segundo
                    """).format(
                        table=sql.Identifier(table_name),
                        fields=sql.SQL(', ').join(map(sql.Identifier, columns))
                    )
                    
                    execute_values(cur, query, prepared_records)
                    logging.info(f"-> Se procesaron {cur.rowcount} filas desde '{filename}'.")

        conn.commit()
        logging.info("--- TRANSACCIÓN GLOBAL CONFIRMADA (COMMIT) ---")

    except psycopg2.Error:
        logging.exception("Error crítico de base de datos. Revirtiendo todos los cambios... (ROLLBACK)")
        if conn:
            conn.rollback()
    except Exception:
        logging.exception("Ha ocurrido un error inesperado durante la ejecución principal.")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")

    logging.info("--- RESUMEN DEL PROCESO ---")
    logging.info(f"Archivos procesados: {len(files_to_process)}")
    logging.info(f"Total de filas procesadas de los archivos: {total_rows_processed}")
    logging.info("-----------------------------\n")

if __name__ == "__main__":
    main()