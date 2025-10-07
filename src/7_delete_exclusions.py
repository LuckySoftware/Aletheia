"""
limpiar_exclusiones_directo.py

Este script elimina directamente de 'validated_data' los registros
que coinciden con una regla de exclusión (exclusion = 0).
No pide confirmación. Carga las credenciales desde el archivo .env.
"""
import os
import sys
import logging
import psycopg2
from dotenv import load_dotenv

# --- 1. Configuración ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] - %(message)s')


# Se calcula la ruta raíz del proyecto y se carga el .env desde allí.
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
ENV_PATH = os.path.join(PROJECT_ROOT, '.env')

if os.path.exists(ENV_PATH):
    load_dotenv(dotenv_path=ENV_PATH)
else:
    logging.critical(f"FATAL: No se encontró el archivo .env en la ruta: {ENV_PATH}")
    sys.exit(1)



# Carga la configuración de la base de datos desde las variables de entorno ya cargadas
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}

# --- 2. Consulta SQL de Eliminación ---
DELETE_QUERY = """
    DELETE FROM validated_data
    USING excluded_data
    WHERE
        validated_data.timestamp_col = excluded_data.fecha_hora
        AND excluded_data.exclusion = 0;
"""

def main():
    """Función principal que orquesta el proceso de limpieza."""
    if not all(DB_CONFIG.values()):
        logging.error("FATAL: Faltan variables de base de datos en .env. Revisa el archivo.")
        sys.exit(1)

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # --- Ejecutar la eliminación directamente ---
                logging.info("Ejecutando la eliminación de registros excluidos de 'validated_data'...")
                cur.execute(DELETE_QUERY)
                deleted_count = cur.rowcount

                # `conn.commit()` se llama automáticamente al salir del bloque `with conn`.

                if deleted_count > 0:
                    logging.info(f"Éxito - Se eliminaron {deleted_count} registros de la tabla 'validated_data'.")
                else:
                    logging.info("No se encontraron registros para eliminar. La tabla ya estaba limpia.")

    except psycopg2.OperationalError as e:
        logging.critical(f"Error de conexión: No se pudo conectar a la base de datos. Detalle: {e}")
    except psycopg2.Error as e:
        logging.error(f"Error de base de datos. La transacción fue revertida. Detalle: {e}")
    except Exception as e:
        logging.critical(f"Error inesperado: {e}", exc_info=True)


if __name__ == "__main__":
    main()