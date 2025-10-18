"""
delete_exclusions.py

Este script identifica los registros en 'validated_data' que coinciden
con una regla de exclusión (exclusion = 0), los archiva en la tabla 
de logs de exclusiones y luego los elimina de 'validated_data'.
No pide confirmación y carga las credenciales desde el archivo .env.
"""
import os
import sys
import logging
import psycopg2
from dotenv import load_dotenv

# --- 1. Configuración ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] - %(message)s')

PROJECT_ROOT = "/app"
ENV_PATH = os.path.join(PROJECT_ROOT, '.env')

if os.path.exists(ENV_PATH):
    load_dotenv(dotenv_path=ENV_PATH)
else:
    logging.critical(f"FATAL: No se encontró el archivo .env en la ruta: {ENV_PATH}")
    sys.exit(1)

CONFIG_DB = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}

# --- 2. Consulta SQL de Archivada y Eliminación ---
# Se utiliza una CTE para asegurar que los mismos registros que se archivan son los que se eliminan.
ARCHIVE_AND_DELETE_QUERY = """
WITH filas_a_eliminar AS (
    -- Primero, identificamos los IDs de las filas en validated_data que deben ser eliminadas.
    SELECT v.id
    FROM validated_data v
    JOIN excluded_data e ON v.timestamp = e.fecha_hora
    WHERE e.exclusion = 0
),
archivado AS (
    -- Segundo, insertamos una referencia de la exclusión en la tabla de logs.
    INSERT INTO excluded_data_logs (excluded_data_id, operation_type, new_values, changed_by)
    SELECT 
        e.id, 
        'DELETE', 
        to_jsonb(v.*), 
        'script'
    FROM validated_data v
    JOIN excluded_data e ON v.timestamp = e.fecha_hora
    WHERE v.id IN (SELECT id FROM filas_a_eliminar)
)
-- Finalmente, eliminamos las filas de la tabla original.
DELETE FROM validated_data
WHERE id IN (SELECT id FROM filas_a_eliminar);
"""

def main():
    """Función principal que orquesta el proceso de limpieza."""
    if not all(CONFIG_DB.values()):
        logging.error("FATAL: Faltan variables de base de datos en .env. Revisa el archivo.")
        sys.exit(1)

    try:
        with psycopg2.connect(**CONFIG_DB) as conn:
            with conn.cursor() as cur:
                logging.info("Iniciando el proceso de archivado y eliminación de registros excluidos de 'validated_data'...")
                
                # Ejecutar la consulta completa
                cur.execute(ARCHIVE_AND_DELETE_QUERY)
                deleted_count = cur.rowcount

                # La transacción se confirma automáticamente al salir del bloque `with conn`.
                
                if deleted_count > 0:
                    logging.info(f"Éxito - Se archivaron y eliminaron {deleted_count} registros de la tabla 'validated_data'.")
                else:
                    logging.info("No se encontraron registros para archivar y eliminar. La tabla ya estaba limpia.")

    except psycopg2.OperationalError as e:
        logging.critical(f"Error de conexión: No se pudo conectar a la base de datos. Detalle: {e}")
    except psycopg2.Error as e:
        logging.error(f"Error de base de datos. La transacción fue revertida. Detalle: {e}")
    except Exception as e:
        logging.critical(f"Error inesperado: {e}", exc_info=True)


if __name__ == "__main__":
    main()