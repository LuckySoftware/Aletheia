"""
handle_duplicates.py

Script optimizado para encontrar, mover y eliminar registros duplicados 
de la tabla Raw_Data. Su diseño es compatible con la estructura de la tabla Duplicated_Data.
"""
import os
import logging
import psycopg2
import sys
from dotenv import load_dotenv

# --- Configuración del Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler()]
)

# 1. Se calcula la ruta raíz del proyecto de forma dinámica.
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
ENV_PATH = os.path.join(PROJECT_ROOT, '.env')

# 2. Se carga el archivo .env desde esa ruta específica.
if os.path.exists(ENV_PATH):
    load_dotenv(dotenv_path=ENV_PATH)
else:
    # Si no se encuentra, el script no puede continuar.
    logging.critical(f"FATAL: No se encontró el archivo .env en la ruta: {ENV_PATH}")
    sys.exit(1)



class DuplicateHandler:
    """
    Gestiona la detección y limpieza de duplicados en la tabla Raw_Data.
    """
    def __init__(self):
        """Lee la configuración de la base de datos desde las variables de entorno ya cargadas."""
        # La llamada a load_dotenv() se eliminó de aquí.
        self.db_config = {
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432")
        }
        self.conn = None

    def _connect(self):
        """Establece la conexión con la base de datos."""
        if not all(self.db_config.values()):
            raise ValueError("Faltan variables de configuración de DB. Revisa tu archivo .env.")
        self.conn = psycopg2.connect(**self.db_config)
        logging.info("Conexión a la base de datos establecida para manejar duplicados.")

    def process_duplicates(self):
        """
        Orquesta el proceso completo: encontrar, mover y eliminar duplicados.
        """
        try:
            self._connect()
            if not self.conn: return

            with self.conn.cursor() as cur:
                # 1. Obtener dinámicamente las columnas de datos (col_1, col_2, etc.)
                cur.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE lower(table_name) = 'raw_data' AND column_name LIKE 'col_%'
                    ORDER BY ordinal_position;
                """)
                data_columns = [row[0] for row in cur.fetchall()]
                if not data_columns:
                    logging.warning("No se encontraron columnas de datos (col_x) en Raw_Data. No se puede procesar.")
                    return

                all_columns_to_move = ['raw_id', 'timestamp_col'] + data_columns
                all_columns_str = ", ".join([f'"{c}"' for c in all_columns_to_move])
                
                # 2. Construir la consulta CTE optimizada
                cte_query = """
                WITH ranked_rows AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY timestamp_col ORDER BY raw_id ASC) as rn
                    FROM raw_data
                )
                """

                # 3. Mover los duplicados (rn > 1) a la tabla duplicated_data
                move_query = cte_query + f"""
                INSERT INTO duplicated_data ({all_columns_str})
                SELECT {all_columns_str}
                FROM ranked_rows
                WHERE rn > 1;
                """
                logging.info("Buscando y moviendo registros duplicados a la tabla Duplicated_Data...")
                cur.execute(move_query)
                moved_count = cur.rowcount
                logging.info(f"Se movieron {moved_count} registros duplicados.")

                # 4. Eliminar los duplicados (rn > 1) de la tabla raw_data
                if moved_count > 0:
                    delete_query = cte_query + """
                    DELETE FROM raw_data
                    WHERE raw_id IN (SELECT raw_id FROM ranked_rows WHERE rn > 1);
                    """
                    logging.info("Eliminando registros duplicados de Raw_Data...")
                    cur.execute(delete_query)
                    deleted_count = cur.rowcount
                    logging.info(f"Se eliminaron {deleted_count} registros duplicados.")
                else:
                    logging.info("No se encontraron duplicados para eliminar.")

                # 5. Confirmar la transacción
                self.conn.commit()

        except Exception as e:
            logging.error(f"Ocurrió un error crítico al procesar duplicados: {e}")
            if self.conn: self.conn.rollback()
        finally:
            if self.conn:
                self.conn.close()
                logging.info("Proceso de duplicados finalizado. Conexión cerrada.")

def main():
    handler = DuplicateHandler()
    handler.process_duplicates()

if __name__ == "__main__":
    main()