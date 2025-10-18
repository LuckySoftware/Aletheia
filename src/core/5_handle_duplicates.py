"""
handle_duplicates.py

Script optimizado para encontrar, mover y eliminar registros duplicados 
de la tabla raw_data. Su diseño es compatible con la estructura de la tabla duplicated_data.
"""
import os
import logging
import psycopg2
import sys
from dotenv import load_dotenv

# --- Configuración del Logging ---
# Se establece un formato claro para los mensajes de registro que se mostrarán en la consola.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# --- Configuración de Rutas ---
# 1. Se calcula la ruta raíz del proyecto de forma dinámica para que el script
#    funcione sin importar desde dónde se ejecute.
PROJECT_ROOT = "/app"
ENV_PATH = os.path.join(PROJECT_ROOT, '.env')

# 2. Se carga el archivo .env desde esa ruta específica.
if os.path.exists(ENV_PATH):
    load_dotenv(dotenv_path=ENV_PATH)
else:
    logging.critical(f"FATAL: No se encontró el archivo .env en la ruta: {ENV_PATH}")
    sys.exit(1)


class ManejadorDuplicados:
    """
    Gestiona la detección, movimiento y limpieza de duplicados en la tabla raw_data.
    """
    def __init__(self):
        """
        Lee la configuración de la base de datos desde las variables de entorno ya cargadas.
        """
        self.config_db = {
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432")
        }
        self.conexion = None

    def conectar(self):
        """
        Establece la conexión con la base de datos.
        Lanza un error si faltan credenciales.
        """
        if not all(self.config_db.values()):
            raise ValueError("Faltan variables de configuración de DB. Revisa tu archivo .env.")
        self.conexion = psycopg2.connect(**self.config_db)
        logging.info("Conexión a la base de datos establecida para manejar duplicados.")

    def procesar_duplicados(self):
        """
        Orquesta el proceso completo: encontrar, mover y eliminar duplicados
        dentro de una única transacción para garantizar la integridad de los datos.
        """
        try:
            self.conectar()
            if not self.conexion: return

            with self.conexion.cursor() as cursor:
                # Paso 1: Obtener dinámicamente las columnas de datos (ej: col_1, col_2...).
                # Esto hace el script adaptable a si se añaden o quitan columnas en el futuro.
                cursor.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE lower(table_name) = 'raw_data' AND column_name LIKE 'col_%'
                    ORDER BY ordinal_position;
                """)
                columnas_datos = [fila[0] for fila in cursor.fetchall()]
                if not columnas_datos:
                    logging.warning("No se encontraron columnas de datos (col_x) en raw_data. No se puede continuar.")
                    return

                # Paso 2: Definir las columnas de origen (raw_data) y destino (duplicated_data).
                # Columnas que se seleccionarán desde `raw_data`.
                columnas_origen = ['id', 'timestamp'] + columnas_datos
                columnas_origen_str = ", ".join([f'"{c}"' for c in columnas_origen])

                # Columnas donde se insertarán los datos en `duplicated_data`.
                # Se mapea `raw_data.id` a `duplicated_data.raw_data_id` y
                # `raw_data.timestamp` a `duplicated_data.timestamp_col`.
                columnas_destino = ['raw_data_id', 'timestamp_col'] + columnas_datos
                columnas_destino_str = ", ".join([f'"{c}"' for c in columnas_destino])

                # Paso 3: Construir la consulta CTE (Common Table Expression).
                # Esta consulta identifica los duplicados particionando por 'timestamp'
                # y asignando un número de fila. El primero (rn=1) es el original.
                consulta_cte = """
                WITH filas_clasificadas AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY timestamp ORDER BY id ASC) as rn
                    FROM raw_data
                )
                """

                # Paso 4: Mover los duplicados (filas con rn > 1) a la tabla `duplicated_data`.
                consulta_mover = consulta_cte + f"""
                INSERT INTO duplicated_data ({columnas_destino_str})
                SELECT {columnas_origen_str}
                FROM filas_clasificadas
                WHERE rn > 1;
                """
                logging.info("Buscando y moviendo registros duplicados a la tabla duplicated_data...")
                cursor.execute(consulta_mover)
                filas_movidas = cursor.rowcount
                logging.info(f"Se movieron {filas_movidas} registros duplicados.")

                # Paso 5: Eliminar los duplicados de la tabla `raw_data` solo si se movió alguno.
                if filas_movidas > 0:
                    consulta_eliminar = consulta_cte + """
                    DELETE FROM raw_data
                    WHERE id IN (SELECT id FROM filas_clasificadas WHERE rn > 1);
                    """
                    logging.info("Eliminando registros duplicados de raw_data...")
                    cursor.execute(consulta_eliminar)
                    filas_eliminadas = cursor.rowcount
                    logging.info(f"Se eliminaron {filas_eliminadas} registros duplicados.")
                else:
                    logging.info("No se encontraron duplicados para eliminar.")

                # Paso 6: Confirmar todos los cambios en la base de datos.
                self.conexion.commit()
                logging.info("Transacción completada exitosamente (COMMIT).")

        except Exception as error:
            logging.error(f"Ocurrió un error crítico al procesar duplicados: {error}")
            if self.conexion:
                self.conexion.rollback()
                logging.warning("Se revirtieron todos los cambios en la base de datos (ROLLBACK).")
        finally:
            if self.conexion:
                self.conexion.close()
                logging.info("Proceso de duplicados finalizado. Conexión a la base de datos cerrada.")

def main():
    """Función principal que inicia el proceso."""
    manejador = ManejadorDuplicados()
    manejador.procesar_duplicados()

if __name__ == "__main__":
    main()