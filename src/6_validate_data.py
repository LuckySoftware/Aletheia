"""
validate_data_in_database.py

Script para validar datos crudos de una tabla,
moviendo los registros válidos a una tabla de datos limpios y registrando
los errores encontrados en una tabla de errores.

Características clave:
- Carga dinámica de reglas de validación desde la base de datos.
- Procesamiento por lotes (batch processing) para manejar grandes volúmenes de datos.
- Mecanismo de bloqueo de filas para evitar condiciones de carrera si se ejecutan múltiples instancias.
- Lógica de validación modular y fácilmente extensible.
- Transacciones atómicas para garantizar la integridad de los datos.
"""

import os
import logging
from decimal import Decimal, InvalidOperation
import sys
from typing import Any, Dict, List, Tuple

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# --- 1. Configuración del Logging ---
# Define el formato y nivel del logging para todo el script.
# En producción, esto podría configurarse para escribir a un archivo o a un servicio de logging centralizado.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler()]  # Muestra los logs en la consola.
)


# Se calcula la ruta raíz del proyecto y se carga el .env desde allí.
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
ENV_PATH = os.path.join(PROJECT_ROOT, '.env')

if os.path.exists(ENV_PATH):
    load_dotenv(dotenv_path=ENV_PATH)
else:
    logging.critical(f"FATAL: No se encontró el archivo .env en la ruta: {ENV_PATH}")
    sys.exit(1)

# --- 2. Constantes ---
# Centralizar los "valores mágicos" como constantes mejora la mantenibilidad y evita errores de tipeo.
class RuleTypes:
    """Define los tipos de reglas de validación permitidos."""
    RANGE = 'range'
    # Futuros tipos de reglas irían aquí. Ej: REGEX = 'regex'

class ErrorCodes:
    """Define identificadores para errores que no provienen de una regla específica."""
    SYSTEM_RULE = 'SYSTEM_RULE'      # Para fallos de pre-condiciones, como un timestamp nulo.
    INVALID_FORMAT = 'INVALID_FORMAT'  # Para valores que no tienen el formato esperado (ej: no numérico).
    MALFORMED_RULE = 'MALFORMED_RULE'  # Para reglas mal configuradas en la base de datos.

class Status:
    """Define los estados posibles para un registro en la tabla de datos crudos."""
    PENDING = 'pending'
    SUCCESS = 'success'
    ERROR = 'error'

# --- 3. Gestión de la Base de Datos ---
class DatabaseManager:
    """Gestiona el ciclo de vida de la conexión con la base de datos PostgreSQL."""

    def __init__(self):
        """
        Carga la configuración de la base de datos desde variables de entorno.
        Lanza un error si falta alguna variable esencial.
        """
        load_dotenv()  # Carga las variables desde el archivo .env
        self.db_config = {
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432")
        }
        # Valida que todas las variables de entorno necesarias estén presentes.
        if not all(self.db_config.values()):
            raise ValueError("Faltan variables de configuración de DB en el archivo .env")
        self.conn = None

    def __enter__(self) -> psycopg2.extensions.connection:
        """
        Establece la conexión a la base de datos al entrar en un bloque 'with'.
        Esto asegura que la conexión se maneje de forma segura.
        """
        self.conn = psycopg2.connect(**self.db_config)
        logging.info("Conexión a la base de datos establecida.")
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Cierra la conexión de forma segura al salir del bloque 'with',
        incluso si ocurren errores.
        """
        if self.conn:
            self.conn.close()
            logging.info("Conexión a la base de datos cerrada.")

# --- 4. Lógica Principal de Validación ---
class RowByRowValidator:
    """Orquesta el proceso de validación, cargando reglas y procesando filas."""

    def __init__(self, conn: psycopg2.extensions.connection):
        """
        Inicializa el validador.

        Args:
            conn: Una conexión activa a la base de datos.
        """
        self.conn = conn
        # Mapeo de tipos de reglas a sus funciones de validación.
        # Este diseño modular (patrón Dispatcher) es clave para un código extensible.
        self.validation_functions = {
            RuleTypes.RANGE: self._validate_range
        }
        # Carga y organiza las reglas de validación desde la base de datos para un acceso eficiente.
        self.rules_map = self._load_and_map_rules()

    def _load_and_map_rules(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Obtiene las reglas activas de la base de datos y las organiza en un diccionario
        para un acceso rápido, donde la clave es el nombre de la columna.
        """
        logging.info("Cargando y mapeando las reglas de validación activas...")
        query = "SELECT rule_id, column_name, rule_type, rule_config FROM validation_rules WHERE is_active = TRUE;"
        rules_map: Dict[str, List[Dict[str, Any]]] = {}
        
        # Se utiliza un DictCursor para poder acceder a las filas como diccionarios (por nombre de columna).
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query)
            for rule in cur.fetchall():
                col_name = rule['column_name']
                if col_name not in rules_map:
                    rules_map[col_name] = []
                rules_map[col_name].append(dict(rule))
        
        rule_count = sum(len(v) for v in rules_map.values())
        logging.info(f"Se cargaron {rule_count} reglas para {len(rules_map)} columnas.")
        return rules_map

    # --- Funciones de Validación Específicas ---
    def _validate_range(self, value: Decimal, config: Dict[str, Any]) -> bool:
        """Función específica para validar una regla de tipo 'rango'."""
        min_val = Decimal(config['min'])
        max_val = Decimal(config['max'])
        return min_val <= value <= max_val

    # --- Método Central de Validación por Fila ---
    def _validate_single_row(self, row: psycopg2.extras.DictRow) -> List[Dict[str, Any]]:
        """
        Valida una fila completa contra todas las reglas aplicables, acumulando todos los errores.
        """
        errors: List[Dict[str, Any]] = []

        # 1. Validación de pre-condiciones (reglas de sistema).
        if row.get('timestamp_col') is None:
            errors.append({
                'rule_id': ErrorCodes.SYSTEM_RULE,
                'column_name': 'timestamp_col',
                'value': 'El timestamp es nulo'
            })

        # 2. Itera sobre las columnas que tienen reglas definidas.
        for col_name, rules in self.rules_map.items():
            if col_name not in row or row[col_name] is None:
                continue

            value = row[col_name]

            # 3. Intenta convertir el valor a un tipo numérico estándar.
            try:
                numeric_value = Decimal(str(value).replace(',', '.'))
            except (InvalidOperation, TypeError):
                errors.append({
                    'rule_id': ErrorCodes.INVALID_FORMAT,
                    'column_name': col_name,
                    'value': f"Valor no numérico: '{value}'"
                })
                continue  # Si no es numérico, no se pueden aplicar más reglas; se pasa a la siguiente columna.

            # 4. Aplica todas las reglas para la columna actual usando el despachador de funciones.
            for rule in rules:
                rule_type = rule['rule_type']
                validation_func = self.validation_functions.get(rule_type)

                if not validation_func:
                    logging.warning(f"No se encontró una función de validación para el tipo de regla '{rule_type}'.")
                    continue
                
                try:
                    is_valid = validation_func(numeric_value, rule['rule_config'])
                    if not is_valid:
                        errors.append({
                            'rule_id': rule['rule_id'],
                            'column_name': col_name,
                            'value': str(value)
                        })
                except KeyError as e:
                    # Manejo robusto: si la regla en la DB está mal configurada (ej: falta 'min').
                    logging.error(f"Regla mal configurada para rule_id={rule['rule_id']}. Falta la clave: {e}")
                    errors.append({
                        'rule_id': ErrorCodes.MALFORMED_RULE,
                        'column_name': col_name,
                        'value': "Configuración de regla inválida en la base de datos."
                    })
        return errors

    # --- Orquestador del Proceso ---
    def run_process(self, batch_size: int = 1000):
        """Ejecuta el ciclo principal de validación, procesando filas en lotes."""
        if not self.rules_map:
            logging.warning("No hay reglas de validación activas para procesar. Terminando.")
            return
        total_processed = 0
        while True:
            # `with self.conn` gestiona la transacción: commit al final, o rollback si hay una excepción.
            with self.conn, self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                try:
                    # Selecciona un lote de filas pendientes.
                    # FOR UPDATE SKIP LOCKED es crucial para producción:
                    # - FOR UPDATE: Bloquea las filas para que otras transacciones no las modifiquen.
                    # - SKIP LOCKED: Si otra instancia del script ya bloqueó filas, esta consulta las ignora
                    #   y toma las siguientes, permitiendo el procesamiento concurrente seguro.
                    cur.execute(
                        "SELECT * FROM raw_data WHERE status = %s LIMIT %s FOR UPDATE SKIP LOCKED;",
                        (Status.PENDING, batch_size)
                    )
                    batch = cur.fetchall()

                    if not batch:
                        logging.info("Proceso finalizado. No hay más datos pendientes.")
                        break
                    
                    # Define la lista de columnas de forma segura antes del bucle para evitar errores y ser más eficiente.
                    raw_cols: List[str] = []
                    if cur.description is not None:
                        raw_cols = [desc.name for desc in cur.description if desc.name not in ('status', 'created_at', 'processed_at')]

                    logging.info(f"Procesando un lote de {len(batch)} registros...")
                    
                    validated_data, error_data = [], []
                    validated_ids, error_ids = [], []

                    for row in batch:
                        found_errors = self._validate_single_row(row)
                        if found_errors:
                            error_ids.append(row['raw_id'])
                            logging.warning(f"Fila raw_id={row['raw_id']} tiene {len(found_errors)} error(es).")
                            for error in found_errors:
                                error_data.append((row['raw_id'], error['rule_id'], error.get('column_name', 'N/A'), error['value']))
                        else:
                            validated_ids.append(row['raw_id'])
                            row_values = tuple(str(row[col]).replace(',', '.') if isinstance(row[col], str) else row[col] for col in raw_cols)
                            validated_data.append(row_values)
                    
                    # --- Operaciones de Base de Datos (Bulk Operations) ---
                    # Usar `execute_values` es mucho más eficiente que hacer un INSERT por cada fila.
                    if validated_data:
                        cols_str = ", ".join(f'"{c}"' for c in raw_cols)
                        insert_sql = f"INSERT INTO validated_data ({cols_str}) VALUES %s"
                        psycopg2.extras.execute_values(cur, insert_sql, validated_data)
                        cur.execute("UPDATE raw_data SET status = %s, processed_at = NOW() WHERE raw_id IN %s;", (Status.SUCCESS, tuple(validated_ids)))

                    if error_data:
                        insert_errors_sql = "INSERT INTO error_data (raw_id, rule_id, offending_column, offending_value) VALUES %s"
                        psycopg2.extras.execute_values(cur, insert_errors_sql, error_data)
                        cur.execute("UPDATE raw_data SET status = %s, processed_at = NOW() WHERE raw_id IN %s;", (Status.ERROR, tuple(error_ids)))
                    
                    logging.info(f"Lote procesado: {len(validated_ids)} filas válidas, {len(error_ids)} filas con error.")
                    total_processed += len(batch)

                except psycopg2.Error as e:
                    logging.error(f"Error de base de datos. La transacción será revertida. Detalle: {e}", exc_info=True)
                    break
                except Exception as e:
                    logging.error(f"Error inesperado en el procesamiento. La transacción será revertida. Detalle: {e}", exc_info=True)
                    break

        logging.info(f"Validación completada. Total de registros procesados: {total_processed}.")

# --- 5. Punto de Entrada del Script ---
def main():
    """Punto de entrada principal del script."""
    logging.info("Iniciando el script de validación de datos...")
    try:
        with DatabaseManager() as conn:
            validator = RowByRowValidator(conn)
            validator.run_process()
    except ValueError as e:
        logging.critical(f"Error de configuración: {e}")
    except psycopg2.OperationalError as e:
        logging.critical(f"No se pudo conectar a la base de datos: {e}")
    except Exception as e:
        logging.critical(f"Error fatal no controlado en la ejecución: {e}", exc_info=True)

if __name__ == "__main__":
    main()