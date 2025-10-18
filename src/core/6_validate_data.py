"""
validate_data_in_database.py

Script para validar datos crudos de una tabla, moviendo los registros válidos
a una tabla de datos limpios y registrando tanto los errores encontrados como
las reglas que fueron superadas exitosamente.

Características clave:
- Carga dinámica de reglas de validación desde la base de datos.
- Procesamiento por lotes (batch processing) para manejar grandes volúmenes de datos.
- Mecanismo de bloqueo de filas para evitar condiciones de carrera.
- Lógica de validación modular y extensible.
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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# --- Configuración de Rutas ---
PROJECT_ROOT = "/app"
ENV_PATH = os.path.join(PROJECT_ROOT, '.env')

if os.path.exists(ENV_PATH):
    load_dotenv(dotenv_path=ENV_PATH)
else:
    logging.critical(f"FATAL: No se encontró el archivo .env en la ruta: {ENV_PATH}")
    sys.exit(1)

# --- 2. Constantes y Clases de Enumeración ---
class TiposDeRegla:
    RANGO = 'range'

class CodigosErrorSistema:
    REGLA_SISTEMA = 'SYSTEM_RULE'
    FORMATO_INVALIDO = 'INVALID_FORMAT'
    REGLA_MALFORMADA = 'MALFORMED_RULE'

class EstadoRegistro:
    PENDIENTE = 'pending'
    EXITO = 'success'
    ERROR = 'error'

# --- 3. Gestión de la Base de Datos ---
class GestorBaseDatos:
    """Gestiona el ciclo de vida de la conexión con la base de datos PostgreSQL."""
    def __init__(self):
        self.config_db = {
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432")
        }
        if not all(self.config_db.values()):
            raise ValueError("Faltan variables de configuración de DB en el archivo .env")
        self.conexion = None

    def __enter__(self) -> psycopg2.extensions.connection:
        self.conexion = psycopg2.connect(**self.config_db)
        logging.info("Conexión a la base de datos establecida.")
        return self.conexion

    def __exit__(self, tipo_excepcion, valor_excepcion, traceback):
        if self.conexion:
            self.conexion.close()
            logging.info("Conexión a la base de datos cerrada.")

# --- 4. Lógica Principal de Validación ---
class ValidadorFilaPorFila:
    """Orquesta el proceso de validación, cargando reglas y procesando filas."""

    def __init__(self, conexion: psycopg2.extensions.connection):
        self.conexion = conexion
        self.funciones_validacion = {
            TiposDeRegla.RANGO: self._validar_rango
        }
        self.mapa_reglas = self._cargar_y_mapear_reglas()

    def _cargar_y_mapear_reglas(self) -> Dict[str, List[Dict[str, Any]]]:
        """Obtiene las reglas activas y las organiza en un diccionario para acceso rápido."""
        logging.info("Cargando y mapeando las reglas de validación activas...")
        consulta_reglas = "SELECT id, column_name, rule_type, rule_config FROM validation_rules WHERE is_active = TRUE;"
        mapa_reglas: Dict[str, List[Dict[str, Any]]] = {}

        with self.conexion.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

            # 1. Obtener el valor dinámico (p.nom) de la base de datos.
            cursor.execute("SELECT p_nom FROM plant_parameters ORDER BY modified_at DESC LIMIT 1")
            p_nom_result = cursor.fetchone()
            if not p_nom_result:
                raise ValueError("No se encontró el parámetro 'p_nom' en la tabla 'plant_parameters'.")
            
            # --- FIX: Use the correct key 'p_nom' instead of 'valor' ---
            valor_p_nom = p_nom_result['p_nom']
            # --- END FIX ---
            
            logging.info(f"Valor dinámico para P.NOM cargado: {valor_p_nom}")

            # 2. Cargar todas las reglas.
            cursor.execute(consulta_reglas)
            for regla in cursor.fetchall():

                # 3. Revisar y reemplazar el placeholder en la configuración de cada regla.
                config = regla['rule_config']
                if isinstance(config.get('max'), str) and config['max'] == '$P_NOM':
                    logging.info(f"Reemplazando placeholder en la regla para '{regla['column_name']}'. Max anterior: {config['max']}")
                    config['max'] = valor_p_nom
                    logging.info(f"Max nuevo para '{regla['column_name']}': {config['max']}")

                # 4. Construir el mapa de reglas como antes.
                nombre_col = regla['column_name']
                if nombre_col not in mapa_reglas:
                    mapa_reglas[nombre_col] = []
                mapa_reglas[nombre_col].append(dict(regla))

        conteo_reglas = sum(len(v) for v in mapa_reglas.values())
        logging.info(f"Se cargaron {conteo_reglas} reglas para {len(mapa_reglas)} columnas.")
        return mapa_reglas

    def _validar_rango(self, valor: Decimal, config: Dict[str, Any]) -> bool:
        """Función específica para validar una regla de tipo 'rango'."""
        min_val = Decimal(config['min'])
        max_val = Decimal(config['max'])
        return min_val <= valor <= max_val

    def _validar_fila_unica(self, fila: psycopg2.extras.DictRow) -> Tuple[List[Dict[str, Any]], List[int]]:
        """
        Valida una fila completa.
        Retorna una tupla con (lista_de_errores, lista_de_ids_de_reglas_superadas).
        """
        errores: List[Dict[str, Any]] = []
        reglas_superadas: List[int] = []

        if fila.get('timestamp') is None:
            errores.append({
                'id_regla': CodigosErrorSistema.REGLA_SISTEMA,
                'nombre_columna': 'timestamp',
                'valor': 'El timestamp es nulo'
            })

        for nombre_col, reglas in self.mapa_reglas.items():
            if nombre_col not in fila or fila[nombre_col] is None:
                continue

            valor = fila[nombre_col]

            try:
                valor_numerico = Decimal(str(valor).replace(',', '.'))
            except (InvalidOperation, TypeError):
                errores.append({
                    'id_regla': CodigosErrorSistema.FORMATO_INVALIDO,
                    'nombre_columna': nombre_col,
                    'valor': f"Valor no numérico: '{valor}'"
                })
                continue

            for regla in reglas:
                tipo_regla = regla['rule_type']
                funcion_validacion = self.funciones_validacion.get(tipo_regla)
                if not funcion_validacion:
                    continue
                
                try:
                    es_valido = funcion_validacion(valor_numerico, regla['rule_config'])
                    if not es_valido:
                        errores.append({
                            'id_regla': regla['id'],
                            'nombre_columna': nombre_col,
                            'valor': str(valor)
                        })
                    else:
                        # NUEVO: Se registra la regla que fue superada con éxito.
                        reglas_superadas.append(regla['id'])
                except KeyError as e:
                    logging.error(f"Regla mal configurada para id={regla['id']}. Falta la clave: {e}")
                    errores.append({
                        'id_regla': CodigosErrorSistema.REGLA_MALFORMADA,
                        'nombre_columna': nombre_col,
                        'valor': "Configuración de regla inválida en la BD."
                    })
        return errores, reglas_superadas

    def ejecutar_proceso(self, tamano_lote: int = 1000):
        """Ejecuta el ciclo principal de validación, procesando filas en lotes."""
        if not self.mapa_reglas:
            logging.warning("No hay reglas de validación activas para procesar. Terminando.")
            return
        total_procesados = 0
        while True:
            with self.conexion, self.conexion.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                try:
                    cursor.execute(
                        "SELECT * FROM raw_data WHERE status = %s LIMIT %s FOR UPDATE SKIP LOCKED;",
                        (EstadoRegistro.PENDIENTE, tamano_lote)
                    )
                    lote = cursor.fetchall()

                    if not lote:
                        logging.info("Proceso finalizado. No hay más datos pendientes.")
                        break
                    
                    if cursor.description is None:
                        logging.error("No se pudo obtener la descripción de las columnas")
                        break
                    columnas_raw = [desc.name for desc in cursor.description]
                    columnas_datos_a_mover = [c for c in columnas_raw if c not in {'id', 'status', 'created_at', 'processed_at'}]
                    columnas_destino_validated = ['raw_data_id'] + columnas_datos_a_mover
                    
                    logging.info(f"Procesando un lote de {len(lote)} registros...")
                    
                    datos_validados_para_insertar = []
                    datos_errores_para_insertar = []
                    ids_validados_en_raw = []
                    ids_con_error_en_raw = []
                    # NUEVO: Mapa para relacionar un id de raw_data con las reglas que superó.
                    mapa_reglas_superadas: Dict[int, List[int]] = {}

                    for fila in lote:
                        errores_encontrados, reglas_superadas = self._validar_fila_unica(fila)
                        id_fila_actual = fila['id']

                        if errores_encontrados:
                            ids_con_error_en_raw.append(id_fila_actual)
                            for error in errores_encontrados:
                                datos_errores_para_insertar.append((id_fila_actual, error['id_regla'], error.get('nombre_columna', 'N/A'), error['valor']))
                        else:
                            ids_validados_en_raw.append(id_fila_actual)
                            valores_fila = [id_fila_actual] + [fila[col] for col in columnas_datos_a_mover]
                            datos_validados_para_insertar.append(tuple(valores_fila))
                            # NUEVO: Se guarda qué reglas superó esta fila.
                            mapa_reglas_superadas[id_fila_actual] = reglas_superadas
                    
                    # --- Operaciones de Base de Datos ---
                    if ids_con_error_en_raw:
                        consulta_insertar_errores = "INSERT INTO validation_error_by_rules (raw_data_id, validation_rule_id, offending_column, offending_value) VALUES %s"
                        psycopg2.extras.execute_values(cursor, consulta_insertar_errores, datos_errores_para_insertar)
                        cursor.execute("UPDATE raw_data SET status = %s, processed_at = NOW() WHERE id IN %s;", (EstadoRegistro.ERROR, tuple(ids_con_error_en_raw)))

                    if ids_validados_en_raw:
                        # Paso 1: Insertar en validated_data y obtener los nuevos IDs.
                        columnas_str = ", ".join(f'"{c}"' for c in columnas_destino_validated)
                        consulta_insertar_validos = f"INSERT INTO validated_data ({columnas_str}) VALUES %s RETURNING id, raw_data_id"
                        
                        # Usamos execute_values con un template para la consulta.
                        id_map_tuples = psycopg2.extras.execute_values(
                            cursor, consulta_insertar_validos, datos_validados_para_insertar, fetch=True
                        )
                        mapa_raw_a_validado = {raw_id: valid_id for valid_id, raw_id in id_map_tuples}

                        # Paso 2: Preparar los datos para validated_data_by_rules.
                        datos_reglas_superadas = []
                        for raw_id, reglas in mapa_reglas_superadas.items():
                            id_validado = mapa_raw_a_validado.get(raw_id)
                            if id_validado:
                                for id_regla in reglas:
                                    datos_reglas_superadas.append((id_validado, id_regla))
                        
                        # Paso 3: Insertar en validated_data_by_rules.
                        if datos_reglas_superadas:
                            consulta_insertar_reglas = "INSERT INTO validated_data_by_rules (validated_data_id, rule_id) VALUES %s"
                            psycopg2.extras.execute_values(cursor, consulta_insertar_reglas, datos_reglas_superadas)

                        # Paso 4: Actualizar el estado en raw_data.
                        cursor.execute("UPDATE raw_data SET status = %s, processed_at = NOW() WHERE id IN %s;", (EstadoRegistro.EXITO, tuple(ids_validados_en_raw)))

                    logging.info(f"Lote procesado: {len(ids_validados_en_raw)} filas válidas, {len(ids_con_error_en_raw)} filas con error.")
                    total_procesados += len(lote)

                except psycopg2.Error as e:
                    logging.error(f"Error de base de datos. La transacción será revertida. Detalle: {e}", exc_info=True)
                    break 
                except Exception as e:
                    logging.error(f"Error inesperado. La transacción será revertida. Detalle: {e}", exc_info=True)
                    break 

            logging.info(f"Validación completada. Total de registros procesados: {total_procesados}.")

# --- 5. Punto de Entrada del Script ---
def main():
    """Punto de entrada principal del script."""
    logging.info("Iniciando el script de validación de datos...")
    try:
        with GestorBaseDatos() as conexion:
            validador = ValidadorFilaPorFila(conexion)
            validador.ejecutar_proceso()
    except ValueError as e:
        logging.critical(f"Error de configuración: {e}")
    except psycopg2.OperationalError as e:
        logging.critical(f"No se pudo conectar a la base de datos: {e}")
    except Exception as e:
        logging.critical(f"Error fatal no controlado en la ejecución: {e}", exc_info=True)

if __name__ == "__main__":
    main()