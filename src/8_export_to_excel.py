"""
Generador de Reportes desde PostgreSQL a Excel.

Este script se conecta a una base de datos PostgreSQL para extraer datos
de validaciones y errores, los procesa utilizando la librería pandas, y 
genera dos reportes en formato Excel (.xlsx):
1. Un reporte de los errores detectados.
2. Un reporte con los datos que han sido validados exitosamente.

Utiliza un archivo .env para gestionar las credenciales y las rutas de salida,
asegurando que la información sensible no esté expuesta en el código.
"""

# --- 1. IMPORTACIÓN DE DEPENDENCIAS ---
import os   
import logging
import sys
from typing import Dict, Any
from datetime import date
from decimal import Decimal

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.sql import Composed, SQL
from dotenv import load_dotenv


# --- 2. CONFIGURACIÓN INICIAL ---
# Configuración del logging para un seguimiento detallado de la ejecución.
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
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



# --- 3. CONSTANTES Y CONFIGURACIONES GLOBALES ---
# Diccionario con las credenciales de la base de datos obtenidas del .env.
DB_CONFIG: Dict[str, Any] = {
    'dbname': os.getenv('DB_NAME'), 
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'), 
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Lista de encabezados personalizados para el reporte de datos validados.
VALIDATED_DATA_HEADERS = [
    "TIMESTAMP",
    "MET1_Irradiancia Plano de Módulos 1 [W/m²]",
    "MET1_Irradiancia Plano de Módulos 2 [W/m²]",
    "MET2_Irradiancia Plano de Módulos 1 [W/m²]",
    "MET2_Irradiancia Plano de Módulos 2 [W/m²]",
    "MET1_Temperatura de Panel 1 [°C]",
    "MET1_Temperatura de Panel 2 [°C]",
    "MET1_Temperatura de Panel 3 [°C]",
    "MET2_Temperatura de Panel 1 [°C]",
    "MET2_Temperatura de Panel 2 [°C]",
    "MET2_Temperatura de Panel 3 [°C]",
    "Energía Activa Entregada - Medidor Principal [kWh]",
    "Factor de Potencia - Medidor Principal",
    "PPC Setpoint Interno del Lazo [kW]",
    "TRK393_Posición Actual [°]",
    "TRK393_Setpoint [°]",
    "TRK397_Posición Actual [°]",
    "TRK397_Setpoint [°]",
    "TRK940_Posición Actual [°]",
    "TRK940_Setpoint [°]",
    "TRK993_Posición Actual [°]",
    "TRK993_Setpoint [°]",
    "PPC Variable de Control del Lazo"
]

# Consulta SQL para obtener un resumen de los errores agrupados por timestamp.
QUERY_ERRORES = SQL("""
    SELECT
        d.timestamp_col,
        STRING_AGG(e.rule_id::text, ', ') AS identificador_de_regla,
        STRING_AGG(e.offending_column, ', ') AS columnas_con_error,
        STRING_AGG(e.offending_value, ', ') AS valores_con_error
    FROM public.error_data AS e
    INNER JOIN public.raw_data AS d ON e.raw_id = d.raw_id
    WHERE d.timestamp_col::time BETWEEN '07:00' AND '18:59:59'
    GROUP BY d.timestamp_col
    ORDER BY d.timestamp_col;
""")


# --- 4. FUNCIONES AUXILIARES ---

def crear_conexion_db(config: Dict[str, Any]) -> psycopg2.extensions.connection | None:
    """Establece y retorna una conexión a la base de datos PostgreSQL."""
    try:
        conn = psycopg2.connect(**config)
        logging.info("Conexión a la base de datos establecida.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Error al conectar con la base de datos: {e}")
        return None

def obtener_columnas_tabla(conn: psycopg2.extensions.connection, schema: str, table_name: str, exclude_cols: list) -> list[str]:
    """Consulta los metadatos de la DB para obtener los nombres de las columnas de una tabla."""
    if exclude_cols is None:
        exclude_cols = []
    query = SQL("SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s AND column_name NOT IN %s ORDER BY ordinal_position;")
    try:
        with conn.cursor() as cur:
            cur.execute(query, (schema, table_name, tuple(exclude_cols)))
            return [row[0] for row in cur.fetchall()]
    except (psycopg2.Error, psycopg2.DatabaseError) as e:
        logging.error(f"No se pudieron obtener las columnas para la tabla {schema}.{table_name}: {e}")
        return []

def ejecutar_consulta_a_dataframe(conn: psycopg2.extensions.connection, query: SQL | Composed) -> pd.DataFrame | None:
    """
    Ejecuta una consulta SQL y carga los resultados en un DataFrame de pandas,
    preservando la precisión decimal exacta.
    """
    try:
        with conn.cursor() as cur:
            logging.info("Ejecutando consulta...")
            cur.execute(query)
            columnas = [desc[0] for desc in cur.description] if cur.description else []
            resultados = cur.fetchall()
            
            # Crear el DataFrame especificando el tipo 'object' para todas las columnas.
            # Esto previene que pandas convierta automáticamente los tipos NUMERIC de la
            # base de datos (que psycopg2 lee como objetos Decimal de alta precisión)
            # a tipos float de baja precisión. Se preserva la precisión original.
            df = pd.DataFrame(resultados, columns=columnas, dtype='object')
            
            logging.info(f"Consulta ejecutada. Se encontraron {len(df)} registros.")
            return df
    except (psycopg2.Error, psycopg2.DatabaseError) as e:
        logging.error(f"Error durante la ejecución de la consulta: {e}")
        conn.rollback()
        return None

def hacer_datetimes_naive(df: pd.DataFrame | None) -> pd.DataFrame | None:
    """Convierte columnas de fecha con zona horaria a 'naive' para compatibilidad con Excel."""
    if df is None:
        return None
    
    # Primero, convertimos explícitamente las columnas que son de fecha a datetime.
    for col in df.columns:
        if 'timestamp' in col or 'time' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    for col in df.select_dtypes(include=['datetimetz']).columns:
        logging.info(f"Convirtiendo la columna de fecha '{col}' a timezone-naive.")
        df[col] = df[col].dt.tz_localize(None)
    return df

def exportar_a_excel(df: pd.DataFrame | None, full_path: str) -> bool:
    """
    Guarda un DataFrame en un archivo de Excel (.xlsx), convirtiendo los
    números a texto con coma decimal para asegurar la correcta visualización.
    """
    if df is None or df.empty:
        logging.warning(f"No hay datos para exportar a {full_path}. Se omitirá el archivo.")
        return False
    try:
        output_dir = os.path.dirname(full_path)
        os.makedirs(output_dir, exist_ok=True)

        # Se crea una copia del DataFrame para no modificar el original.
        df_copy = df.copy()

        # Iterar sobre las columnas para procesar los números de alta precisión.
        for col_name in df_copy.columns:
            # Comprobar si la columna contiene objetos Decimal para procesarla.
            if any(isinstance(x, Decimal) for x in df_copy[col_name].dropna()):
                logging.info(f"Formateando columna de alta precisión '{col_name}' para Excel.")
                # Aplicar una función a cada celda de la columna:
                # 1. Si es un Decimal que es exactamente cero, lo convierte al texto '0'.
                # 2. Si es otro Decimal, lo convierte a texto y reemplaza el punto por una coma.
                # 3. Si no es un Decimal, lo deja como está.
                df_copy[col_name] = df_copy[col_name].apply(
                    lambda x: '0' if isinstance(x, Decimal) and x.is_zero() else str(x).replace('.', ',') if isinstance(x, Decimal) else x
                )
        
        # Exportar el DataFrame modificado. Ahora los números son texto con el formato deseado.
        df_copy.to_excel(full_path, index=False, engine='openpyxl')
        # --- FIN: LÓGICA CORREGIDA ---
        
        logging.info(f"Reporte '{full_path}' generado exitosamente.")
        return True
    except PermissionError:
        logging.error(
            f"No se pudo guardar el archivo '{full_path}'. "
            f"Error de Permiso Denegado. Por favor, asegúrese de que el archivo no esté abierto en Excel "
            f"u otro programa y que tenga permisos de escritura en el directorio."
        )
        return False
    except Exception as e:
        logging.error(f"Ocurrió un error inesperado al guardar el archivo Excel '{full_path}': {e}")
        return False


# --- 5. FUNCIÓN PRINCIPAL DE EJECUCIÓN ---

def main():
    """
    Función principal que orquesta todo el proceso de generación de reportes.
    """
    logging.info("Iniciando el proceso de generación de reportes.")
    
    # Validar que todas las variables de entorno necesarias estén presentes.
    if any(value is None for value in DB_CONFIG.values()):
        logging.critical("Faltan variables de entorno de la base de datos. Saliendo.")
        return

    errors_dir = os.getenv('ERRORS_OUTPUT_DIR')
    validated_dir = os.getenv('VALIDATED_OUTPUT_DIR')
    if not errors_dir or not validated_dir:
        logging.critical("Faltan las variables de entorno para los directorios de salida (ERRORS_OUTPUT_DIR, VALIDATED_OUTPUT_DIR).")
        return

    # Establecer la conexión a la base de datos.
    conn = crear_conexion_db(DB_CONFIG)
    if not conn:
        return

    try:
        # Obtener la fecha actual para incluirla en los nombres de archivo.
        today_str = date.today().strftime('%Y-%m-%d')
        
        # --- Proceso para el Reporte de Errores ---
        df_errores = ejecutar_consulta_a_dataframe(conn, QUERY_ERRORES)
        df_errores = hacer_datetimes_naive(df_errores)
        
        file_name_errores = f"reporte_errores_{today_str}.xlsx"
        ruta_errores = os.path.join(errors_dir, file_name_errores)
        exportar_a_excel(df_errores, ruta_errores)

        # --- Proceso para el Reporte de Datos Validados ---
        logging.info("Obteniendo dinámicamente las columnas para 'public.validated_data'...")
        columnas_db = obtener_columnas_tabla(conn, 'public', 'validated_data', ['raw_id', 'created_at', 'validated_at'])
        
        if columnas_db:
            # Se construye una consulta SELECT simple, ya que la precisión se maneja en Python.
            query_validados = sql.SQL("SELECT {cols} FROM {table}").format(
                cols=sql.SQL(', ').join(map(sql.Identifier, columnas_db)),
                table=sql.Identifier('public', 'validated_data')
            )
            
            df_validados = ejecutar_consulta_a_dataframe(conn, query_validados)
            df_validados = hacer_datetimes_naive(df_validados)

            if df_validados is not None:
                # Validar que el número de columnas coincida antes de renombrar.
                if len(df_validados.columns) == len(VALIDATED_DATA_HEADERS):
                    logging.info("La cantidad de columnas coincide. Renombrando encabezados.")
                    df_validados.columns = VALIDATED_DATA_HEADERS
                else:
                    logging.error(
                        f"Discrepancia de columnas! La BD devolvió {len(df_validados.columns)} columnas "
                        f"pero se esperaban {len(VALIDATED_DATA_HEADERS)}. No se generará el reporte de validados."
                    )
                    df_validados = None

            file_name_validados = f"reporte_validados_{today_str}.xlsx"
            ruta_validados = os.path.join(validated_dir, file_name_validados)
            exportar_a_excel(df_validados, ruta_validados)
        else:
            logging.warning("No se encontraron columnas para 'validated_data'. Se omitirá este reporte.")
            
    finally:
        # Asegurar que la conexión a la base de datos se cierre siempre.
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")
            
    logging.info("Proceso de generación de reportes finalizado.")


# --- 6. PUNTO DE ENTRADA DEL SCRIPT ---
if __name__ == '__main__':
    main()