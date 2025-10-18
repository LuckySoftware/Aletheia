# =================================================================================================
# SCRIPT DE GENERACIÓN DE REPORTES A EXCEL
# -------------------------------------------------------------------------------------------------
# Este script se conecta a una base de datos, extrae datos y genera reportes en formato Excel.
# Está diseñado para ser ejecutado en un entorno Docker, leyendo su configuración de
# variables de entorno para máxima portabilidad y seguridad.
# =================================================================================================

# --- 1. IMPORTACIÓN DE DEPENDENCIAS ---
# Se importan las librerías necesarias para el funcionamiento del script.

import os  # Para interactuar con el sistema operativo (ej: construir rutas de archivos).
import logging  # Para registrar mensajes informativos y de error durante la ejecución.
import sys  # Para interactuar con el sistema, como terminar el script en caso de un error fatal.
from typing import Dict, Any, Union, List  # Para definir tipos de datos, mejorando la legibilidad del código.
from datetime import date  # Para obtener la fecha actual y nombrar los reportes.
from decimal import Decimal  # Para manejar números de alta precisión provenientes de la base de datos.

import pandas as pd  # Librería fundamental para la manipulación y análisis de datos (DataFrames).
import psycopg2  # El adaptador (driver) para conectar Python con la base de datos PostgreSQL.
from psycopg2 import sql  # Módulo específico para construir consultas SQL de forma segura y dinámica.
from psycopg2.sql import Composed, SQL  # Clases para componer consultas SQL seguras.


# --- 2. CONFIGURACIÓN INICIAL ---
# Se establecen las configuraciones básicas para que el script se ejecute correctamente.

# Se configura el sistema de logging para que muestre mensajes en la consola.
logging.basicConfig(
    level=logging.INFO,  # Se establece el nivel mínimo de mensajes a mostrar (INFO, WARNING, ERROR, etc.).
    format='%(asctime)s - [%(levelname)s] - %(message)s'  # Se define el formato de cada mensaje de log.
)


# --- 3. CONSTANTES Y CONFIGURACIONES GLOBALES ---
# Se definen las variables y configuraciones que no cambiarán durante la ejecución.

# Se crea un diccionario con las credenciales para la conexión a la base de datos.
DB_CONFIG: Dict[str, Any] = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Lista completa y ordenada de los encabezados para el archivo Excel de datos validados.
VALIDATED_DATA_HEADERS = [
    "TIMESTAMP",
    "CAÑAHUATE I-MET1_Irradiancia horizontal global - Promedio [W/m²]",
    "CAÑAHUATE I-MET2_Irradiancia horizontal global - Promedio [W/m²]",
    "CAÑAHUATE I-MET1_Temperatura ambiente - Promedio [°C]",
    "CAÑAHUATE I-MET2_Temperatura ambiente - Promedio [°C]",
    "CAÑAHUATE I-MET1_Irradiancia Plano de Módulos 1 - Promedio [W/m²]",
    "CAÑAHUATE I-MET1_Irradiancia Plano de Módulos 2 - Promedio [W/m²]",
    "CAÑAHUATE I-MET2_Irradiancia Plano de Módulos 1 - Promedio [W/m²]",
    "CAÑAHUATE I-MET2_Irradiancia Plano de Módulos 2 - Promedio [W/m²]",
    "CAÑAHUATE I-MET1_Temperatura de Panel 1 - Promedio [°C]",
    "CAÑAHUATE I-MET1_Temperatura de Panel 2 - Promedio [°C]",
    "CAÑAHUATE I-MET1_Temperatura de Panel 3 - Promedio [°C]",
    "CAÑAHUATE I-MET2_Temperatura de Panel 1 - Promedio [°C]",
    "CAÑAHUATE I-MET2_Temperatura de Panel 2 - Promedio [°C]",
    "CAÑAHUATE I-MET2_Temperatura de Panel 3 - Promedio [°C]",
    "CAÑAHUATE I-MET1_Irradiancia difusa 1 - Promedio [W/m²]",
    "CAÑAHUATE I-MET2_Irradiancia Difusa 1 - Promedio [W/m²]",
    "CAÑAHUATE I-MET1_Irradiancia de Albedo 1 - Promedio [W/m²]",
    "CAÑAHUATE I-MET1_Irradiancia de Albedo 2 - Promedio [W/m²]",
    "CAÑAHUATE I-MET2_Irradiancia de Albedo 1 - Promedio [W/m²]",
    "CAÑAHUATE I-MET2_Irradiancia de Albedo 2 - Promedio [W/m²]",
    "Medidor 115kV Principal_Energía Activa Entregada - Medidor Principal - Diferencia [kWh]",
    "Medidor 115kV Respaldo_Energía Activa Entregada - Medidor Backup - Diferencia [kWh]",
    "Factor de Potencia - Medidor Principal",
    "PPC Setpoint Interno del Lazo [kW]",
    "NCU01-TC03-TRK393_Posición Actual - Promedio [°]",
    "NCU01-TC03-TRK393_Setpoint - Promedio [°]",
    "TRK397_Posición Actual [°]",
    "TRK397_Setpoint [°]",
    "TRK940_Posición Actual [°]",
    "TRK940_Setpoint [°]",
    "TRK993_Posición Actual [°]",
    "TRK993_Setpoint [°]",
    "PPC Variable de Control del Lazo",
    "Nombre de Nueva Columna 1",
    "Nombre de Nueva Columna 2",
    "Nombre de Nueva Columna 3"
]

# Se define la consulta SQL para obtener los datos de errores.
QUERY_ERRORES = SQL("""
    SELECT
        d.timestamp,
        STRING_AGG(e.validation_rule_id::text, ', ') AS identificador_de_regla,
        STRING_AGG(e.offending_column, ', ') AS columnas_con_error,
        STRING_AGG(e.offending_value, ', ') AS valores_con_error
    FROM public.validation_error_by_rules AS e
    INNER JOIN public.raw_data AS d ON e.raw_data_id = d.id
    WHERE d.timestamp::time BETWEEN '07:00' AND '18:59:59'
    GROUP BY d.timestamp
    ORDER BY d.timestamp;
""")


# --- 4. FUNCIONES AUXILIARES ---
# Funciones pequeñas y reutilizables que realizan tareas específicas.

def crear_conexion_db(config: Dict[str, Any]) -> Union[psycopg2.extensions.connection, None]:
    """Esta función intenta conectarse a la base de datos usando las credenciales proporcionadas."""
    try:
        conn = psycopg2.connect(**config)
        logging.info(f"Conexión a la base de datos '{config['dbname']}' en '{config['host']}' establecida.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Error al conectar con la base de datos: {e}")
        return None

def obtener_columnas_tabla(conn: psycopg2.extensions.connection, schema: str, table_name: str, exclude_cols: list) -> List[str]:
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

def ejecutar_consulta_a_dataframe(conn: psycopg2.extensions.connection, query: Union[SQL, Composed]) -> Union[pd.DataFrame, None]:
    """Esta función ejecuta una consulta SQL y convierte el resultado en un DataFrame de pandas."""
    try:
        with conn.cursor() as cur:
            logging.info("Ejecutando consulta...")
            cur.execute(query)
            columnas = [desc[0] for desc in cur.description] if cur.description else []
            resultados = cur.fetchall()
            df = pd.DataFrame(resultados, columns=columnas, dtype='object')
            logging.info(f"Consulta ejecutada. Se encontraron {len(df)} registros.")
            return df
    except (psycopg2.Error, psycopg2.DatabaseError) as e:
        logging.error(f"Error durante la ejecución de la consulta: {e}")
        conn.rollback()
        return None

def hacer_datetimes_naive(df: Union[pd.DataFrame, None]) -> Union[pd.DataFrame, None]:
    """Convierte columnas de fecha con zona horaria a 'naive' para compatibilidad con Excel."""
    if df is None:
        return None
    for col in df.columns:
        if 'timestamp' in col or 'time' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    for col in df.select_dtypes(include=['datetimetz']).columns:
        logging.info(f"Convirtiendo la columna de fecha '{col}' a timezone-naive.")
        df[col] = df[col].dt.tz_localize(None)
    return df

def exportar_a_excel(df: Union[pd.DataFrame, None], full_path: str) -> bool:
    """Esta función guarda un DataFrame de pandas en un archivo Excel .xlsx."""
    if df is None or df.empty:
        logging.warning(f"No hay datos para exportar a {full_path}. Se omitirá el archivo.")
        return False
    try:
        output_dir = os.path.dirname(full_path)
        os.makedirs(output_dir, exist_ok=True)
        df_copy = df.copy()
        for col_name in df_copy.columns:
            if any(isinstance(x, Decimal) for x in df_copy[col_name].dropna()):
                logging.info(f"Formateando columna de alta precisión '{col_name}' para Excel.")
                df_copy[col_name] = df_copy[col_name].apply(
                    lambda x: '0' if isinstance(x, Decimal) and x.is_zero() else str(x).replace('.', ',') if isinstance(x, Decimal) else x
                )
        df_copy.to_excel(full_path, index=False, engine='openpyxl')
        logging.info(f"Reporte '{os.path.basename(full_path)}' generado exitosamente en '{output_dir}'.")
        return True
    except Exception as e:
        logging.error(f"Ocurrió un error inesperado al guardar el archivo Excel '{full_path}': {e}")
        return False


# --- 5. FUNCIÓN PRINCIPAL DE EJECUCIÓN ---
# Esta es la función que orquesta todo el flujo de trabajo del script.

def main():
    """Función principal que llama a las demás funciones en el orden correcto."""
    logging.info("Iniciando el proceso de generación de reportes.")

    if any(value is None for value in DB_CONFIG.values()):
        logging.critical("Faltan variables de entorno de la base de datos. Saliendo.")
        sys.exit(1)

    output_dir = os.getenv('VALIDATED_OUTPUT_DIR')
    if not output_dir:
        logging.critical("FATAL: La variable de entorno 'VALIDATED_OUTPUT_DIR' no está definida. Saliendo.")
        sys.exit(1)

    conn = crear_conexion_db(DB_CONFIG)
    if not conn:
        sys.exit(1)

    try:
        today_str = date.today().strftime('%Y-%m-%d')

        # --- Bloque de Proceso para el Reporte de Errores ---
        logging.info("Generando reporte de errores...")
        df_errores = ejecutar_consulta_a_dataframe(conn, QUERY_ERRORES)
        df_errores = hacer_datetimes_naive(df_errores)
        file_name_errores = f"reporte_errores_{today_str}.xlsx"
        ruta_errores = os.path.join(output_dir, file_name_errores)
        exportar_a_excel(df_errores, ruta_errores)

        # --- Bloque de Proceso para el Reporte de Datos Validados ---
        # A diferencia del reporte de errores, este se construye dinámicamente.
        logging.info("Generando reporte de datos validados...")
        # Se definen las columnas internas que no deben aparecer en el reporte final.
        columnas_a_excluir = ['id', 'raw_data_id', 'status', 'created_at', 'processed_at']
        # Se le pide a la base de datos la lista actual de columnas en la tabla 'validated_data'.
        columnas_db = obtener_columnas_tabla(conn, 'public', 'validated_data', columnas_a_excluir)
        
        # Se procede solo si la función anterior devolvió una lista de columnas.
        if columnas_db:
            # Se construye la consulta SQL de forma segura para evitar inyección SQL.
            query_validados = sql.SQL("SELECT {cols} FROM {table}").format(
                # Se unen los nombres de las columnas, escapando cada uno como un identificador SQL.
                cols=sql.SQL(', ').join(map(sql.Identifier, columnas_db)),
                # Se escapa el nombre de la tabla como un identificador SQL.
                table=sql.Identifier('public', 'validated_data')
            )
            # Se ejecuta la consulta dinámica para obtener los datos validados.
            df_validados = ejecutar_consulta_a_dataframe(conn, query_validados)
            
            # Se convierte cualquier columna de fecha a un formato compatible con Excel.
            df_validados = hacer_datetimes_naive(df_validados)

            # Se realizan comprobaciones sobre el DataFrame resultante antes de guardarlo.
            if df_validados is not None:
                # Se verifica que el número de columnas obtenidas de la BD coincida con el esperado.
                if len(df_validados.columns) == len(VALIDATED_DATA_HEADERS):
                    # Si coinciden, se renombran las columnas con los nombres descriptivos definidos arriba.
                    logging.info("La cantidad de columnas coincide. Renombrando encabezados.")
                    df_validados.columns = VALIDATED_DATA_HEADERS
                else:
                    # Si no coinciden, es un error grave. Se registra y se anula el DataFrame.
                    logging.error(
                        f"Discrepancia de columnas! La BD devolvió {len(df_validados.columns)} columnas "
                        f"pero se esperaban {len(VALIDATED_DATA_HEADERS)}. No se generará el reporte."
                    )
                    # Al ponerlo a None, la función 'exportar_a_excel' no creará el archivo.
                    df_validados = None

            # Se define el nombre del archivo para el reporte de datos validados.
            file_name_validados = f"reporte_validados_{today_str}.xlsx"
            # Se construye la ruta de salida completa.
            ruta_validados = os.path.join(output_dir, file_name_validados)
            # Se llama a la función de exportación.
            exportar_a_excel(df_validados, ruta_validados)
        else:
            # Si no se pudieron obtener las columnas de la tabla, se registra una advertencia.
            logging.warning("No se encontraron columnas para 'validated_data'. Se omitirá este reporte.")

    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")

    logging.info("Proceso de generación de reportes finalizado.")


# --- 6. PUNTO DE ENTRADA DEL SCRIPT ---
if __name__ == '__main__':
    main()