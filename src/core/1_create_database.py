# =================================================================================================
# SCRIPT AUTOMÁTICO DE CREACIÓN DE BASE DE DATOS Y ESQUEMA
# -------------------------------------------------------------------------------------------------
# Versión: Final
# Autor: [Tu Nombre/Equipo]
#
# Descripción:
#   Este script es el primer paso fundamental en el ciclo de vida de la aplicación. Su única
#   responsabilidad es construir la infraestructura de la base de datos desde cero de forma
#   completamente automática. Se conecta a un servidor PostgreSQL, crea una nueva base de datos,
#   y luego define dentro de ella toda la estructura de tablas, tipos de datos personalizados
#   e índices de optimización.
#
# Funcionamiento:
#   El script es NO-INTERACTIVO. Lee toda su configuración (credenciales, nombres, parámetros)
#   de "variables de entorno", lo que lo hace ideal para la automatización en Docker.
# =================================================================================================

# --- 1. IMPORTACIÓN DE MÓDULOS ESENCIALES ---

import os
import sys
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_DEFAULT
from typing import Dict, Any


# --- 2. CONFIGURACIÓN INICIAL ---

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s'
)


# --- 3. CARGADOR Y VALIDADOR DE CONFIGURACIÓN ---

def load_and_validate_config():
    """
    Lee todas las variables de entorno necesarias, las valida y las convierte a los tipos de datos correctos.
    Si alguna configuración esencial falta o es inválida, el script se detiene con un error fatal.
    """
    config: Dict[str, Any] = {
        'db_name': os.getenv('DB_NAME'),
        'db_user': os.getenv('DB_USER'),
        'db_password': os.getenv('DB_PASSWORD'),
        'db_host': os.getenv('DB_HOST'),
        'db_port': os.getenv('DB_PORT'),
        'num_columns': os.getenv('NUM_COLUMNS'),
        'p_max': os.getenv('P_MAX')
    }

    missing_vars = [key for key, value in config.items() if not value]
    if missing_vars:
        logging.critical(f"FATAL: Faltan las siguientes variables de entorno: {', '.join(missing_vars)}")
        sys.exit(1)

    try:
        config['num_columns'] = int(config['num_columns'])
        config['p_max'] = float(config['p_max'])
        if config['num_columns'] <= 0:
            raise ValueError("NUM_COLUMNS debe ser un entero positivo.")
    except (ValueError, TypeError) as e:
        logging.critical(f"FATAL: Variable de entorno inválida. {e}")
        sys.exit(1)

    logging.info("Configuración cargada y validada exitosamente desde el entorno.")
    return config


# --- 4. CLASES DE GESTIÓN DE LA BASE DE DATOS ---

class DatabaseManager:
    """Gestiona la conexión con PostgreSQL y la ejecución de consultas."""
    def __init__(self, config, connect_to_postgres_db=False):
        self.db_name = config['db_name']
        db_to_connect = "postgres" if connect_to_postgres_db else self.db_name
        try:
            self.conn = psycopg2.connect(
                dbname=db_to_connect,
                user=config['db_user'],
                password=config['db_password'],
                host=config['db_host'],
                port=config['db_port']
            )
            self.cursor = self.conn.cursor()
            logging.info(f"Conectado exitosamente a la base de datos '{db_to_connect}'.")
        except psycopg2.OperationalError as e:
            logging.critical(f"FATAL: No se pudo conectar a PostgreSQL. Verifique credenciales y que el servicio esté activo.\nDetalles: {e}")
            raise e

    def create_database(self):
        """Crea la base de datos de la aplicación si no existe."""
        self.cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (self.db_name,))
        if self.cursor.fetchone():
            logging.warning(f"La base de datos '{self.db_name}' ya existe. Omitiendo creación.")
            return
        try:
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor.execute(f'CREATE DATABASE "{self.db_name}";')
            logging.info(f"Base de datos '{self.db_name}' creada exitosamente.")
        except Exception as e:
            logging.error(f"No se pudo crear la base de datos '{self.db_name}': {e}")
            raise e
        finally:
            self.conn.set_isolation_level(ISOLATION_LEVEL_DEFAULT)

    def execute_queries(self, queries):
        """Ejecuta una lista de consultas SQL en una transacción."""
        try:
            for query in queries:
                if query and query.strip():
                    self.cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Una consulta falló. Realizando rollback. Detalles: {e}")
            raise e

    def close(self):
        """Cierra el cursor y la conexión a la base de datos."""
        if self.cursor: self.cursor.close()
        if self.conn: self.conn.close()
        logging.info("Conexión a la base de datos cerrada.")

class SchemaBuilder:
    """Construye el esquema completo de la base de datos (tablas, tipos, índices)."""
    def __init__(self, db_manager: DatabaseManager, num_columns: int):
        self.db = db_manager
        self.num_columns = num_columns

    def generate_columns_sql(self, use_timestamp_col=False):
        """Genera dinámicamente la definición para las columnas de datos (col_1, col_2, etc.)."""
        numeric_type = "NUMERIC(26, 13)"
        timestamp_column_name = "timestamp_col" if use_timestamp_col else "timestamp"
        not_null_constraint = " NOT NULL" if not use_timestamp_col else ""
        
        columns_sql = [f"{timestamp_column_name} TIMESTAMPTZ{not_null_constraint}"]
        columns_sql.extend([f"col_{i} {numeric_type}" for i in range(1, self.num_columns + 1)])
        return ",\n    ".join(columns_sql)

    def create_schema(self):
        """Contiene y ejecuta la definición completa de la estructura de la base de datos."""
        base_columns = self.generate_columns_sql()
        
        queries = [
            # --- Definición de Tipos Personalizados (ENUMs) ---
            """DO $$ BEGIN CREATE TYPE data_status AS ENUM ('pending', 'processing', 'success', 'error'); EXCEPTION WHEN duplicate_object THEN null; END $$;""",
            """DO $$ BEGIN CREATE TYPE rule_type_enum AS ENUM ('not_null', 'range', 'NOT_POSITIVE_IN_RANGE'); EXCEPTION WHEN duplicate_object THEN null; END $$;""",
            
            # --- Definición de Tablas ---
            f"""CREATE TABLE IF NOT EXISTS raw_data (
                id BIGSERIAL PRIMARY KEY, {base_columns}, status data_status NOT NULL DEFAULT 'pending',
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, processed_at TIMESTAMPTZ
            );""",
            
            """CREATE TABLE IF NOT EXISTS validation_rules (
                id SERIAL PRIMARY KEY, column_name VARCHAR(100) NOT NULL, rule_type rule_type_enum NOT NULL,
                rule_config JSONB, error_message VARCHAR(255) NOT NULL, is_active BOOLEAN NOT NULL DEFAULT TRUE,
                UNIQUE (column_name, rule_type)
            );""",
            
            f"""CREATE TABLE IF NOT EXISTS validated_data (
                id BIGSERIAL PRIMARY KEY, raw_data_id BIGINT UNIQUE NOT NULL, {base_columns},
                status data_status NOT NULL DEFAULT 'pending', created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMPTZ, CONSTRAINT fk_validated_raw FOREIGN KEY (raw_data_id) REFERENCES raw_data(id) ON DELETE CASCADE
            );""",
            
            """CREATE TABLE IF NOT EXISTS validated_data_by_rules (
                id BIGSERIAL PRIMARY KEY, validated_data_id BIGINT NOT NULL, rule_id INT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT fk_vdr_validated FOREIGN KEY (validated_data_id) REFERENCES validated_data(id) ON DELETE CASCADE,
                CONSTRAINT fk_vdr_rules FOREIGN KEY (rule_id) REFERENCES validation_rules(id) ON DELETE CASCADE,
                UNIQUE (validated_data_id, rule_id)
            );""",
            
            """CREATE TABLE IF NOT EXISTS validation_error_by_rules (
                id BIGSERIAL PRIMARY KEY, raw_data_id BIGINT NOT NULL, validation_rule_id INT,
                offending_column VARCHAR(255), offending_value TEXT, created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT fk_error_raw FOREIGN KEY (raw_data_id) REFERENCES raw_data(id) ON DELETE CASCADE,
                CONSTRAINT fk_error_rule FOREIGN KEY (validation_rule_id) REFERENCES validation_rules(id) ON DELETE SET NULL
            );""",

            f"""CREATE TABLE IF NOT EXISTS duplicated_data (
                id BIGSERIAL PRIMARY KEY, raw_data_id BIGINT, {self.generate_columns_sql(use_timestamp_col=True)},
                detected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT fk_duplicate_raw FOREIGN KEY (raw_data_id) REFERENCES raw_data(id) ON DELETE SET NULL 
            );""",
            
            """CREATE TABLE IF NOT EXISTS excluded_data (
                id BIGSERIAL PRIMARY KEY, fecha_hora TIMESTAMP WITHOUT TIME ZONE NOT NULL, potencia_pico_kw NUMERIC(10, 2),
                exclusion INTEGER, motivo TEXT, anio INTEGER, mes INTEGER, dia INTEGER, hora VARCHAR(10), minuto INTEGER,
                segundo INTEGER, CONSTRAINT fecha_hora_unique UNIQUE (fecha_hora)
            );""",

            """CREATE TABLE IF NOT EXISTS excluded_data_logs (
                log_id BIGSERIAL PRIMARY KEY, excluded_data_id BIGINT, operation_type VARCHAR(10) NOT NULL,
                old_values JSONB, new_values JSONB, changed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                changed_by VARCHAR(100),
                CONSTRAINT fk_log_excluded_data FOREIGN KEY (excluded_data_id) REFERENCES excluded_data(id) ON DELETE SET NULL
            );""",
            
            """CREATE TABLE IF NOT EXISTS plant_parameters (
                id SERIAL PRIMARY KEY, p_nom NUMERIC NOT NULL, p_max NUMERIC NOT NULL,
                q_min NUMERIC NOT NULL, q_max NUMERIC NOT NULL, modified_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );""",
            
            # --- Definición de Índices para Optimización ---
            """CREATE INDEX IF NOT EXISTS idx_raw_data_pending_status ON raw_data (id) WHERE status = 'pending';""",
            """CREATE INDEX IF NOT EXISTS idx_validated_data_timestamp ON validated_data (timestamp);""",
            """CREATE INDEX IF NOT EXISTS idx_verror_raw_data_id ON validation_error_by_rules (raw_data_id);""",
            """CREATE INDEX IF NOT EXISTS idx_verror_validation_rule_id ON validation_error_by_rules (validation_rule_id);""",
            """CREATE INDEX IF NOT EXISTS idx_vdr_validated_id ON validated_data_by_rules (validated_data_id);""",
            """CREATE INDEX IF NOT EXISTS idx_excluded_data_logs_excluded_id ON excluded_data_logs (excluded_data_id);"""
        ]
        
        logging.info("Creando/Verificando tipos, tablas e índices...")
        self.db.execute_queries(queries)
        logging.info("Esquema de la base de datos verificado/creado correctamente.")


# --- 5. FUNCIÓN PRINCIPAL DE EJECUCIÓN ---

def main():
    """Flujo principal que orquesta la creación y configuración de la base de datos."""
    db_admin = None
    db_manager = None
    try:
        config = load_and_validate_config()

        logging.info("Paso 1/4: Creando la base de datos (si no existe)...")
        db_admin = DatabaseManager(config, connect_to_postgres_db=True)
        db_admin.create_database()
        db_admin.close()

        logging.info("Paso 2/4: Conectando a la base de datos de la aplicación...")
        db_manager = DatabaseManager(config)
        
        logging.info("Paso 3/4: Construyendo el esquema (tablas, tipos, índices)...")
        schema_builder = SchemaBuilder(db_manager, config['num_columns'])
        schema_builder.create_schema()

        logging.info("Paso 4/4: Insertando parámetros iniciales de la planta...")
        p_max = config['p_max']
        params = {'p_nom': p_max, 'p_max': p_max, 'q_min': -p_max, 'q_max': p_max}
        db_manager.execute_queries([
            "TRUNCATE plant_parameters RESTART IDENTITY;",
            f"""INSERT INTO plant_parameters (p_nom, p_max, q_min, q_max)
               VALUES ({params['p_nom']}, {params['p_max']}, {params['q_min']}, {params['q_max']});"""
        ])
        logging.info(f"Parámetros de planta insertados: P_max={params['p_max']}, Q_max={params['q_max']}, etc.")

        logging.info("\n[SUCCESS] El proceso de configuración de la base de datos ha finalizado.")
    
    except Exception as e:
        logging.critical(f"\n[FATAL] El script fue interrumpido por un error: {e}")
        sys.exit(1)
    finally:
        if db_admin: db_admin.close()
        if db_manager: db_manager.close()

# --- 6. PUNTO DE ENTRADA DEL SCRIPT ---
if __name__ == "__main__":
    main()