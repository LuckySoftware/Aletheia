"""
create_database_and_tables.py

Script para crear y configurar una base de datos PostgreSQL, sus tablas,
y generar el archivo de configuración .env necesario para otros scripts.
"""

import psycopg2
from getpass import getpass
from pathlib import Path

class ConfigurationManager:
    """Gestiona la obtención, validación y guardado de la configuración del usuario."""

    @staticmethod
    def _save_config_to_env(config: dict):
        """
        Crea o sobrescribe el archivo .env con la configuración proporcionada.
        """
        env_content = [
            "# Este es el archivo .env (aqui van todas las credenciales de la base de datos)",
            "# Nunca compartir estas credenciales",
            f"DB_NAME={config['db_name']}",
            f"DB_USER={config['db_user']}",
            f"DB_PASSWORD={config['db_password']}",
            f"DB_HOST=localhost",
            f"DB_PORT={config['db_port']}",
            "# Ruta del directorio que contiene los archivos CSV a analizar",
            f"CSV_DIRECTORY={config['csv_directory'].replace('\\', '\\\\')}",
            "# Ruta del directorio para guardar los archivos de Excel con errores",
            f"ERRORS_OUTPUT_DIR={config['errors_output_dir'].replace('\\', '\\\\')}",
            "# Ruta del directorio para guardar los archivos de Excel validados",
            f"VALIDATED_OUTPUT_DIR={config['validated_output_dir'].replace('\\', '\\\\')}",
            "# Ruta del archivo Excel con datos de exclusiones",
            f"EXCLUSIONES_FOLDER_PATH={config['exclusions_folder_path'].replace('\\', '\\\\')}"
        ]
        
        env_file_path = "../.env"

        try:
            with open(env_file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(env_content) + "\n")
            print(f"\n[INFO] Configuración guardada exitosamente en el archivo: {env_file_path}")
        except IOError as e:
            print(f"\n[ERROR] No se pudo escribir en el archivo {env_file_path}: {e}")

    @staticmethod
    def get_user_configuration():
        """
        Solicita interactivamente al usuario toda la información necesaria.
        """
        config = {}
        print("--- Configuración de la Base de Datos y Entorno ---")
        
        config['db_name'] = input("1. Ingrese el nombre de la base de datos a crear: ").strip()
        while not config['db_name']:
            print("[ERROR] El nombre de la base de datos no puede estar vacío.")
            config['db_name'] = input("1. Ingrese el nombre de la base de datos a crear: ").strip()

        config['db_user'] = input("2. Ingrese el usuario de PostgreSQL (debe ser superusuario para crear BD): ").strip()
        while not config['db_user']:
            print("[ERROR] El usuario no puede estar vacío.")
            config['db_user'] = input("2. Ingrese el usuario de PostgreSQL: ").strip()

        config['db_password'] = getpass("3. Ingrese la contraseña para el usuario (la entrada estará oculta): ")
        
        while True:
            try:
                port = input("4. Ingrese el puerto de PostgreSQL (ej: 5432): ").strip()
                config['db_port'] = int(port)
                if not (1 <= config['db_port'] <= 65535): raise ValueError("Puerto fuera de rango")
                break
            except ValueError as e:
                print(f"[ERROR] Ingrese un número de puerto válido: {e}")
        
        while True:
            try:
                columns = input("5. Ingrese el número de columnas de datos (ej: 22 para col_1 a col_22): ").strip()
                config['num_columns'] = int(columns)
                if config['num_columns'] <= 0: raise ValueError("Debe ser un entero positivo")
                break
            except ValueError as e:
                print(f"[ERROR] Entrada inválida: {e}")

        while True:
            path_str = input("6. Ingrese la ruta completa al directorio de los CSVs: ").strip()
            path = Path(path_str)
            if path.exists() and path.is_dir():
                config['csv_directory'] = str(path)
                break
            print(f"[ERROR] La ruta '{path_str}' no existe o no es un directorio válido.")

        while True:
            path_str = input("7. Ingrese la ruta para guardar los excels con errores (ERRORS_OUTPUT_DIR): ").strip()
            path = Path(path_str)
            if path.exists() and path.is_dir():
                config['errors_output_dir'] = str(path)
                break
            print(f"[ERROR] La ruta '{path_str}' no existe o no es un directorio válido.")

        while True:
            path_str = input("8. Ingrese la ruta para guardar los excels validados (VALIDATED_OUTPUT_DIR): ").strip()
            path = Path(path_str)
            if path.exists() and path.is_dir():
                config['validated_output_dir'] = str(path)
                break
            print(f"[ERROR] La ruta '{path_str}' no existe o no es un directorio válido.")
            
        while True:
            # Se ajusta el texto para pedir un directorio
            path_str = input("9. Ingrese la ruta al directorio de exclusiones (EXCLUSIONES_FOLDER_PATH): ").strip()
            path = Path(path_str)
            # Se valida que la ruta exista y que sea un directorio
            if path.exists() and path.is_dir():
                config['exclusions_folder_path'] = str(path)
                break
            # Se ajusta el mensaje de error
            print(f"[ERROR] La ruta '{path_str}' no existe o no es un directorio válido.")
            
        ConfigurationManager._save_config_to_env(config)

        print("\n[INFO] Configuración recibida. Procediendo a crear la base de datos...")
        return config


class DatabaseManager:
    """Clase responsable de manejar la conexión con PostgreSQL y ejecutar instrucciones SQL."""
    def __init__(self, config, connect_to_postgres_db=False):
        self.db_name = config['db_name']
        self.user = config['db_user']
        self.password = config['db_password']
        self.host = 'localhost'
        self.port = config['db_port']
        db_to_connect = "postgres" if connect_to_postgres_db else self.db_name
        try:
            self.conn = psycopg2.connect(
                dbname=db_to_connect, user=self.user, password=self.password,
                host=self.host, port=self.port
            )
            self.cursor = self.conn.cursor()
            print(f"[INFO] Conectado exitosamente a la base de datos '{db_to_connect}'.")
        except psycopg2.OperationalError as e:
            raise Exception(f"[ERROR FATAL] No se pudo conectar a PostgreSQL. Verifique credenciales.\nDetalle: {e}")

    def create_database(self):
        """Crea la base de datos principal si no existe."""
        self.cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (self.db_name,))
        if self.cursor.fetchone():
            print(f"[INFO] La base de datos '{self.db_name}' ya existe. Omitiendo creación.")
            return
        try:
            self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor.execute(f'CREATE DATABASE "{self.db_name}";')
            print(f"[INFO] Base de datos '{self.db_name}' creada exitosamente.")
        except Exception as e:
            raise Exception(f"[ERROR] No se pudo crear la base de datos '{self.db_name}': {e}")
        finally:
            self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_DEFAULT)

    def execute_queries(self, queries):
        """Ejecuta una lista de queries SQL."""
        try:
            for query in queries:
                if query and query.strip():
                    self.cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"[ERROR] Una consulta falló. Haciendo rollback. Detalle: {e}")

    def close(self):
        """Cierra la conexión y el cursor."""
        if self.cursor: self.cursor.close()
        if self.conn: self.conn.close()
        print("[INFO] Conexión a la base de datos cerrada.")


class SchemaBuilder:
    """Construye el esquema profesional de la base de datos dinámicamente."""
    def __init__(self, db_manager: DatabaseManager, num_columns: int):
        self.db = db_manager
        self.num_columns = num_columns

    def generate_columns_sql(self):
        """Genera dinámicamente las definiciones de las columnas 'col_N'."""
        numeric_type = "NUMERIC(26, 13)"
        columns_sql = ["timestamp_col TIMESTAMPTZ NOT NULL"]
        columns_sql.extend([f"col_{i} {numeric_type}" for i in range(1, self.num_columns + 1)])
        return ",\n    ".join(columns_sql)

    def create_schema(self):
        """Crea todos los tipos, tablas e índices necesarios."""
        base_columns = self.generate_columns_sql()

        type_creation_queries = [
            """DO $$ BEGIN CREATE TYPE data_status AS ENUM ('pending', 'processing', 'success', 'error'); EXCEPTION WHEN duplicate_object THEN null; END $$;""",
            """DO $$ BEGIN CREATE TYPE rule_type_enum AS ENUM ('not_null', 'range', 'NOT_POSITIVE_IN_RANGE'); EXCEPTION WHEN duplicate_object THEN null; END $$;"""
        ]
        
        raw_data_sql = f"CREATE TABLE IF NOT EXISTS Raw_Data (raw_id BIGSERIAL PRIMARY KEY, {base_columns}, status data_status NOT NULL DEFAULT 'pending', created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, processed_at TIMESTAMPTZ);"
        validation_rules_sql = "CREATE TABLE IF NOT EXISTS Validation_Rules (rule_id SERIAL PRIMARY KEY, column_name VARCHAR(100) NOT NULL, rule_type rule_type_enum NOT NULL, rule_config JSONB, error_message VARCHAR(255) NOT NULL, is_active BOOLEAN NOT NULL DEFAULT TRUE, UNIQUE (column_name, rule_type));"
        validated_data_sql = f"CREATE TABLE IF NOT EXISTS Validated_Data (raw_id BIGINT PRIMARY KEY, {base_columns}, validated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, CONSTRAINT fk_validated_raw FOREIGN KEY (raw_id) REFERENCES Raw_Data(raw_id) ON DELETE CASCADE);"
        
        error_data_sql = "CREATE TABLE IF NOT EXISTS Error_Data (error_id BIGSERIAL PRIMARY KEY, raw_id BIGINT NOT NULL, rule_id INT, offending_column VARCHAR(255), offending_value TEXT, created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, CONSTRAINT fk_error_raw FOREIGN KEY (raw_id) REFERENCES Raw_Data(raw_id) ON DELETE CASCADE, CONSTRAINT fk_error_rule FOREIGN KEY (rule_id) REFERENCES Validation_Rules(rule_id) ON DELETE CASCADE);"
        
        duplicated_data_sql = f"""
        CREATE TABLE IF NOT EXISTS Duplicated_Data (
            duplicate_id BIGSERIAL PRIMARY KEY,
            raw_id BIGINT, 
            {base_columns},
            detected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_duplicate_raw FOREIGN KEY (raw_id)
                REFERENCES Raw_Data(raw_id)
                ON DELETE SET NULL 
        );
        """
        
        # --- BLOQUE MODIFICADO ---
        excluded_data_sql = """
        CREATE TABLE IF NOT EXISTS excluded_data (
            excluded_id BIGSERIAL PRIMARY KEY,
            fecha_hora TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            potencia_pico_kw NUMERIC(10, 2),
            exclusion INTEGER,
            motivo TEXT,
            anio INTEGER,
            mes INTEGER,
            dia INTEGER,
            hora VARCHAR(10),
            minuto INTEGER,
            segundo INTEGER,
            CONSTRAINT fecha_hora_unique UNIQUE (fecha_hora)
        );
        """
        
        indexes_sql = "CREATE INDEX IF NOT EXISTS idx_raw_data_pending_status ON Raw_Data (raw_id) WHERE status = 'pending'; CREATE INDEX IF NOT EXISTS idx_validated_data_timestamp_col ON Validated_Data (timestamp_col); CREATE INDEX IF NOT EXISTS idx_error_data_raw_id ON Error_Data (raw_id); CREATE INDEX IF NOT EXISTS idx_error_data_rule_id ON Error_Data (rule_id);"
        
        print("[INFO] Creando tipos de datos personalizados (ENUMs)...")
        self.db.execute_queries(type_creation_queries)

        print("[INFO] Creando tablas...")
        all_tables = [raw_data_sql, validation_rules_sql, validated_data_sql, error_data_sql, duplicated_data_sql, excluded_data_sql]
        self.db.execute_queries(all_tables)

        print("[INFO] Creando índices para optimizar el rendimiento...")
        self.db.execute_queries(indexes_sql.split(';'))
        
        print("[INFO] Esquema de base de datos creado exitosamente.")

def main():
    """Flujo principal del script."""
    db_admin = None
    db_manager = None
    try:
        config = ConfigurationManager.get_user_configuration()

        db_admin = DatabaseManager(config, connect_to_postgres_db=True)
        db_admin.create_database()
        db_admin.close()

        db_manager = DatabaseManager(config)
        schema_builder = SchemaBuilder(db_manager, config['num_columns'])
        schema_builder.create_schema()

        print("\n[ÉXITO] El proceso de configuración de la base de datos ha finalizado.")
        
    except Exception as e:
        print(f"\n[ERROR FATAL] El script se interrumpió: {e}")
        return 1
    finally:
        if db_admin: db_admin.close()
        if db_manager: db_manager.close()
    return 0

if __name__ == "__main__":
    exit(main())