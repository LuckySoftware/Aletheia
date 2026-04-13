import logging
import re
import unicodedata
from pathlib import Path
from typing import List
from src.core.database import DatabaseManager
from src.models.plant import Plant

class ColumnNameProcessor:
    MAX_COLUMN_NAME_LENGTH = 63
    
    @staticmethod
    def sanitize_column_name(raw_name: str) -> str:
        name = raw_name.lower()
        name = re.sub(r'\[.*?\]', '', name)
        name = unicodedata.normalize('NFD', name)
        name = ''.join(char for char in name if unicodedata.category(char) != 'Mn')
        
        replacements = {'ñ': 'n', '°': 'deg', '²': '2', '³': '3', 'º': 'deg'}
        for old_char, new_char in replacements.items():
            name = name.replace(old_char, new_char)
        
        name = re.sub(r'[^a-z0-9_]+', '_', name)
        name = re.sub(r'_+', '_', name)
        name = name.strip('_')
        
        if not name:
             name = f"col_raw_{raw_name[:10].replace(' ','')}"
        if name and name[0].isdigit():
            name = 'col_' + name
            
        if len(name) > ColumnNameProcessor.MAX_COLUMN_NAME_LENGTH:
            name = name[:ColumnNameProcessor.MAX_COLUMN_NAME_LENGTH].rstrip('_')
        
        return name
    
    @staticmethod
    def get_columns_from_plant_input(plant: Plant, delimiter: str = ';') -> List[str]:
        """Busca el CSV en la carpeta input de la planta y extrae las columnas"""
        dir_path = Path(plant.input_path)
        if not dir_path.exists():
            raise FileNotFoundError(f"Carpeta no existe: {dir_path}")
        
        csv_files = list(dir_path.glob('*.csv'))
        if not csv_files:
            raise FileNotFoundError(f"No hay CSV de referencia en: {dir_path}")
        
        csv_path = csv_files[0]
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                header_line = f.readline().strip()
        except UnicodeDecodeError:
            with open(csv_path, 'r', encoding='latin-1') as f:
                header_line = f.readline().strip()

        raw_column_names = header_line.split(delimiter)
        data_column_names = raw_column_names[1:] # Saltar la primera (asumiendo que es timestamp)
        return [ColumnNameProcessor.sanitize_column_name(n) for n in data_column_names]


class SchemaBuilder:
    def __init__(self, db_manager: DatabaseManager, column_names: List[str]):
        self.db = db_manager
        self.column_names = column_names

    def generate_columns_sql(self, use_timestamp_col: bool = False) -> str:
        numeric_type = "NUMERIC(26, 13)"
        timestamp_column_name = "timestamp_col" if use_timestamp_col else "timestamp"
        not_null = " NOT NULL" if not use_timestamp_col else ""
        columns_sql = [f"{timestamp_column_name} TIMESTAMPTZ{not_null}"]
        for col_name in self.column_names:
            columns_sql.append(f'"{col_name}" {numeric_type}')
        return ",\n    ".join(columns_sql)

    def build(self):
        """Ejecuta todos los queries de creación de tablas"""
        base_columns = self.generate_columns_sql()
        duplicated_columns = self.generate_columns_sql(use_timestamp_col=True)
        
        # Aquí van exactamente todos tus CREATE TABLE strings que tenías en 1_create_database.py
        # Por brevedad en esta respuesta, asume que están aquí. (Copia y pega la lista de queries)
        queries = [
            """DO $$BEGIN CREATE TYPE data_status AS ENUM ('pending', 'processing', 'success', 'error', 'waived'); EXCEPTION WHEN duplicate_object THEN null; END$$;""",
            """DO $$BEGIN CREATE TYPE rule_type_enum AS ENUM ('not_null', 'range', 'enum', 'NOT_POSITIVE_IN_RANGE'); EXCEPTION WHEN duplicate_object THEN null; END$$;""",
            """CREATE TABLE IF NOT EXISTS parametros_planta (id SERIAL PRIMARY KEY, potencia_pico_kw NUMERIC(12,2) NOT NULL, created_at TIMESTAMP DEFAULT NOW());""",
            f"""CREATE TABLE IF NOT EXISTS raw_data (id BIGSERIAL PRIMARY KEY, {base_columns}, status data_status NOT NULL DEFAULT 'pending', created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, processed_at TIMESTAMPTZ);""",
            """CREATE TABLE IF NOT EXISTS load_control (inventory_date DATE PRIMARY KEY, min_time TIME, max_time TIME, row_count INTEGER DEFAULT 0, last_updated TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP);""",
            """CREATE TABLE IF NOT EXISTS validation_rules (id SERIAL PRIMARY KEY, column_pattern VARCHAR(100) NOT NULL, rule_type rule_type_enum NOT NULL, rule_config JSONB, error_message VARCHAR(255) NOT NULL, is_active BOOLEAN NOT NULL DEFAULT TRUE, UNIQUE (column_pattern, rule_type));""",
            f"""CREATE TABLE IF NOT EXISTS validated_data (id BIGSERIAL PRIMARY KEY, raw_data_id BIGINT UNIQUE NOT NULL, {base_columns}, status data_status NOT NULL DEFAULT 'pending', created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, processed_at TIMESTAMPTZ, CONSTRAINT fk_validated_raw FOREIGN KEY (raw_data_id) REFERENCES raw_data(id) ON DELETE CASCADE);""",
            """CREATE TABLE IF NOT EXISTS validated_data_by_rules (id BIGSERIAL PRIMARY KEY, validated_data_id BIGINT NOT NULL, rule_id INT NOT NULL, created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, CONSTRAINT fk_vdr_validated FOREIGN KEY (validated_data_id) REFERENCES validated_data(id) ON DELETE CASCADE, CONSTRAINT fk_vdr_rules FOREIGN KEY (rule_id) REFERENCES validation_rules(id) ON DELETE CASCADE, UNIQUE (validated_data_id, rule_id));""",
            """CREATE TABLE IF NOT EXISTS validation_error_by_rules (id BIGSERIAL PRIMARY KEY, raw_data_id BIGINT NOT NULL, validation_rule_id INT, offending_column VARCHAR(255), offending_value TEXT, error_type VARCHAR(20) DEFAULT 'error' CHECK (error_type IN ('error', 'alarm')), created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, CONSTRAINT fk_error_raw FOREIGN KEY (raw_data_id) REFERENCES raw_data(id) ON DELETE CASCADE, CONSTRAINT fk_error_rule FOREIGN KEY (validation_rule_id) REFERENCES validation_rules(id) ON DELETE SET NULL);""",
            f"""CREATE TABLE IF NOT EXISTS duplicated_data (id BIGSERIAL PRIMARY KEY, raw_data_id BIGINT, {duplicated_columns}, detected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, CONSTRAINT fk_duplicate_raw FOREIGN KEY (raw_data_id) REFERENCES raw_data(id) ON DELETE SET NULL);""",
            """CREATE TABLE IF NOT EXISTS excluded_data (id BIGSERIAL PRIMARY KEY, form_timestamp TIMESTAMPTZ NOT NULL UNIQUE, exclusion_start TIMESTAMPTZ NOT NULL, exclusion_end TIMESTAMPTZ NOT NULL, exclusion_type VARCHAR(100), exclusion INTEGER NOT NULL, motivo TEXT, observacion TEXT, potencia_pico_kw NUMERIC(10, 2), excluded_variables TEXT, created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP);""",
            """CREATE TABLE IF NOT EXISTS excluded_data_logs (log_id BIGSERIAL PRIMARY KEY, excluded_data_id BIGINT, deleted_id BIGINT, operation_type VARCHAR(50), changed_by VARCHAR(100), created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, CONSTRAINT fk_log_excluded_data FOREIGN KEY (excluded_data_id) REFERENCES excluded_data(id) ON DELETE SET NULL);""",
            """CREATE OR REPLACE VIEW public.view_potencia_pico_kw_ultimo_registro AS SELECT CURRENT_TIMESTAMP AS fecha_consulta, COALESCE(override_data.potencia_pico_kw, planta.potencia_pico_kw) AS potencia_activa_kw, COALESCE(override_data.form_timestamp, planta.created_at)::timestamp WITHOUT TIME ZONE AS modified_at, CASE WHEN override_data.id IS NOT NULL THEN 'EXCLUSION/MODIFICACION (' || override_data.exclusion_type || ')' ELSE 'PARAMETRO_BASE' END AS origen_dato, override_data.id as id_registro_tomado FROM (SELECT potencia_pico_kw, created_at FROM public.parametros_planta ORDER BY id DESC LIMIT 1) planta LEFT JOIN LATERAL (SELECT potencia_pico_kw, id, exclusion_type, form_timestamp FROM public.excluded_data WHERE exclusion_start <= CURRENT_TIMESTAMP AND (exclusion_end > CURRENT_TIMESTAMP OR exclusion_type LIKE 'Modificación%' OR exclusion_start = exclusion_end) ORDER BY form_timestamp DESC LIMIT 1) override_data ON TRUE;""",
            """CREATE INDEX IF NOT EXISTS idx_raw_data_pending_status ON raw_data (id) WHERE status = 'pending';""",
            """CREATE INDEX IF NOT EXISTS idx_validated_data_timestamp ON validated_data (timestamp);""",
            """CREATE INDEX IF NOT EXISTS idx_load_control_date ON load_control (inventory_date);"""
        ]
        
        logging.info(f">>> Creando esquema para {self.db.plant.name}...")
        self.db.execute_queries(queries)
        logging.info(f"Esquema completado para {self.db.plant.name}.")

# -------------------------------------------------------------------
# BLOQUE DE EJECUCIÓN INDEPENDIENTE (Para correrlo cuando tú quieras)
# -------------------------------------------------------------------
if __name__ == "__main__":
    from src.core.config_loader import ConfigLoader
    
    # 1. Cargar configuraciones
    loader = ConfigLoader()
    
    # 2. Preguntar a qué planta se le aplicará (útil para uso manual)
    print("Plantas disponibles:")
    for i, p in enumerate(loader.plants):
        print(f"[{i}] {p.name}")
        
    seleccion = int(input("\nIngresa el número de la planta para inicializar su BD: "))
    planta_elegida = loader.plants[seleccion]
    
    # 3. Flujo de creación
    try:
        # A. Crear la BD conectándonos al host maestro ('postgres')
        db_admin = DatabaseManager(planta_elegida, connect_to_postgres_db=True)
        db_admin.create_database()
        db_admin.close()

        # B. Extraer columnas del CSV de input
        columnas = ColumnNameProcessor.get_columns_from_plant_input(planta_elegida)
        
        # C. Conectarse a la nueva BD y construir tablas
        db_planta = DatabaseManager(planta_elegida)
        builder = SchemaBuilder(db_planta, columnas)
        builder.build()
        db_planta.close()
        
        print(f"\nBase de datos {planta_elegida.db_name} lista y configurada.")
    except Exception as e:
        print(f"Error fatal: {e}")