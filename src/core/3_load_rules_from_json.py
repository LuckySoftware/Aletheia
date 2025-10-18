"""
3_load_rules_from_json.py
"""
import os
import json
import logging
import psycopg2
from dotenv import load_dotenv

# --- Configuración del Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Esto hace que el script funcione sin importar desde dónde se ejecute.
# 1. Obtiene la ruta del directorio donde está este script (...\src)
PROJECT_ROOT = "/app"
ENV_PATH = os.path.join(PROJECT_ROOT, '.env')


def load_rules(json_file_path: str, env_path: str):
    """
    Función principal que lee e inserta/actualiza las reglas en la base de datos.
    """
    if not os.path.exists(env_path):
        logging.error(f"FATAL: El archivo de entorno '{env_path}' no fue encontrado.")
        return
    load_dotenv(dotenv_path=env_path)
    
    db_config = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432")
    }

    if not all(db_config.values()):
        logging.error(f"FATAL: Faltan variables de base de datos en el archivo '{env_path}'.")
        return

    conn = None
    rules_processed = 0
    rules_upserted = 0
    rules_failed = 0

    try:
        logging.info(f"Leyendo archivo de reglas: '{json_file_path}'")
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            raise ValueError("El JSON de reglas debe ser una lista de objetos.")
        rules_to_load = data

        conn = psycopg2.connect(**db_config)
        logging.info(f"Conexión exitosa a la base de datos '{db_config['dbname']}'.")
        
        for rule in rules_to_load:
            rules_processed += 1
            try:
                query = """
                    INSERT INTO validation_rules (column_name, rule_type, rule_config, error_message, is_active)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (column_name, rule_type) 
                    DO UPDATE SET
                        rule_config = EXCLUDED.rule_config,
                        error_message = EXCLUDED.error_message,
                        is_active = EXCLUDED.is_active;
                """
                with conn.cursor() as cur:
                    cur.execute(query, (
                        rule["column_name"],
                        rule["rule_type"],
                        json.dumps(rule.get("rule_config")) if rule.get("rule_config") else None,
                        rule["error_message"],
                        rule.get("is_active", True)
                    ))
                rules_upserted += 1
            except Exception as e:
                logging.error(f"No se pudo procesar la regla para '{rule.get('column_name')}'. Error: {e}")
                rules_failed += 1
        
        conn.commit()

    except FileNotFoundError:
        logging.error(f"FATAL: El archivo de reglas '{json_file_path}' no fue encontrado.")
    except (json.JSONDecodeError, ValueError) as e:
        logging.error(f"FATAL: Error en el formato del archivo JSON. Detalle: {e}")
    except psycopg2.Error as e:
        logging.error(f"FATAL: Error de base de datos. Detalle: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")

    logging.info("--- Resumen de la Carga ---")
    logging.info(f"Reglas encontradas en el archivo: {rules_processed}")
    logging.info(f"Reglas insertadas/actualizadas: {rules_upserted}")
    logging.info(f"Reglas fallidas: {rules_failed}")
    logging.info("----------------------------")


if __name__ == "__main__":
    # 3. Construye las rutas completas y seguras a los archivos
    ENV_FILE_PATH = os.path.join(PROJECT_ROOT, ".env")
    JSON_FILE_PATH = os.path.join(PROJECT_ROOT, "src", "settings", "rules.json")
    
    if not os.path.exists(JSON_FILE_PATH):
        logging.error(f"El archivo de configuración '{JSON_FILE_PATH}' no fue encontrado.")
    else:
        load_rules(JSON_FILE_PATH, ENV_FILE_PATH)