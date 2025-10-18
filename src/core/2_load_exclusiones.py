import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import sys
import os
import pandas as pd
from dotenv import load_dotenv
import logging
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# --- LOGGING CONFIGURATION ---
def setup_logging():
    """Configures the logging system to record only in the console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info("Logging configured for terminal output.")

# --- PATH AND ENVIRONMENT CONFIGURATION ---
PROJECT_ROOT = "/app"
ENV_PATH = os.path.join(PROJECT_ROOT, '.env')

setup_logging()

if os.path.exists(ENV_PATH):
    load_dotenv(dotenv_path=ENV_PATH)
    logging.info(".env file loaded successfully.")
else:
    logging.error(f"Could not find the .env file at the expected path: {ENV_PATH}")
    sys.exit(1)

# --- DATABASE CONNECTION CONFIGURATION ---
DB_CONFIG = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT")
}

if not all(DB_CONFIG.values()):
    logging.error("Missing database environment variables. Check your .env file.")
    sys.exit(1)

# --- GOOGLE SHEETS CONFIGURATION ---
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME")
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME")
CREDENTIALS_FILE = os.environ.get("GOOGLE_CREDENTIALS_FILE")

if not all([GOOGLE_SHEET_NAME, WORKSHEET_NAME, CREDENTIALS_FILE]):
    logging.error("Missing Google Sheets environment variables. Check your .env file.")
    sys.exit(1)

CREDENTIALS_PATH = os.path.join(PROJECT_ROOT, CREDENTIALS_FILE) # type: ignore -- never returns None

def get_data_from_google_sheet():
    """
    Connects to Google Sheets and fetches data into a pandas DataFrame.
    """
    if not GOOGLE_SHEET_NAME:
        logging.error("The GOOGLE_SHEET_NAME environment variable is not set. Please check your .env file.")
        return None
        
    logging.info(f"Attempting to read '{WORKSHEET_NAME}' from Google Sheet '{GOOGLE_SHEET_NAME}'.")
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
        client = gspread.authorize(creds)
        
        spreadsheet = client.open(GOOGLE_SHEET_NAME)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME) # type: ignore -- never returns None
        
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        
        logging.info(f"Successfully fetched {len(df)} rows from Google Sheets.")
        return df
    except Exception:
        logging.exception("An unexpected error occurred while fetching data from Google Sheets.")
        return None

def prepare_data_for_db(df):
    """
    Transforms the Google Sheet data by expanding date ranges into individual
    second-by-second timestamps for database insertion.
    """
    if df is None or df.empty:
        logging.warning("Input DataFrame is empty or None. Skipping data preparation.")
        return None
    
    # 1. Define a mapping for your long Spanish column names to simple, clean names.
    column_mapping = {
        'Seleccione la fecha exacta de inicio de la exclusión:': 'start_date',
        'Seleccione la hora exacta de inicio de la exclusión:': 'start_time',
        'Seleccione la fecha exacta de finalización de la exclusión:': 'end_date',
        'Seleccione la hora exacta de finalización de la exclusión:': 'end_time',
        'Seleccione el 0 para marcar la exclusión': 'exclusion',
        'Escriba el motivo de la exclusión:': 'motivo',
        'Escriba la potencia pico:': 'potencia_pico_kw'
    }
    df.rename(columns=column_mapping, inplace=True)

    all_records = []
    logging.info("Starting transformation of date ranges into individual records...")

    # 2. Iterate through each row of the DataFrame.
    for index, row in df.iterrows():
        try:
            # 3. Combine date and time strings to create full start and end timestamps.
            start_ts = pd.to_datetime(f"{row['start_date']} {row['start_time']}")
            end_ts = pd.to_datetime(f"{row['end_date']} {row['end_time']}")

            # 4. Generate a date range for every second between start and end.
            date_range = pd.date_range(start=start_ts, end=end_ts, freq='s') # <-- THIS IS THE CHANGE

            # 5. For each generated timestamp, create a database-ready record.
            for timestamp in date_range:
                record = (
                    timestamp,                                        # fecha_hora
                    pd.to_numeric(row['potencia_pico_kw'], errors='coerce'), # potencia_pico_kw
                    pd.to_numeric(row['exclusion'], errors='coerce'), # exclusion
                    row['motivo'],                                    # motivo
                    timestamp.year,                                   # anio
                    timestamp.month,                                  # mes
                    timestamp.day,                                    # dia
                    timestamp.hour,                                   # hora
                    timestamp.minute,                                 # minuto
                    timestamp.second,                                 # segundo
                )
                all_records.append(record)
        
        except (ValueError, TypeError) as e:
            logging.warning(f"Skipping row {index + 2} in Google Sheet due to invalid data (e.g., bad date/time format): {e}")
            continue

    logging.info(f"Transformation complete. Generated {len(all_records)} records to be inserted/updated.")
    return all_records

def main():
    """
    Main function to orchestrate the entire process.
    """
    logging.info("--- STARTING EXCLUSION DATA LOAD PROCESS ---")
    
    exclusions_df = get_data_from_google_sheet()
    
    if exclusions_df is None or exclusions_df.empty:
        logging.info("No data fetched from Google Sheets. Ending the script.")
        return

    prepared_records = prepare_data_for_db(exclusions_df)

    if not prepared_records:
        logging.warning("Data preparation failed or resulted in no records. Ending the script.")
        return
        
    conn = None
    try:
        logging.info("Establishing connection to the database...")
        conn = psycopg2.connect(
            f"dbname={DB_CONFIG['dbname']} user={DB_CONFIG['user']} password={DB_CONFIG['password']} "
            f"host={DB_CONFIG['host']} port={DB_CONFIG['port']}"
        )
        logging.info("Database connection successful.")

        with conn.cursor() as cur:
            table_name = "excluded_data"
            columns = [
                "fecha_hora", "potencia_pico_kw", "exclusion", "motivo", 
                "anio", "mes", "dia", "hora", "minuto", "segundo"
            ]
            
            query = sql.SQL("""
                INSERT INTO {table} ({fields}) VALUES %s
                ON CONFLICT (fecha_hora) DO UPDATE SET
                potencia_pico_kw = EXCLUDED.potencia_pico_kw,
                exclusion = EXCLUDED.exclusion,
                motivo = EXCLUDED.motivo,
                anio = EXCLUDED.anio,
                mes = EXCLUDED.mes,
                dia = EXCLUDED.dia,
                hora = EXCLUDED.hora,
                minuto = EXCLUDED.minuto,
                segundo = EXCLUDED.segundo
            """).format(
                table=sql.Identifier(table_name),
                fields=sql.SQL(', ').join(map(sql.Identifier, columns))
            )
            
            execute_values(cur, query, prepared_records, page_size=1000)
            logging.info(f"-> Processed {cur.rowcount} rows into the database.")

        conn.commit()
        logging.info("--- GLOBAL TRANSACTION COMMITTED ---")

    except psycopg2.Error:
        logging.exception("Critical database error. Rolling back all changes...")
        if conn:
            conn.rollback()
    except Exception:
        logging.exception("An unexpected error occurred during the main execution.")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    logging.info("--- PROCESS SUMMARY ---")
    logging.info(f"Total rows generated and processed: {len(prepared_records)}")
    logging.info("---------------------------\n")

if __name__ == "__main__":
    main()