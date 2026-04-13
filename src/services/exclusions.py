import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
from psycopg2.extras import execute_values
import gspread
from google.oauth2.service_account import Credentials

from src.models.plant import Plant
from src.core.database import DatabaseManager
from src.utils.helpers import ColumnNameProcessor

class ExclusionManager:
    def __init__(self, db_manager: DatabaseManager, plant: Plant, creds_path: Path):
        self.db = db_manager
        self.plant = plant
        self.creds_path = creds_path

    def sync_from_sheets(self):
        """Descarga e inserta/actualiza exclusiones desde Google Sheets (Script 2)."""
        logging.info(f"[{self.plant.name}] Sincronizando Exclusiones de Google Sheets...")
        
        # Obtenemos el nombre de la hoja desde la configuración de la planta
        gs_sheet = getattr(self.plant, 'gs_sheet', None) 
        if not gs_sheet or not self.creds_path.exists():
            logging.warning(f"[{self.plant.name}] Faltan credenciales GS o 'gs_sheet' en config. Saltando.")
            return

        try:
            scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_file(str(self.creds_path), scopes=scope)
            client = gspread.authorize(creds)
            
            worksheet = client.open(gs_sheet).sheet1
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
            
            if df.empty:
                logging.info(f"[{self.plant.name}] La hoja de exclusiones está vacía.")
                return

            # Transformación de Fechas
            df['exclusion_start'] = pd.to_datetime(pd.to_datetime(df['Seleccione la fecha exacta de inicio de la exclusión:'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d') + ' ' + df['Seleccione la hora exacta de inicio de la exclusión:'].astype(str).str.strip(), errors='coerce')
            df['exclusion_end'] = pd.to_datetime(pd.to_datetime(df['Seleccione la fecha exacta de finalización de la exclusión:'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d') + ' ' + df['Seleccione la hora exacta de finalización de la exclusión:'].astype(str).str.strip(), errors='coerce')
            df['form_timestamp'] = pd.to_datetime(df['Marca temporal'], dayfirst=True, errors='coerce')
            df.dropna(subset=['form_timestamp', 'exclusion_start', 'exclusion_end'], inplace=True)

            # Transformación de Variables
            def parse_vars(val):
                if not val or pd.isna(val): return None
                items = [ColumnNameProcessor.sanitize_column_name(x.strip()) for x in str(val).split(',') if x.strip()]
                return ",".join(filter(None, items)) or None
                
            df['excluded_variables_parsed'] = df.get('Seleccione la variables a excluir', pd.Series()).apply(parse_vars)
            
            # Numéricos
            df['potencia_pico_kw'] = pd.to_numeric(df.get('Solo ingresar el valor de la potencia pico en kW ', 0), errors='coerce').fillna(0.0)
            df['exclusion'] = pd.to_numeric(df.get('Seleccione para realizar exclusión', 0), errors='coerce').fillna(0).astype(int)

            # Búsqueda dinámica de columnas texto
            col_tipo = next((c for c in df.columns if 'tipo de exclusi' in c.lower()), None)
            col_motivo = next((c for c in df.columns if 'motivo' in c.lower()), None)
            col_obs = next((c for c in df.columns if 'observaci' in c.lower()), None)

            records = []
            for _, row in df.iterrows():
                records.append((
                    row['form_timestamp'], row['exclusion_start'], row['exclusion_end'],
                    row[col_tipo] if col_tipo else None, row['exclusion'], 
                    row[col_motivo] if col_motivo else None, row[col_obs] if col_obs else None,
                    row['potencia_pico_kw'], row['excluded_variables_parsed']
                ))

            # UPSERT Query
            query = """
                INSERT INTO excluded_data (
                    form_timestamp, exclusion_start, exclusion_end, exclusion_type,
                    exclusion, motivo, observacion, potencia_pico_kw, excluded_variables
                ) VALUES %s
                ON CONFLICT (form_timestamp) DO UPDATE SET
                    exclusion_start=EXCLUDED.exclusion_start, exclusion_end=EXCLUDED.exclusion_end,
                    exclusion_type=EXCLUDED.exclusion_type, exclusion=EXCLUDED.exclusion,
                    motivo=EXCLUDED.motivo, observacion=EXCLUDED.observacion,
                    potencia_pico_kw=EXCLUDED.potencia_pico_kw, excluded_variables=EXCLUDED.excluded_variables;
            """
            execute_values(self.db.cursor, query, records)
            self.db.conn.commit()
            logging.info(f"[{self.plant.name}] ✓ {len(records)} exclusiones procesadas (UPSERT).")
            
        except Exception as e:
            self.db.conn.rollback()
            logging.error(f"[{self.plant.name}] Error sincronizando exclusiones GS: {e}")

    def clean_archived_exclusions(self):
        """Aplica las reglas de exclusión a la data ya validada y genera logs (Script 7)."""
        logging.info(f"[{self.plant.name}] Aplicando Exclusiones Retroactivas (Cleaner)...")
        
        analysis_query = """
        SELECT 
            v.id AS target_id, v.timestamp, e_del.id AS rule_id,
            CASE 
                WHEN EXISTS (
                    SELECT 1 FROM excluded_data e_acc
                    WHERE v.timestamp >= e_acc.exclusion_start 
                      AND v.timestamp <= e_acc.exclusion_end
                      AND TRIM(e_acc.exclusion_type) ILIKE '%Aceptaci%n%'
                ) THEN 'PROTECTED'
                ELSE 'TO_DELETE'
            END AS action_status
        FROM validated_data v
        JOIN excluded_data e_del 
          ON v.timestamp >= e_del.exclusion_start 
          AND v.timestamp <= e_del.exclusion_end
        WHERE e_del.exclusion = 0 AND TRIM(e_del.exclusion_type) ILIKE 'Exclusi%n de periodo marcado';
        """
        
        try:
            rows = self.db.execute_single_query(analysis_query, fetchall=True)
            to_delete_ids = []
            to_delete_logs = []
            protected_count = 0

            for row in rows:
                if row[3] == 'PROTECTED':
                    protected_count += 1
                elif row[3] == 'TO_DELETE':
                    to_delete_ids.append(row[0])
                    to_delete_logs.append((row[2], row[0], 'DELETE_RANGE', 'exclusions.py', datetime.now()))

            if to_delete_ids:
                archive_query = "INSERT INTO excluded_data_logs (excluded_data_id, deleted_id, operation_type, changed_by, created_at) VALUES %s"
                execute_values(self.db.cursor, archive_query, to_delete_logs)
                
                delete_query = "DELETE FROM validated_data WHERE id IN %s"
                self.db.cursor.execute(delete_query, (tuple(to_delete_ids),))
                
                self.db.conn.commit()
                logging.info(f"[{self.plant.name}] ✓ {len(to_delete_ids)} registros eliminados por exclusión. (Protegidos: {protected_count})")
            else:
                logging.info(f"[{self.plant.name}] No hay registros retroactivos para eliminar.")
                
        except Exception as e:
            self.db.conn.rollback()
            logging.error(f"[{self.plant.name}] Error en cleaner retroactivo: {e}")