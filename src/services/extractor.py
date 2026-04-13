import os
import json
import logging
from glob import glob
from pathlib import Path
import pandas as pd
from psycopg2.extras import execute_values

from src.models.plant import Plant
from src.core.database import DatabaseManager
from src.utils.helpers import ColumnNameProcessor, enforce_pg_numeric_constraints, SmartCsvReader

class DataExtractor:
    def __init__(self, db_manager: DatabaseManager, plant: Plant):
        self.db = db_manager
        self.plant = plant
        
        # Estado y Reportes
        self.errores_extraccion = []  # Lista de dicts con los errores
        self.archivos_procesados_ok = []
        
        # Rutas específicas de esta planta
        self.input_dir = Path(self.plant.input_path)
        self.state_file = Path(self.plant.rules_path).parent / 'processed_files.json'
        
        # Configuraciones de negocio (Podrían venir del settings.json de la planta en el futuro)
        self.min_rows_expected = int(os.getenv("MIN_ROWS_EXPECTED", "0"))

    def _get_new_files(self) -> set:
        if not self.input_dir.exists(): return set()
            
        current_filenames = {f.name.strip() for f in self.input_dir.glob("*.csv")}
        
        registered_filenames = set()
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    # NORMALIZACIÓN: Extraer solo el nombre del archivo de lo que sea que haya en el JSON
                    registered_filenames = {os.path.basename(f_path).strip() for f_path in json.load(f)}
            except: pass
                
        return current_filenames - registered_filenames

    def _save_state(self, newly_processed_files: list):
        """Añade los nuevos archivos exitosos al historial de la planta."""
        if not newly_processed_files:
            return
            
        registered_files = []
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    registered_files = json.load(f)
            except:
                pass
                
        # Merge evitando duplicados
        updated_files = list(set(registered_files + newly_processed_files))
        
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, 'w', encoding='utf-8') as f:
            json.dump(updated_files, f, indent=4, ensure_ascii=False)
            
        logging.info(f"Historial actualizado para {self.plant.name}: +{len(newly_processed_files)} archivos.")

    def _log_error(self, archivo: str, tipo_error: str, descripcion: str):
        """Acumula errores para el Notifier, sin detener el pipeline."""
        self.errores_extraccion.append({
            'planta': self.plant.name,
            'archivo': archivo,
            'tipo': tipo_error,
            'descripcion': descripcion
        })
        logging.error(f"[{self.plant.name}] {tipo_error} en {archivo}: {descripcion}")

    def _get_target_columns(self) -> list:
        """Obtiene las columnas reales de la BD, omitiendo las de sistema."""
        query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE lower(table_name) = 'raw_data'
            ORDER BY ordinal_position;
        """
        db_cols = [row[0].lower() for row in self.db.execute_single_query(query, fetchall=True)]
        return [c for c in db_cols if c not in {'id', 'status', 'created_at', 'processed_at'}]

    def run(self):
        """Ejecuta el ciclo de extracción para la planta actual."""
        logging.info(f"--- Iniciando Extracción: {self.plant.name} ---")
        
        new_files = self._get_new_files()
        if not new_files:
            logging.info(f"No hay archivos CSV nuevos para procesar en {self.plant.name}.")
            return
            
        logging.info(f"Detectados {len(new_files)} archivo(s) nuevo(s).")
        
        try:
            target_columns = self._get_target_columns()
            if not target_columns:
                self._log_error("SISTEMA", "ERROR_BD", "No se detectaron columnas en 'raw_data'.")
                return
                
            db_num_columns = len(target_columns)
            ts_col = target_columns[0]
            
            for filename in sorted(list(new_files)):
                file_path = self.input_dir / filename
                logging.info(f"Procesando: {filename}")
                
                try:
                    # 1. Lectura
                    try:
                        df = SmartCsvReader.read_csv_robust(str(file_path), db_num_columns)
                    except ValueError as e:
                        self._log_error(filename, "ERROR_LECTURA", str(e))
                        continue

                    # 2. Validación de columnas
                    if len(df.columns) != db_num_columns:
                        self._log_error(filename, "ERROR_COLUMNAS", f"Esperadas {db_num_columns}, encontradas {len(df.columns)}")
                        continue
                        
                    df.columns = target_columns

                    # 3. Limpieza de Fechas
                    df[ts_col] = df[ts_col].astype(str).str.strip()
                    total_rows_pre = len(df)
                    df[ts_col] = pd.to_datetime(df[ts_col], infer_datetime_format=True, errors='coerce')
                    
                    if df[ts_col].isna().all() and total_rows_pre > 0:
                        self._log_error(filename, "ERROR_PARSE_FECHAS", "Fallo total de parseo de fechas. Formato desconocido.")
                        continue

                    df.dropna(subset=[ts_col], inplace=True)

                    # 4. Limpieza Numérica
                    numeric_cols = [c for c in target_columns if c != ts_col]
                    for col in numeric_cols:
                        if df[col].dtype == 'object':
                             df[col] = df[col].astype(str).str.replace('.', '.', regex=False).str.replace(',', '.', regex=False)
                        s_numeric = pd.to_numeric(df[col], errors='coerce')
                        df[col] = enforce_pg_numeric_constraints(s_numeric)

                    df.dropna(how='all', subset=numeric_cols, inplace=True)
                    cleaned_rows = len(df)

                    if cleaned_rows < self.min_rows_expected:
                        self._log_error(filename, "ERROR_FILAS", f"Filas insuficientes: {cleaned_rows} (Pre: {total_rows_pre})")
                        continue

                    # 5. Inventario (load_control)
                    if not df.empty:
                        inv_df = df[[ts_col]].copy()
                        inv_df['d'] = inv_df[ts_col].dt.date
                        stats = inv_df.groupby('d')[ts_col].agg(['min', 'max', 'count']).reset_index()

                        for _, row in stats.iterrows():
                            upsert_sql = """
                                INSERT INTO load_control (inventory_date, min_time, max_time, row_count, last_updated)
                                VALUES (%s, %s, %s, %s, NOW())
                                ON CONFLICT (inventory_date) DO UPDATE SET
                                    min_time = LEAST(load_control.min_time, EXCLUDED.min_time),
                                    max_time = GREATEST(load_control.max_time, EXCLUDED.max_time),
                                    row_count = load_control.row_count + EXCLUDED.row_count,
                                    last_updated = NOW();
                            """
                            self.db.execute_single_query(upsert_sql, (row['d'], row['min'].time(), row['max'].time(), row['count']))

                    # 6. Carga Masiva a raw_data
                    df_to_load = df.where(pd.notnull(df), None)
                    cols_str = '", "'.join(target_columns)
                    query = f'INSERT INTO "raw_data" ("{cols_str}") VALUES %s'
                    data_tuples = list(df_to_load.itertuples(index=False, name=None))
                    
                    # Usamos el cursor expuesto por la conexión de db_manager
                    execute_values(self.db.cursor, query, data_tuples)
                    self.db.conn.commit()
                    
                    logging.info(f"✓ {cleaned_rows} filas insertadas desde {filename}.")
                    self.archivos_procesados_ok.append(filename)

                except Exception as e:
                    self.db.conn.rollback()
                    self._log_error(filename, "ERROR_CRITICO", f"Error inesperado: {str(e)}")

        except Exception as e:
            self._log_error("SISTEMA", "ERROR_GLOBAL", f"Fallo al procesar la planta: {str(e)}")
            
        finally:
            # Guardamos en el historial local SOLO los que fueron exitosos
            self._save_state(self.archivos_procesados_ok)