import json
import logging
import re
import pandas as pd
from pathlib import Path
from psycopg2.extras import execute_values

from src.models.plant import Plant
from src.core.database import DatabaseManager
from src.utils.validation_tools import SolarConditionClassifier, ValidationBypassHandler

class DataValidator:
    def __init__(self, db_manager: DatabaseManager, plant: Plant):
        self.db = db_manager
        self.plant = plant

    def _sync_rules(self):
        """Asegura que el rules.json local esté en la BD."""
        logging.info(f"[{self.plant.name}] Sincronizando reglas JSON...")
        rules_file = Path(self.plant.rules_path)
        if not rules_file.exists():
            logging.warning(f"No hay rules.json en {rules_file}")
            return
            
        with open(rules_file, 'r', encoding='utf-8') as f:
            rules = json.load(f)

        query = """
            INSERT INTO validation_rules (column_pattern, rule_type, rule_config, error_message, is_active)
            VALUES %s
            ON CONFLICT (column_pattern, rule_type) DO UPDATE SET
                rule_config = EXCLUDED.rule_config, error_message = EXCLUDED.error_message, is_active = EXCLUDED.is_active;
        """
        
        records = []
        for rule in rules:
            cfg = json.dumps(rule.get("rule_config")) if rule.get("rule_config") else None
            records.append((rule["column_pattern"], rule["rule_type"], cfg, rule["error_message"], rule.get("is_active", True)))
        
        if records:
            # Optimizamos el UPSERT usando execute_values para enviarlo en bloque
            execute_values(self.db.cursor, query, records)
            self.db.conn.commit()

    def process(self):
        """Motor principal de validación."""
        try:
            self._sync_rules()
            logging.info(f"[{self.plant.name}] Validando datos pendientes...")
            
            # Cargar Metadatos
            reglas_raw = self.db.execute_single_query("SELECT id, column_pattern, rule_type, rule_config, error_message FROM validation_rules WHERE is_active = TRUE", fetchall=True)
            reglas = [{'id': r[0], 'column_pattern': r[1], 'rule_type': r[2], 'rule_config': r[3], 'error_message': r[4]} for r in reglas_raw]
            
            columnas_db = [c[0] for c in self.db.execute_single_query("SELECT column_name FROM information_schema.columns WHERE lower(table_name) = 'raw_data' AND column_name NOT IN ('id', 'status', 'created_at', 'processed_at') ORDER BY ordinal_position;", fetchall=True)]
            
            if not reglas or not columnas_db: return

            # Leer Pending
            self.db.cursor.execute("SELECT * FROM raw_data WHERE status = 'pending' ORDER BY id")
            pending_data = self.db.cursor.fetchall()
            if not pending_data:
                logging.info(f"[{self.plant.name}] ✓ No hay datos pendientes de validación.")
                return
                
            df = pd.DataFrame(pending_data, columns=[d[0] for d in self.db.cursor.description])
            
            # Le pasamos el plant_name para que el Regex funcione dinámicamente
            solar_classifier = SolarConditionClassifier(
                available_columns=columnas_db, 
                plant_name=self.plant.name
            )
            bypass_handler = ValidationBypassHandler(self.db)
            
            df['solar_classification'] = solar_classifier.classify_dataframe(df)
            df['final_status'] = 'success'
            ts_col = columnas_db[0]
            
            errores_db = []
            stats = {'checked': 0, 'bypassed': 0, 'errors': 0}

            # Motor de Reglas
            for regla in reglas:
                pat = regla['column_pattern'].lower()
                target_cols = [c for c in columnas_db if pat in c.lower() or (re.search(pat, c, re.IGNORECASE) if '*' in pat or '^' in pat else False)]
                cfg = regla['rule_config'] or {}
                
                for col in target_cols:
                    for idx, row in df.iterrows():
                        val = row[col]
                        ok = True
                        if pd.isna(val): ok = (regla['rule_type'] != 'not_null')
                        elif regla['rule_type'] == 'range':
                            try:
                                v = float(val)
                                if 'min' in cfg and v < cfg['min']: ok = False
                                if 'max' in cfg and v > cfg['max']: ok = False
                            except: ok = False
                        elif regla['rule_type'] == 'enum':
                            if val not in cfg.get('allowed_values', []): ok = False
                            
                        if not ok:
                            stats['checked'] += 1
                            if bypass_handler.should_bypass(row[ts_col], col):
                                if df.at[idx, 'final_status'] != 'error': df.at[idx, 'final_status'] = 'waived'
                                stats['bypassed'] += 1
                            else:
                                df.at[idx, 'final_status'] = 'error'
                                stats['errors'] += 1
                                errores_db.append((row['id'], regla['id'], col, str(val), row['solar_classification']))

            df['status'] = df['final_status']
            success_df = df[df['status'].isin(['success', 'waived'])]
            error_df = df[df['status'] == 'error']

            # Guardar en DB
            if not success_df.empty:
                cols_ins = ['raw_data_id', 'status'] + [c for c in success_df.columns if c in columnas_db]
                cols_formatted = ','.join([f'"{c}"' for c in cols_ins])
                query = f"INSERT INTO validated_data ({cols_formatted}) VALUES %s"
                records = [tuple([row['id'], row['status']] + [row[c] for c in cols_ins[2:]]) for _, row in success_df.iterrows()]
                execute_values(self.db.cursor, query, records)

            if errores_db:
                execute_values(self.db.cursor, "INSERT INTO validation_error_by_rules (raw_data_id, validation_rule_id, offending_column, offending_value, error_type) VALUES %s", errores_db)
            
            if not success_df.empty:
                ok_ids = success_df['id'].tolist()
                self.db.cursor.execute("UPDATE raw_data SET status='success', processed_at=NOW() WHERE id = ANY(%s)", (ok_ids,))
            
            if not error_df.empty:
                err_ids = error_df['id'].tolist()
                self.db.cursor.execute("UPDATE raw_data SET status='error', processed_at=NOW() WHERE id = ANY(%s)", (err_ids,))

            self.db.conn.commit()
            logging.info(f"[{self.plant.name}] ✓ Validación: {len(success_df)} OK/Waived | {len(error_df)} Errores (Bypassed: {stats['bypassed']})")

        except Exception as e:
            self.db.conn.rollback()
            logging.error(f"[{self.plant.name}] Error en Validación CORE: {e}", exc_info=True)