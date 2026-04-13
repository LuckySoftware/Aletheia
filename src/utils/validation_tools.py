import re
from datetime import datetime
from typing import List
import pandas as pd
import numpy as np

from src.core.database import DatabaseManager

import re
import logging
from datetime import datetime
from typing import List
import pandas as pd
import numpy as np

class SolarConditionClassifier:
    """Clasifica la condición solar de la planta basándose ESTRICTAMENTE en el promedio de las GHI."""
    
    SOLAR_THRESHOLD = 50.0
    
    # Patrones para atrapar ÚNICAMENTE las horizontales/GHI (nuestra fuente de verdad)
    GHI_PATTERNS = {
        'buga2': r'irradiancia.*horizontal',
        'canahuate': r'irradiancia_horizontal_global',
        'lacira': r'principal.*irradiancia', 
        'palmira2': r'irradiancia_ghi',
        'sanfelipe': r'irr_h',
        'puertotejada': r'x_em\d+_irr_h1'
    }
    
    def __init__(self, available_columns: List[str], plant_name: str):
        self.available_columns = available_columns
        self.plant_name = str(plant_name).lower().replace(" ", "").strip()
        
        # 1. Obtener patrón estricto de GHI
        self.ghi_pattern = self.GHI_PATTERNS.get(
            self.plant_name, 
            r'ghi|horizontal' # Fallback estricto a ghi u horizontal
        )
        
        # 2. Identificar SOLO las columnas GHI para usarlas como referencia
        self.ghi_reference_columns = [
            col for col in self.available_columns 
            if re.search(self.ghi_pattern, col, re.IGNORECASE)
        ]
        
        if not self.ghi_reference_columns:
            logging.warning(f"[{self.plant_name}] No se detectaron columnas GHI para el promedio. Se usará el respaldo por horario (06:00-18:00).")

    def classify_dataframe(self, df: pd.DataFrame) -> pd.Series:
        if df.empty: return pd.Series(dtype=str)
        
        # Buscar columna de tiempo para el respaldo
        time_col = 'timestamp' if 'timestamp' in df.columns else None

        def classify_record(row):
            # 1. Intentar clasificar usando EL PROMEDIO DE LAS GHI
            if self.ghi_reference_columns:
                ghi_vals = []
                for c in self.ghi_reference_columns:
                    val = row.get(c)
                    if pd.notna(val):
                        try:
                            ghi_vals.append(float(val))
                        except (ValueError, TypeError):
                            pass
                
                # Si logramos extraer valores GHI válidos, tomamos la decisión
                if ghi_vals:
                    promedio_ghi = np.mean(ghi_vals)
                    # Si el GHI > 50, la planta está activa -> Error. Si no -> Alarma.
                    return 'error' if promedio_ghi > self.SOLAR_THRESHOLD else 'alarm'

            # 2. RESPALDO: Clasificación por HORARIO (Si las GHI están caídas o en NaN)
            try:
                if time_col and time_col in row:
                    current_time = row[time_col]
                elif row.name is not None:
                    current_time = row.name # Por si el timestamp es el índice del DataFrame
                else:
                    raise ValueError("Sin referencia de tiempo")

                if isinstance(current_time, str):
                    current_time = pd.to_datetime(current_time)
                
                hour = current_time.hour
                # Entre las 6:00 y las 17:59:59 es horario solar (Error)
                if 6 <= hour < 18:
                    return 'error'
                else:
                    return 'alarm'
            except Exception as e:
                # Si todo falla, fallamos hacia "error" para no ocultar problemas reales
                return 'error'
            
        return df.apply(classify_record, axis=1)

class ValidationBypassHandler:
    """Gestiona las excepciones (waivers) activas en la BD."""
    def __init__(self, db: DatabaseManager):
        self.db = db
        self._active_bypasses = {} 
        self._load_bypasses()

    def _normalize_timestamp(self, ts) -> datetime:
        if pd.isna(ts): return None
        if isinstance(ts, str): ts = pd.to_datetime(ts)
        if isinstance(ts, pd.Timestamp): ts = ts.to_pydatetime()
        if ts and ts.tzinfo: ts = ts.replace(tzinfo=None)
        return ts

    def _load_bypasses(self):
        rows = self.db.execute_single_query("SELECT id, exclusion_start, exclusion_end, excluded_variables FROM excluded_data WHERE exclusion = 0;", fetchall=True)
        if not rows: return
        
        for row in rows:
            start_dt, end_dt, raw_vars = self._normalize_timestamp(row[1]), self._normalize_timestamp(row[2]), row[3]
            target_vars = ['ALL'] if not raw_vars else [v.strip() for v in str(raw_vars).split(',') if v.strip()]
            
            for var in target_vars:
                col_key = re.match(r'^(col_\d+)', var, re.IGNORECASE)
                col_key = col_key.group(1).lower() if col_key else var.lower().strip()
                if col_key not in self._active_bypasses: self._active_bypasses[col_key] = []
                self._active_bypasses[col_key].append({'start': start_dt, 'end': end_dt})

    def should_bypass(self, timestamp, column_name: str) -> bool:
        col_key = re.match(r'^(col_\d+)', column_name, re.IGNORECASE)
        target_key = col_key.group(1).lower() if col_key else column_name.lower().strip()
        
        if target_key not in self._active_bypasses and 'ALL' not in self._active_bypasses: return False
        
        check_time = self._normalize_timestamp(timestamp)
        for key in [target_key, 'ALL']:
            if key in self._active_bypasses:
                for rule in self._active_bypasses[key]:
                    if rule['start'] <= check_time <= rule['end']: return True
        return False