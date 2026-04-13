import logging
from datetime import datetime
from src.core.database import DatabaseManager

class StateManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    def get_last_state(self):
        """Recupera el último estado y sus métricas para preservación."""
        # Se ordena por ID para garantizar la secuencia exacta de inserción
        query_state = """
            SELECT status, last_active_date, error_count, alarm_count, cadence, exclusions_min
            FROM pipeline_status_history 
            ORDER BY id DESC LIMIT 1
        """
        # Contamos el total absoluto de ejecuciones en la historia
        query_count = "SELECT COUNT(*) FROM pipeline_status_history"
        
        state_row = self.db.execute_single_query(query_state, fetchone=True)
        total_runs = self.db.execute_single_query(query_count, fetchone=True)[0]
        
        return {
            "last_status": state_row[0] if state_row else None,
            "last_active_date": state_row[1] if state_row else None,
            "prev_errors": state_row[2] if state_row else 0,
            "prev_alarms": state_row[3] if state_row else 0,
            "prev_cadence": state_row[4] if state_row else "N/A",
            "prev_excl": float(state_row[5]) if state_row else 0.0,
            "next_run_number": int(total_runs) + 1
        }

    def save_execution_state(self, status: str, files_count: int, last_active: datetime, run_number: int, errors: int = 0, alarms: int = 0, cadence: str = "N/A", exclusions: float = 0.0, obs: str = ""):
        """Guarda el snapshot completo del estado para el dashboard de auditores."""
        query = """
            INSERT INTO pipeline_status_history 
            (status, files_count, last_active_date, run_number, error_count, alarm_count, cadence, exclusions_min, observation)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (status, files_count, last_active, run_number, errors, alarms, cadence, exclusions, obs)
        self.db.execute_single_query(query, params)
        logging.info(f">>> [Run #{run_number}] Estado guardado: {status} (Err: {errors}, Alr: {alarms}, Cad: {cadence})")