import logging
from src.core.database import DatabaseManager
from src.models.plant import Plant

class DuplicateHandler:
    def __init__(self, db_manager: DatabaseManager, plant: Plant):
        self.db = db_manager
        self.plant = plant

    def process(self):
        """Ejecuta el ciclo de detección y limpieza de duplicados."""
        logging.info(f"[{self.plant.name}] Iniciando manejo de duplicados...")
        
        try:
            # 1. Introspección de columnas (col_X)
            query_cols = """
                SELECT column_name FROM information_schema.columns 
                WHERE lower(table_name) = 'raw_data' AND column_name LIKE 'col_%' 
                ORDER BY ordinal_position;
            """
            cols_datos = [fila[0] for fila in self.db.execute_single_query(query_cols, fetchall=True)]
            
            if not cols_datos:
                logging.warning(f"[{self.plant.name}] No se encontraron columnas de datos (col_X) en raw_data.")
                return

            # 2. Construcción de strings SQL
            columnas_origen = ['id', 'timestamp'] + cols_datos
            columnas_origen_str = ", ".join([f'"{c}"' for c in columnas_origen])
            
            columnas_destino = ['raw_data_id', 'timestamp_col'] + cols_datos
            columnas_destino_str = ", ".join([f'"{c}"' for c in columnas_destino])

            # 3. Queries CTE (Common Table Expression)
            consulta_cte = """
            WITH filas_clasificadas AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY timestamp ORDER BY id ASC) as rn 
                FROM raw_data
            )
            """
            
            consulta_mover = consulta_cte + f"""
            INSERT INTO duplicated_data ({columnas_destino_str})
            SELECT {columnas_origen_str} FROM filas_clasificadas WHERE rn > 1;
            """
            
            consulta_eliminar = consulta_cte + """
            DELETE FROM raw_data WHERE id IN (SELECT id FROM filas_clasificadas WHERE rn > 1);
            """

            # 4. Ejecución Transaccional
            self.db.cursor.execute(consulta_mover)
            filas_movidas = self.db.cursor.rowcount
            logging.info(f"[{self.plant.name}] Se movieron {filas_movidas} registros duplicados.")

            if filas_movidas > 0:
                self.db.cursor.execute(consulta_eliminar)
                filas_eliminadas = self.db.cursor.rowcount
                logging.info(f"[{self.plant.name}] Se eliminaron {filas_eliminadas} registros duplicados de 'raw_data'.")
                
                if filas_movidas != filas_eliminadas:
                    raise Exception("Discrepancia en conteo de filas movidas vs. eliminadas.")

            self.db.conn.commit()
            
        except Exception as e:
            self.db.conn.rollback()
            logging.error(f"[{self.plant.name}] Error crítico procesando duplicados: {e}")