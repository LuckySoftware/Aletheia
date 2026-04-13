import os
import logging
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

from src.models.plant import Plant
from src.core.database import DatabaseManager

class GraphicsGenerator:
    START_TIME = '06:00:00'
    END_TIME = '18:59:59'

    def __init__(self, db_manager: DatabaseManager, plant: Plant):
        self.db = db_manager
        self.plant = plant
        self.img_dir = self.plant.img_path
        self.exec_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        os.makedirs(self.img_dir, exist_ok=True)
        plt.style.use('ggplot')

    def _get_data(self, error_type: str) -> pd.DataFrame:
        query = """
            SELECT ve.offending_column AS nombre_columna, COUNT(*) AS cantidad
            FROM validation_error_by_rules ve
            JOIN raw_data rd ON ve.raw_data_id = rd.id
            WHERE ve.error_type = %s AND rd.timestamp::time BETWEEN %s AND %s AND DATE(ve.created_at) = CURRENT_DATE
            GROUP BY ve.offending_column
        """
        rows = self.db.execute_single_query(query, (error_type, self.START_TIME, self.END_TIME), fetchall=True)
        return pd.DataFrame(rows, columns=['nombre_columna', 'cantidad']) if rows else pd.DataFrame()

    def process(self):
        """Genera los gráficos de barras y torta."""
        logging.info(f"[{self.plant.name}] Generando gráficos analíticos...")
        
        configs = [
            ('error', 'Errores', '#c0392b', 'grafico_errores'),
            ('alarm', 'Alarmas', '#f39c12', 'grafico_alarmas')
        ]

        try:
            for err_type, title, color, filename in configs:
                df = self._get_data(err_type)
                
                if df.empty: continue
                df = df[df['cantidad'] > 1].copy()
                if len(df) < 2: continue
                
                df['sort_id'] = df['nombre_columna'].str.extract(r'col_(\d+)_').astype(float).fillna(-1)
                
                # 1. Gráfico de Barras
                df_bar = df.sort_values(by='sort_id', ascending=False)
                plt.figure(figsize=(10, max(6, len(df_bar) * 0.4)))
                plt.barh(df_bar['nombre_columna'], df_bar['cantidad'], color=color)
                plt.title(f"{title} - Hoy")
                plt.tight_layout()
                plt.savefig(os.path.join(self.img_dir, f"{filename}_bar_{self.exec_timestamp}.png"))
                plt.close()

                # 2. Gráfico de Torta
                df_pie = df.sort_values(by='sort_id', ascending=True)
                plt.figure(figsize=(12, 8))
                plt.pie(df_pie['cantidad'], labels=None, autopct='%1.1f%%', colors=plt.cm.tab20.colors)
                plt.legend(df_pie['nombre_columna'], title="Columnas", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
                plt.title(f"{title} - Distribución Hoy")
                plt.tight_layout()
                plt.savefig(os.path.join(self.img_dir, f"{filename}_torta_{self.exec_timestamp}.png"))
                plt.close()

            logging.info(f"[{self.plant.name}] ✓ Gráficos exportados exitosamente en {self.img_dir}")
        except Exception as e:
            logging.error(f"[{self.plant.name}] Error generando gráficos: {e}")