import logging
from datetime import datetime
from src.core.database import DatabaseManager
from src.models.plant import Plant
from src.core.config_loader import ConfigLoader

from src.services.extractor import DataExtractor
from src.services.duplicates import DuplicateHandler
from src.services.exclusions import ExclusionManager
from src.services.validator import DataValidator
from src.services.exporter import DataExporter
from src.services.graphics import GraphicsGenerator
from src.services.notifier import Notifier
from src.services.state_manager import StateManager

class AletheiaPipeline:
    def __init__(self, config: ConfigLoader):
        self.config = config

    def run(self, plant: Plant):
        logging.info("\n" + "="*60)
        logging.info(f" [ADV] INICIANDO PIPELINE PARA: {plant.name.upper()}")
        logging.info("="*60)

        db_manager = None
        try:
            db_manager = DatabaseManager(plant)
            state_mgr = StateManager(db_manager)
            
            # 1. Recuperar contexto previo (Incluyendo métricas para heredar)
            context = state_mgr.get_last_state()
            current_run = context["next_run_number"]

            # 2. Fases de Procesamiento
            extractor = DataExtractor(db_manager, plant)
            extractor.run()
            
            DuplicateHandler(db_manager, plant).process()
            ExclusionManager(db_manager, plant, self.config.creds_path).sync_from_sheets()
            
            # El validador genera los errores/alarmas en la BD
            DataValidator(db_manager, plant).process()
            
            # 3. DETERMINACIÓN DE ESTADO (Secuencia histórica estricta, sin importar el tiempo)
            new_files = len(extractor.archivos_procesados_ok)
            last_status = context["last_status"]
            last_active = context["last_active_date"]

            if new_files > 0:
                final_status = "ACTIVA"
                last_active = datetime.now()
            else:
                if last_status == "ACTIVA":
                    # 1ra ejecución sin datos (El estado anterior tuvo datos)
                    final_status = "PRESERVADA"
                    
                elif last_status == "PRESERVADA":
                    # 2da o 3ra ejecución sin datos: Miramos 1 paso más atrás en el historial
                    query_hist = "SELECT status FROM pipeline_status_history ORDER BY id DESC LIMIT 2"
                    hist_rows = db_manager.execute_single_query(query_hist, fetchall=True)
                    
                    # hist_rows[0] es la ejecución inmediatamente anterior (PRESERVADA)
                    # hist_rows[1] es la ejecución penúltima
                    if len(hist_rows) == 2 and hist_rows[1][0] == "PRESERVADA":
                        # Si la penúltima también fue PRESERVADA, significa que esta es la 3ra corrida sin datos
                        final_status = "SIN_ACTIVIDAD"
                    else:
                        # Si la penúltima fue ACTIVA, entonces esta es apenas la 2da corrida sin datos
                        final_status = "PRESERVADA"
                        
                else:
                    # Si veníamos de SIN_ACTIVIDAD y seguimos sin datos...
                    final_status = "SIN_ACTIVIDAD"

            # 4. RECOLECCIÓN O HERENCIA DE MÉTRICAS
            if final_status == "ACTIVA":
                # Recolectar KPIs reales generados en esta corrida
                today = datetime.now().date()
                err_count = db_manager.execute_single_query("SELECT COUNT(*) FROM validation_error_by_rules WHERE DATE(created_at) = %s AND error_type = 'error'", (today,), fetchone=True)[0] or 0
                alr_count = db_manager.execute_single_query("SELECT COUNT(*) FROM validation_error_by_rules WHERE DATE(created_at) = %s AND error_type = 'alarm'", (today,), fetchone=True)[0] or 0
                
                sql_cadencia = """
                    WITH CalculoPrevio AS (SELECT "timestamp" FROM public.raw_data WHERE "timestamp" IS NOT NULL ORDER BY "timestamp" DESC LIMIT 5000),
                    Diferencias AS (SELECT ABS(EXTRACT(EPOCH FROM ("timestamp" - LAG("timestamp") OVER (ORDER BY "timestamp" DESC)))) AS seg FROM CalculoPrevio),
                    Agrupacion AS (SELECT ROUND(seg) AS seg FROM Diferencias WHERE seg IS NOT NULL)
                    SELECT CASE WHEN seg >= 60 THEN (seg / 60)::int::text || 'm' ELSE seg::int::text || 's' END 
                    FROM Agrupacion GROUP BY seg ORDER BY COUNT(*) DESC LIMIT 1;
                """
                res_cad = db_manager.execute_single_query(sql_cadencia, fetchone=True)
                cadencia_act = str(res_cad[0]) if res_cad else "N/A"

                # Contar minutos reales excluidos (match exacto con datos procesados hoy)
                sql_excl = """
                    SELECT COUNT(DISTINCT r."timestamp")
                    FROM public.raw_data r
                    INNER JOIN public.excluded_data e 
                      ON r."timestamp" >= e.exclusion_start AND r."timestamp" <= e.exclusion_end
                    WHERE e.exclusion = 0 
                      AND r.created_at >= %s AND r.created_at <= %s;
                """
                
                # Ventana de procesamiento actual (hoy)
                w_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                w_end = datetime.now()
                
                res_excl = db_manager.execute_single_query(sql_excl, (w_start, w_end), fetchone=True)
                excl_act = float(res_excl[0] or 0.0) if res_excl else 0.0

            elif final_status == "PRESERVADA":
                # HEREDAR métricas de la corrida anterior (vienen del context)
                err_count = context.get("prev_errors", 0)
                alr_count = context.get("prev_alarms", 0)
                cadencia_act = context.get("prev_cadence", "N/A")
                excl_act = context.get("prev_excl", 0.0)
                logging.info(f" [INFO] Estado PRESERVADO. Heredando métricas (Err: {err_count}, Alr: {alr_count})")
            else:
                # SIN_ACTIVIDAD resetea contadores
                err_count, alr_count, excl_act = 0, 0, 0.0
                cadencia_act = "N/A"

            # 5. GUARDAR ESTADO (Snapshot completo para auditores)
            state_mgr.save_execution_state(
                status=final_status,
                files_count=new_files,
                last_active=last_active,
                run_number=current_run,
                errors=err_count,
                alarms=alr_count,
                cadence=cadencia_act,
                exclusions=excl_act,
                obs=f"Ejecución {datetime.now().strftime('%A')}"
            )

            # 6. Generación de Reportes y Notificaciones
            if final_status != "SIN_ACTIVIDAD":
                DataExporter(db_manager, plant).export_excel_reports()
                GraphicsGenerator(db_manager, plant).process()
            
            # NOTIFICAR A LA PLANTA EN 'ACTIVA' Y 'SIN_ACTIVIDAD' (Omitir en PRESERVADA)
            if final_status != "PRESERVADA":
                Notifier(db_manager, plant, self.config.smtp_config, extractor.errores_extraccion).run()
            else:
                logging.info(f"Estado PRESERVADA para {plant.name}. Correo omitido para evitar spam.")

            logging.info(f"Pipeline finalizado para {plant.name} (Estado: {final_status} | Errores: {err_count})")

        except Exception as e:
            logging.critical(f"FALLO en Pipeline de {plant.name}: {e}", exc_info=True)
        finally:
            if db_manager: db_manager.close()