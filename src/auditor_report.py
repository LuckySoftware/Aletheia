import logging
import sys
import smtplib
from datetime import date, datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from src.core.config_loader import ConfigLoader
from src.core.database import DatabaseManager

# Configuración de logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] - %(message)s', 
    handlers=[logging.StreamHandler(sys.stdout)]
)

class AuditorReportService:
    def __init__(self, config: ConfigLoader):
        self.config = config
        self.reportes = []

    def recolectar_datos_planta(self, plant) -> dict:
        """Extrae los KPIs específicos para el reporte de auditoría usando la conexión centralizada."""
        reporte = {
            'plant_name': plant.name, 'has_activity': False, 'interval': "Sin Conexión",
            'error_count': 0, 'alarm_count': 0, 'validados': 0,
            'excl_min': 0, 'excl_hor': 0.0, 'excl_dia': 0.0,
            'kpi_loaded': 0, 'kpi_elapsed': 0, 'kpi_gap': 0, 'kpi_pct': 0.0,
            'drive_link': getattr(plant, 'graficas_link', '#'),
            'ultimo_estado_db': 'SIN_ACTIVIDAD'
        }

        db = None
        try:
            db = DatabaseManager(plant)
            today_date = date.today()
            now = datetime.now()
            last_week = now - timedelta(days=7)

            # 1. Cadencia (Consulta en vivo por si hay datos nuevos)
            sql_cadencia = """
                WITH CalculoPrevio AS (SELECT "timestamp" FROM public.raw_data WHERE "timestamp" IS NOT NULL ORDER BY "timestamp" DESC LIMIT 5000),
                Diferencias AS (SELECT ABS(EXTRACT(EPOCH FROM ("timestamp" - LAG("timestamp") OVER (ORDER BY "timestamp" DESC)))) AS segundos_exactos FROM CalculoPrevio),
                Agrupacion AS (SELECT ROUND(segundos_exactos) AS segundos FROM Diferencias WHERE segundos_exactos IS NOT NULL)
                SELECT CASE WHEN segundos = 0 THEN '0s (Dupl)' WHEN segundos >= 60 THEN (segundos / 60)::int::text || 'm' ELSE segundos::int::text || 's' END AS cadencia_fmt
                FROM Agrupacion GROUP BY segundos ORDER BY COUNT(*) DESC LIMIT 1;
            """
            res_cad = db.execute_single_query(sql_cadencia, fetchone=True)
            reporte['interval'] = str(res_cad[0]) if res_cad and res_cad[0] else "N/A"

            # 2. Conteo Diario (Tablas de trabajo)
            reporte['error_count'] = db.execute_single_query("SELECT COUNT(DISTINCT raw_data_id) FROM public.validation_error_by_rules WHERE DATE(created_at) = %s AND error_type = 'error'", (today_date,), fetchone=True)[0] or 0
            reporte['alarm_count'] = db.execute_single_query("SELECT COUNT(DISTINCT raw_data_id) FROM public.validation_error_by_rules WHERE DATE(created_at) = %s AND error_type = 'alarm'", (today_date,), fetchone=True)[0] or 0
            reporte['validados'] = db.execute_single_query("SELECT COUNT(*) FROM public.validated_data WHERE DATE(created_at) = %s", (today_date,), fetchone=True)[0] or 0

            # 3. Exclusiones (Ventana de 7 días exactos cruzados con los datos reales)
            sql_excl = """
                SELECT COUNT(DISTINCT r."timestamp")
                FROM public.raw_data r
                INNER JOIN public.excluded_data e 
                  ON r."timestamp" >= e.exclusion_start AND r."timestamp" <= e.exclusion_end
                WHERE e.exclusion = 0 
                  AND r."timestamp" >= %s AND r."timestamp" <= %s;
            """
            res_excl = db.execute_single_query(sql_excl, (last_week, now), fetchone=True)
            if res_excl and res_excl[0]:
                total_min = int(res_excl[0])
                reporte['excl_min'] = total_min
                reporte['excl_hor'] = round(total_min / 60.0, 2)
                reporte['excl_dia'] = round(total_min / 1440.0, 2)

            # 4. KPI de Carga (Cobertura anual)
            sql_kpi = """
                SELECT COUNT(*) FILTER (WHERE min_time <= '05:00:00'::time AND max_time >= '19:00:00'::time),
                (EXTRACT(DOY FROM CURRENT_DATE)::int - 1),
                ((EXTRACT(DOY FROM CURRENT_DATE)::int - 1) - COUNT(*) FILTER (WHERE min_time <= '05:00:00'::time AND max_time >= '19:00:00'::time)),
                COALESCE(ROUND((COUNT(*) FILTER (WHERE min_time <= '05:00:00'::time AND max_time >= '19:00:00'::time)::numeric / NULLIF((EXTRACT(DOY FROM CURRENT_DATE)::int - 1), 0)::numeric) * 100, 2), 0.00)
                FROM load_control WHERE inventory_date < CURRENT_DATE AND EXTRACT(YEAR FROM inventory_date) = EXTRACT(YEAR FROM CURRENT_DATE);
            """
            res_kpi = db.execute_single_query(sql_kpi, fetchone=True)
            if res_kpi:
                reporte['kpi_loaded'], reporte['kpi_elapsed'], reporte['kpi_gap'], reporte['kpi_pct'] = int(res_kpi[0] or 0), int(res_kpi[1] or 0), int(res_kpi[2] or 0), float(res_kpi[3] or 0)

            # 5. INTEGRACIÓN CON HISTORIAL DE PRESERVACIÓN
            # Se agregó 'exclusions_min' a la consulta y se ordenó por ID para mayor precisión
            sql_estado = "SELECT status, error_count, alarm_count, cadence, exclusions_min FROM public.pipeline_status_history ORDER BY id DESC LIMIT 1"
            res_hist = db.execute_single_query(sql_estado, fetchone=True)
            
            if res_hist:
                reporte['ultimo_estado_db'] = res_hist[0]
                # Si el estado es PRESERVADA, recuperamos TODAS las métricas de la tabla de historial
                if reporte['ultimo_estado_db'] == "PRESERVADA":
                    reporte['error_count'] = res_hist[1] or 0
                    reporte['alarm_count'] = res_hist[2] or 0
                    reporte['interval'] = res_hist[3] or "N/A"
                    
                    # Recuperar exclusiones preservadas
                    total_min = int(float(res_hist[4] or 0.0))
                    reporte['excl_min'] = total_min
                    reporte['excl_hor'] = round(total_min / 60.0, 2)
                    reporte['excl_dia'] = round(total_min / 1440.0, 2)

            # Verificar actividad total
            reporte['has_activity'] = (reporte['error_count'] + reporte['alarm_count'] + reporte['validados']) > 0 or reporte['ultimo_estado_db'] == "PRESERVADA"

        except Exception as e:
            logging.error(f"Error procesando auditoría para {plant.name}: {e}")
        finally:
            if db: db.close()

        return reporte

    def generar_html(self) -> str:
        rows_html = ""
        for r in self.reportes:
            # Lógica de Badge
            if not r['has_activity']:
                badge_bg, badge_fg, status_text, icon = "#e0e0e0", "#424242", "Sin Actividad", "ℹ️"
            elif r.get('ultimo_estado_db') == "PRESERVADA":
                badge_bg, badge_fg, status_text, icon = "#cfd8dc", "#455a64", "Preservado", "💤"
            elif r['error_count'] > 0:
                badge_bg, badge_fg, status_text, icon = "#ffcdd2", "#c62828", "Con Errores", "❌"
            elif r['alarm_count'] > 0:
                badge_bg, badge_fg, status_text, icon = "#fff9c4", "#fbc02d", "Con Alarmas", "⚠️"
            else:
                badge_bg, badge_fg, status_text, icon = "#c8e6c9", "#2e7d32", "Validado", "✅"

            btn_graficas = f'<a href="{r["drive_link"]}" target="_blank" style="display:inline-block; padding:6px 12px; background:#1976d2; color:white; text-decoration:none; border-radius:15px; font-size:11px; font-weight:bold; box-shadow: 0 2px 5px rgba(0,0,0,0.1);">Ver Datos 📊</a>' if r['drive_link'] != '#' else '<span style="color:#adb5bd;font-size:18px;">&ndash;</span>'
            kpi_color = "#2e7d32" if r['kpi_gap'] <= 6 else ("#f9a825" if r['kpi_gap'] <= 10 else "#c62828")
            
            rows_html += f"""
            <tr style="border-bottom: 1px solid #eeeeee;">
                <td style="padding:15px 10px; text-align:center;"><span style="background-color:{badge_bg}; color:{badge_fg}; padding:6px 14px; border-radius:20px; font-size:11px; font-weight:bold; white-space:nowrap; display:inline-block;">{icon} {status_text.upper()}</span></td>
                <td style="padding:15px 10px; font-weight:600; color:#424242;">{r['plant_name']}</td>
                <td style="padding:15px 10px; text-align:center;">
                    <div style="font-weight:bold; color:{kpi_color}; font-size:1.1em;">{r['kpi_pct']}%</div>
                    <div style="font-size:0.85em; color:#616161;">{r['kpi_loaded']} / {r['kpi_elapsed']} días</div>
                    <div style="font-size:0.8em; color:#9e9e9e;">Gap: {r['kpi_gap']}d</div>
                </td>
                <td style="padding:15px 10px; text-align:center; color:#616161;">{r['interval']}</td> 
                <td style="padding:15px 10px; text-align:center;"><span style="color:{'#d32f2f' if r['error_count']>0 else '#bdbdbd'}; font-weight:{'bold' if r['error_count']>0 else 'normal'}; font-size:14px;">{r['error_count']}</span></td>
                <td style="padding:15px 10px; text-align:center;"><span style="color:{'#fbc02d' if r['alarm_count']>0 else '#bdbdbd'}; font-weight:{'bold' if r['alarm_count']>0 else 'normal'}; font-size:14px;">{r['alarm_count']}</span></td>
                <td style="padding:15px 10px; text-align:right;"><div style="font-size:12px; color:#424242; line-height:1.4;"><span style="font-weight:700;">{r['excl_min']}</span> min<br><span style="font-weight:700;">{r['excl_hor']}</span> h<br><span style="font-size:11px; color:#757575;">{r['excl_dia']} d</span></div></td>
                <td style="padding:15px 10px; text-align:center;">{btn_graficas}</td>
            </tr>
            """
        
        return f"""
        <!DOCTYPE html><html><head><meta charset="utf-8">
        <style>
            body {{ font-family: 'Segoe UI', Roboto, sans-serif; background-color: #f4f6f8; margin: 0; padding: 20px; }}
            .container {{ max-width: 1000px; margin: 0 auto; background: white; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); overflow: hidden; }}
            .header {{ background: #1976d2; color: white; padding: 25px; text-align: center; }}
            .content {{ padding: 25px; }}
            table {{ width: 100%; border-collapse: separate; border-spacing: 0; }}
            th {{ background: #f5f5f5; padding: 15px; text-align: left; font-size: 12px; color: #616161; text-transform: uppercase; border-bottom: 2px solid #e0e0e0; }}
            th:first-child {{ border-top-left-radius: 8px; }} th:last-child {{ border-top-right-radius: 8px; }}
            td {{ font-size: 13px; color: #424242; }}
            .footer {{ padding: 20px; text-align: center; font-size: 11px; color: #9e9e9e; background: #fafafa; border-top: 1px solid #eeeeee; }}
        </style></head><body>
        <div class="container"><div class="header"><h2 style="margin:0; font-weight:400;">Control Semanal de Carga de Datos</h2><p style="margin:5px 0 0 0; opacity:0.9; font-size:14px;">Fecha: {date.today().strftime('%d/%m/%Y')}</p></div>
        <div class="content"><table><thead><tr>
        <th style="text-align:center;">Estado</th><th>Planta</th><th style="text-align:center;">Días<br>Acumulados</th><th style="text-align:center;">Cadencia</th><th style="text-align:center;">Errores<br>(Solar)</th><th style="text-align:center;">Alarmas<br>(No-Solar)</th><th style="text-align:right;">Exclusiones</th><th style="text-align:center;">Link</th>
        </tr></thead><tbody>{rows_html}</tbody></table></div>
        <div class="footer">Sistema de Control de Carga de Datos y Validación - Aletheia V2.1.2</div>
        </div></body></html>
        """

    def ejecutar_y_enviar(self):
        logging.info("--- Recolectando métricas globales para Auditores ---")
        for plant in self.config.plants:
            self.reportes.append(self.recolectar_datos_planta(plant))

        if not self.config.auditor_emails:
            logging.warning("No hay 'AUDITOR_EMAILS' definidos en el .env. Cancelando envío.")
            return

        html_body = self.generar_html()
        try:
            msg = MIMEMultipart('alternative')
            msg['From'] = self.config.smtp_config.get('user', '')
            msg['To'] = ", ".join(self.config.auditor_emails)
            msg['Subject'] = f"Reporte Semanal de Control de Carga de Datos - COLOMBIA {date.today().strftime('%d/%m')}"
            msg.attach(MIMEText(html_body, 'html', 'utf-8'))
            
            with smtplib.SMTP(self.config.smtp_config['server'], self.config.smtp_config['port']) as server:
                server.starttls()
                server.login(self.config.smtp_config['user'], self.config.smtp_config['password'])
                server.send_message(msg)
                
            logging.info(f"Reporte global enviado a {len(self.config.auditor_emails)} auditores.")
        except Exception as e:
            logging.error(f"Error enviando reporte global de auditoría: {e}")

if __name__ == "__main__":
    try:
        conf = ConfigLoader("config.json")
        auditor = AuditorReportService(conf)
        auditor.ejecutar_y_enviar()
    except Exception as e:
        logging.critical(f"Fallo critico en reporte de auditoria: {e}", exc_info=True)