import logging
import smtplib
from datetime import date, datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from src.models.plant import Plant
from src.core.database import DatabaseManager

class Notifier:
    def __init__(self, db_manager: DatabaseManager, plant: Plant, smtp_config: dict, extraction_errors: list):
        self.db = db_manager
        self.plant = plant
        self.smtp_config = smtp_config
        self.extraction_errors = extraction_errors
        
        # Métricas
        self.metrics = {'errores': 0, 'alarmas': 0, 'validados': 0, 'dias_cargados': 0}
        self.cobertura = {'days_loaded': 0, 'days_target': 0, 'percentage': 0}
        self.previews = []

    def _gather_metrics(self):
        """Consulta a la base de datos para obtener los KPIs del día."""
        today = date.today()
        try:
            self.metrics['errores'] = self.db.execute_single_query("SELECT COUNT(*) FROM validation_error_by_rules WHERE DATE(created_at) = %s AND error_type = 'error'", (today,), fetchone=True)[0]
            self.metrics['alarmas'] = self.db.execute_single_query("SELECT COUNT(*) FROM validation_error_by_rules WHERE DATE(created_at) = %s AND error_type = 'alarm'", (today,), fetchone=True)[0]
            self.metrics['validados'] = self.db.execute_single_query("SELECT COUNT(*) FROM validated_data WHERE DATE(created_at) = %s", (today,), fetchone=True)[0]
            self.metrics['dias_cargados'] = self.db.execute_single_query("SELECT COUNT(*) FROM load_control WHERE DATE(last_updated) = %s", (today,), fetchone=True)[0]
            
            # Cobertura Anual
            cov_query = """
                SELECT COUNT(*) as loaded, (EXTRACT(DOY FROM CURRENT_DATE)::int - 1) as target
                FROM load_control WHERE inventory_date < CURRENT_DATE AND EXTRACT(YEAR FROM inventory_date) = EXTRACT(YEAR FROM CURRENT_DATE)
            """
            cov_res = self.db.execute_single_query(cov_query, fetchone=True)
            if cov_res and cov_res[1] > 0:
                self.cobertura = {'days_loaded': cov_res[0], 'days_target': cov_res[1], 'percentage': round((cov_res[0]/cov_res[1])*100, 2)}
                
            # Previews para la tabla del email
            preview_query = """
                SELECT 
                    CAST(d.timestamp AS TIMESTAMP) AS timestamp, 
                    e.error_type, 
                    e.offending_column, 
                    e.offending_value
                FROM validation_error_by_rules e 
                JOIN raw_data d ON e.raw_data_id = d.id
                WHERE error_type != 'alarm' 
                ORDER BY d.timestamp DESC;
            """
            self.previews = self.db.execute_single_query(preview_query, (today,), fetchall=True)
        except Exception as e:
            logging.error(f"Error recopilando métricas para correo: {e}")

    def _build_html(self) -> str:
        """Construye el HTML con tu diseño moderno."""
        # Colores dinámicos
        err_color = "#EF4444" if self.metrics['errores'] > 0 else "#111827"
        alr_color = "#F59E0B" if self.metrics['alarmas'] > 0 else "#111827"
        
        # Bloque de errores de extracción (Si el CSV estaba corrupto)
        ext_err_html = ""
        if self.extraction_errors:
            lis = "".join([f"<li><b>{e['archivo']}</b>: {e['descripcion']}</li>" for e in self.extraction_errors])
            ext_err_html = f"<div style='background:#FEF2F2; padding:15px; border-left:4px solid #EF4444; margin-bottom:20px;'><h4 style='color:#991B1B; margin-top:0;'>⚠️ Errores de Lectura CSV</h4><ul>{lis}</ul></div>"

        # Previews
        preview_rows = ""
        for row in self.previews:
            preview_rows += f"<tr><td style='padding:8px; border-bottom:1px solid #ddd;'>{row[0]}</td><td style='padding:8px; border-bottom:1px solid #ddd;'>{row[2]}</td><td style='padding:8px; border-bottom:1px solid #ddd; color:#d32f2f;'>{str(row[3])[:30]}</td></tr>"
        
        preview_table = f"<table style='width:100%; text-align:left; border-collapse:collapse; font-size:13px;'><tr style='background:#f9f9f9;'><th>Timestamp</th><th>Columna</th><th>Valor</th></tr>{preview_rows}</table>" if self.previews else "<p>No hay errores detectados.</p>"

        # Botón Drive
        btn_drive = f"<div style='text-align:center; margin-top:20px;'><a href='{self.plant.graficas_link}' style='background:#2563EB; color:white; padding:10px 20px; text-decoration:none; border-radius:5px;'>Ver Reportes en Drive</a></div>" if self.plant.graficas_link != "#" else ""

        html = f"""
        <html>
        <body style="font-family: Arial, sans-serif; background-color: #f4f6f8; padding: 20px;">
            <div style="max-width: 600px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <h2 style="color: #1F2937; text-align: center; border-bottom: 2px solid #eee; padding-bottom: 10px;">Reporte de Validación: {self.plant.name}</h2>
                
                {ext_err_html}
                
                <div style="display: flex; justify-content: space-between; margin-bottom: 20px;">
                    <div style="width: 48%; padding: 15px; background: #f9fafb; border: 1px solid #eee; border-radius: 5px; text-align: center;">
                        <div style="font-size: 12px; color: #666; text-transform: uppercase;">Validados Hoy</div>
                        <div style="font-size: 28px; font-weight: bold; color: #10B981;">{self.metrics['validados']}</div>
                    </div>
                    <div style="width: 48%; padding: 15px; background: #f9fafb; border: 1px solid #eee; border-radius: 5px; text-align: center;">
                        <div style="font-size: 12px; color: #666; text-transform: uppercase;">Días Cargados (Batch)</div>
                        <div style="font-size: 28px; font-weight: bold; color: #2563EB;">{self.metrics['dias_cargados']}</div>
                    </div>
                </div>

                <div style="display: flex; justify-content: space-between; margin-bottom: 20px;">
                    <div style="width: 48%; padding: 15px; background: #f9fafb; border: 1px solid #eee; border-radius: 5px; text-align: center;">
                        <div style="font-size: 12px; color: #666; text-transform: uppercase;">Errores (Solar)</div>
                        <div style="font-size: 28px; font-weight: bold; color: {err_color};">{self.metrics['errores']}</div>
                    </div>
                    <div style="width: 48%; padding: 15px; background: #f9fafb; border: 1px solid #eee; border-radius: 5px; text-align: center;">
                        <div style="font-size: 12px; color: #666; text-transform: uppercase;">Alarmas (No-Solar)</div>
                        <div style="font-size: 28px; font-weight: bold; color: {alr_color};">{self.metrics['alarmas']}</div>
                    </div>
                </div>
                
                <div style="background: #e0f2fe; padding: 15px; border-radius: 5px; text-align: center; margin-bottom: 20px;">
                    <div style="font-size: 14px; color: #0369a1; font-weight: bold;">Cobertura Anual: {self.cobertura['percentage']}%</div>
                    <div style="font-size: 12px; color: #0284c7;">{self.cobertura['days_loaded']} días cargados de {self.cobertura['days_target']} objetivo.</div>
                </div>

                <h3 style="color: #374151; font-size: 16px;">Vista Previa de Errores</h3>
                {preview_table}
                
                {btn_drive}
                
                <div style="text-align: center; margin-top: 30px; font-size: 11px; color: #aaa;">
                    Generado automáticamente por Aletheia Pipeline V2.1.2 • {datetime.now().strftime('%Y-%m-%d %H:%M')}
                </div>
            </div>
        </body>
        </html>
        """
        return html

    def run(self):
        """Ejecuta la recolección y envío del correo."""
        if not self.plant.emails:
            logging.warning(f"[{self.plant.name}] No hay correos configurados. Saltando notificación.")
            return
            
        logging.info(f"[{self.plant.name}] Preparando notificación por correo...")
        self._gather_metrics()
        
        # Regla: Si no hubo nada que procesar y no hay errores de extracción, no enviamos spam.
        if sum(self.metrics.values()) == 0 and not self.extraction_errors:
            logging.info(f"Sin actividad para {self.plant.name}. Correo omitido.")
            return

        html_body = self._build_html()
        asunto = f"Aletheia Reporte: {self.plant.name} - {date.today().strftime('%d/%m/%Y')}"
        
        try:
            msg = MIMEMultipart('alternative')
            msg['From'] = self.smtp_config.get('user', 'no-reply@aletheia.com')
            msg['To'] = ", ".join(self.plant.emails)
            msg['Subject'] = asunto
            msg.attach(MIMEText(html_body, 'html', 'utf-8'))
            
            with smtplib.SMTP(self.smtp_config['server'], self.smtp_config['port']) as server:
                server.starttls()
                server.login(self.smtp_config['user'], self.smtp_config['password'])
                server.send_message(msg)
                
            logging.info(f"Correo enviado a: {msg['To']}")
        except Exception as e:
            logging.error(f"Fallo al enviar correo para {self.plant.name}: {e}")