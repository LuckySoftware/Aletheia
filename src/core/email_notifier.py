"""
email_notifier.py

Módulo integrado de notificación por correo electrónico para el pipeline Aletheia.
Incluye:
- Monitoreo de archivos (file_checker)
- Acceso a datos (database_manager)
- Envío de notificaciones SMTP

Versión corregida: Soluciona el problema de doble detección y emails duplicados.

Uso:
    python email_notifier.py
"""

import os
import sys
import json
import re
import logging
import smtplib
import psycopg2
from datetime import date
from typing import Dict, List, Optional, Any, Tuple
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

# --- CONFIGURACIÓN DEL LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# --- CONFIGURACIÓN DE RUTAS Y ENTORNO ---
PROJECT_ROOT = "/app"
ENV_PATH = os.path.join(PROJECT_ROOT, '.env')

if os.path.exists(ENV_PATH):
    load_dotenv(dotenv_path=ENV_PATH)
else:
    logging.critical(f"FATAL: No se encontró el archivo .env en la ruta: {ENV_PATH}")
    sys.exit(1)


# =================================================================================================
# SECCIÓN 1: MONITOREO DE ARCHIVOS (file_checker integrado)
# =================================================================================================

def extract_plant_name(path: str) -> str:
    """Extrae el nombre de la planta de una ruta."""
    match = re.search(r"files_from_scada[\\/]([^\\/]+)", path, re.IGNORECASE)
    if match:
        return match.group(1).capitalize()
    return "Desconocida"


class FileMonitor:
    """Monitorea un directorio para detectar archivos nuevos."""
    
    def __init__(self, folder_path: str, state_file_path: Optional[str] = None):
        if not os.path.isdir(folder_path):
            logging.warning(f"La carpeta especificada no existe: {folder_path}")
        
        self.folder_path = folder_path
        
        if state_file_path is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            state_file_path = os.path.join(current_dir, '..', 'settings', 'file_state.json')
            state_file_path = os.path.normpath(state_file_path)
        
        self.state_file_path = state_file_path
        
        # MEJORA: Cachear el estado actual para evitar múltiples lecturas
        self._current_files_cache = None
    
    def _load_last_state(self) -> Dict[str, List[str]]:
        try:
            if not os.path.exists(self.state_file_path):
                return {}
            
            with open(self.state_file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            logging.error(f"No se pudo leer el archivo de estado '{self.state_file_path}': {e}")
            return {}
    
    def _save_current_state(self, state: Dict[str, List[str]]) -> None:
        try:
            state_dir = os.path.dirname(self.state_file_path)
            if state_dir and not os.path.exists(state_dir):
                os.makedirs(state_dir, exist_ok=True)
            
            with open(self.state_file_path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=4, ensure_ascii=False)
        except IOError as e:
            logging.error(f"No se pudo escribir el archivo de estado '{self.state_file_path}': {e}")
    
    def _get_current_files(self) -> List[str]:
        """Obtiene la lista actual de archivos (con caché)."""
        if self._current_files_cache is not None:
            return self._current_files_cache
        
        current_files = []
        if os.path.exists(self.folder_path):
            for dirpath, _, filenames in os.walk(self.folder_path):
                for filename in filenames:
                    full_path = os.path.join(dirpath, filename)
                    current_files.append(full_path)
        
        self._current_files_cache = current_files
        return current_files
    
    def check_for_new_files(self, save_state: bool = True) -> Tuple[bool, List[str]]:
        """
        Detecta archivos nuevos comparando con el estado guardado.
        
        Args:
            save_state: Si es True, actualiza el archivo de estado inmediatamente
        
        Returns:
            Tupla (tiene_archivos_nuevos, lista_archivos_nuevos)
        """
        current_files = self._get_current_files()
        
        full_state = self._load_last_state()
        last_files_set = set(full_state.get(self.folder_path, []))
        current_files_set = set(current_files)
        
        new_files = sorted(list(current_files_set - last_files_set))
        
        # CRÍTICO: Solo actualizar el estado si se solicita explícitamente
        if save_state:
            full_state[self.folder_path] = sorted(current_files)
            self._save_current_state(full_state)
            logging.debug(f"Estado guardado para: {self.folder_path}")
        
        return len(new_files) > 0, new_files


# =================================================================================================
# SECCIÓN 2: ACCESO A DATOS (database_manager integrado)
# =================================================================================================

class DatabaseManager:
    """Gestiona consultas a la base de datos PostgreSQL."""
    
    def __init__(self, db_config: dict):
        self.db_config = db_config
    
    def _get_connection(self):
        return psycopg2.connect(
            host=self.db_config.get('host') or 'localhost',
            port=int(self.db_config.get('port') or '5432'),
            database=self.db_config.get('dbname') or 'canahuate_db',
            user=self.db_config.get('user') or 'postgres',
            password=self.db_config.get('password') or 'postgres'
        )
    
    def get_error_count(self) -> Optional[int]:
        sql_query = "SELECT COUNT(*) FROM public.validation_error_by_rules WHERE DATE(created_at) = %s;"
        connection = None
        try:
            connection = self._get_connection()
            with connection.cursor() as cursor:
                today = date.today()
                cursor.execute(sql_query, (today,))
                result = cursor.fetchone()
                count = result[0] if result else 0
                logging.info(f"Se encontraron {count} errores de validación para {today}.")
                return count
        except (psycopg2.Error, Exception) as e:
            logging.error(f"Error en consulta de errores: {type(e).__name__}: {e}")
            return None
        finally:
            if connection:
                connection.close()
    
    def get_excluded_periods_count(self) -> Optional[int]:
        sql_query = "SELECT COUNT(*) FROM public.excluded_data_logs WHERE DATE(changed_at) = %s;"
        connection = None
        try:
            connection = self._get_connection()
            with connection.cursor() as cursor:
                today = date.today()
                cursor.execute(sql_query, (today,))
                result = cursor.fetchone()
                count = result[0] if result else 0
                logging.info(f"Se encontraron {count} exclusiones para {today}.")
                return count
        except (psycopg2.Error, Exception) as e:
            logging.error(f"Error en consulta de exclusiones: {type(e).__name__}: {e}")
            return None
        finally:
            if connection:
                connection.close()


# =================================================================================================
# SECCIÓN 3: NOTIFICACIÓN POR EMAIL
# =================================================================================================

class ConfiguradorSMTP:
    """Carga y valida la configuración SMTP desde variables de entorno."""
    
    def __init__(self):
        self.servidor = os.getenv('SMTP_SERVER') or 'smtp.gmail.com'
        self.puerto = int(os.getenv('SMTP_PORT') or '587')
        self.usuario = os.getenv('SMTP_USER') or ''
        self.contrasena = os.getenv('SMTP_PASSWORD') or ''
        self.emails_auditores = self._parsear_lista_emails(os.getenv('AUDITOR_EMAILS') or '')
        
        self._validar_configuracion()
    
    def _parsear_lista_emails(self, emails_str: str) -> List[str]:
        if not emails_str:
            return []
        try:
            return json.loads(emails_str)
        except json.JSONDecodeError:
            return [e.strip() for e in emails_str.split(',') if e.strip()]
    
    def _validar_configuracion(self) -> None:
        faltantes = []
        if not self.servidor:
            faltantes.append('SMTP_SERVER')
        if not self.usuario:
            faltantes.append('SMTP_USER')
        if not self.contrasena:
            faltantes.append('SMTP_PASSWORD')
        
        if faltantes:
            logging.warning(f"Configuración SMTP incompleta. Faltan: {', '.join(faltantes)}")
            logging.warning("Los emails NO serán enviados.")
    
    def esta_configurado(self) -> bool:
        return bool(self.servidor and self.usuario and self.contrasena)


class NotificadorPlanta:
    """Encapsula la lógica de notificación para una planta específica."""
    
    def __init__(self, nombre_planta: str, ruta_archivos: str, 
                 email_contacto: str, db_manager: DatabaseManager):
        self.nombre_planta = nombre_planta
        self.ruta_archivos = ruta_archivos
        self.email_contacto = email_contacto
        self.db_manager = db_manager
        
        # MEJORA: Reutilizar el mismo monitor para evitar doble detección
        self.monitor = FileMonitor(self.ruta_archivos)
        
        self.tiene_archivos = False
        self.archivos_nuevos = []
        self.cantidad_errores = 0
        self.cantidad_exclusiones = 0
    
    def detectar_archivos(self, save_state: bool = False) -> None:
        """
        Detecta archivos nuevos.
        
        Args:
            save_state: Si es True, guarda el estado inmediatamente
        """
        try:
            self.tiene_archivos, self.archivos_nuevos = self.monitor.check_for_new_files(save_state=save_state)
            logging.info(f"{self.nombre_planta}: {'Sí' if self.tiene_archivos else 'No'} hay archivos nuevos "
                        f"({len(self.archivos_nuevos)} detectados)")
        except Exception as e:
            logging.error(f"Error detectando archivos para {self.nombre_planta}: {e}")
    
    def guardar_estado(self) -> None:
        """Guarda el estado actual de archivos sin volver a detectar."""
        try:
            # Cargar el estado actual
            full_state = self.monitor._load_last_state()
            
            # Obtener archivos actuales (usa caché si existe)
            current_files = self.monitor._get_current_files()
            
            # Actualizar solo esta planta
            full_state[self.ruta_archivos] = sorted(current_files)
            
            # Guardar
            self.monitor._save_current_state(full_state)
            logging.info(f"Estado guardado para {self.nombre_planta}")
        except Exception as e:
            logging.error(f"Error guardando estado para {self.nombre_planta}: {e}")
    
    def obtener_metricas_bd(self) -> None:
        try:
            self.cantidad_errores = self.db_manager.get_error_count() or 0
            self.cantidad_exclusiones = self.db_manager.get_excluded_periods_count() or 0
            logging.info(f"{self.nombre_planta}: {self.cantidad_errores} errores, "
                        f"{self.cantidad_exclusiones} exclusiones")
        except Exception as e:
            logging.error(f"Error obteniendo métricas para {self.nombre_planta}: {e}")
    
    def generar_html_email(self) -> str:
        error_count_is_positive = self.cantidad_errores > 0
        
        if not self.tiene_archivos:
            status_color = "#dc3545"
            icon = "⚠️"
            title = "Alerta: No se Detectaron Nuevos Archivos"
            files_section = "<p>No se encontraron archivos nuevos en la carpeta de la planta.</p>"
            correction_message = ""
        elif self.tiene_archivos and error_count_is_positive:
            status_color = "#ffc107"
            icon = "❌"
            title = "Alerta: Datos Recibidos con Errores"
            files_html = "".join(f'<li><code>{os.path.basename(f)}</code></li>' 
                               for f in self.archivos_nuevos)
            files_section = f"<h3>Archivos Nuevos Detectados:</h3><ul>{files_html}</ul>"
            correction_message = ('<p style="text-align: center; font-weight: bold; color: #b54a09;">'
                                'Por favor, revise y corrija los errores encontrados en los datos.</p>')
        else:
            status_color = "#28a745"
            icon = "✅"
            title = "Datos Recibidos y Validados Correctamente"
            files_html = "".join(f'<li><code>{os.path.basename(f)}</code></li>' 
                               for f in self.archivos_nuevos)
            files_section = f"<h3>Archivos Nuevos Detectados:</h3><ul>{files_html}</ul>"
            correction_message = ""
        
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; background-color: #f4f4f4; margin: 0; padding: 0; }}
                .container {{ max-width: 600px; margin: 20px auto; background: #fff; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); overflow: hidden;}}
                .header {{ background-color: {status_color}; color: white; padding: 25px; text-align: center; }}
                .header h1 {{ margin: 0; font-size: 24px; }}
                .header p {{ font-size: 18px; margin: 5px 0 0; }}
                .content {{ padding: 30px; }}
                .metrics {{ display: flex; justify-content: space-around; text-align: center; margin-top: 20px; padding: 15px; background: #f8f9fa; border-radius: 8px; }}
                .metric div:first-child {{ font-size: 16px; color: #555; }}
                .metric span {{ display: block; font-size: 28px; font-weight: bold; color: #333; }}
                ul {{ padding-left: 20px; }}
                code {{ background-color: #eee; padding: 2px 5px; border-radius: 4px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>{icon} {title}</h1>
                    <p>Planta: {self.nombre_planta}</p>
                </div>
                <div class="content">
                    {files_section}
                    {correction_message}
                    <div class="metrics">
                        <div class="metric">
                            <div>Errores en Datos</div>
                            <span>{self.cantidad_errores}</span>
                        </div>
                        <div class="metric">
                            <div>Períodos Excluidos</div>
                            <span>{self.cantidad_exclusiones}</span>
                        </div>
                        <div class="metric">
                            <div>Archivos Procesados</div>
                            <span>{len(self.archivos_nuevos)}</span>
                        </div>
                    </div>
                </div>
            </div>
        </body>
        </html>
        """
        return html_body
    
    def obtener_datos_auditoria(self) -> Dict[str, Any]:
        return {
            'plant_name': self.nombre_planta,
            'has_files': self.tiene_archivos,
            'new_files': self.archivos_nuevos,
            'error_count': self.cantidad_errores,
            'excluded_periods': self.cantidad_exclusiones
        }


class GestorEmailSMTP:
    """Gestiona el envío de correos electrónicos vía SMTP."""
    
    def __init__(self, config_smtp: ConfiguradorSMTP):
        self.config = config_smtp
    
    def enviar_email(self, destinatario: str, asunto: str, cuerpo_html: str) -> bool:
        if not self.config.esta_configurado():
            logging.warning(f"SMTP no configurado. Email a {destinatario} NO será enviado.")
            return False
        
        try:
            mensaje = MIMEMultipart('alternative')
            mensaje['From'] = self.config.usuario
            mensaje['To'] = destinatario
            mensaje['Subject'] = asunto
            
            parte_html = MIMEText(cuerpo_html, 'html', 'utf-8')
            mensaje.attach(parte_html)
            
            with smtplib.SMTP(self.config.servidor, self.config.puerto) as servidor:
                servidor.starttls()
                servidor.login(self.config.usuario, self.config.contrasena)
                servidor.send_message(mensaje)
            
            logging.info(f"✅ Email enviado exitosamente a {destinatario}")
            return True
        
        except smtplib.SMTPException as e:
            logging.error(f"Error SMTP al enviar a {destinatario}: {e}")
            return False
        except Exception as e:
            logging.error(f"Error inesperado al enviar email a {destinatario}: {e}")
            return False


class NotificadorAletheia:
    """Orquestador principal del sistema de notificaciones."""
    
    def __init__(self, config_db: Dict[str, Any]):
        self.config_smtp = ConfiguradorSMTP()
        self.gestor_smtp = GestorEmailSMTP(self.config_smtp)
        self.db_manager = DatabaseManager(config_db)
        
        self.notificadores_plantas = []
        self._inicializar_plantas()
    
    def _inicializar_plantas(self) -> None:
        plantas_config_str = os.getenv('PLANTAS_CONFIG')
        if not plantas_config_str:
            logging.warning("PLANTAS_CONFIG no definido en .env")
            return
        
        try:
            plantas = json.loads(plantas_config_str)
            for planta in plantas:
                notif = NotificadorPlanta(
                    nombre_planta=planta.get('nombre'),
                    ruta_archivos=planta.get('ruta'),
                    email_contacto=planta.get('email'),
                    db_manager=self.db_manager
                )
                self.notificadores_plantas.append(notif)
                logging.info(f"Planta registrada: {planta.get('nombre')}")
        except json.JSONDecodeError as e:
            logging.error(f"Error parseando PLANTAS_CONFIG: {e}")
    
    def enviar_notificaciones_plantas(self) -> None:
        """
        FLUJO CORREGIDO:
        1. Detectar archivos nuevos (sin guardar estado)
        2. Obtener métricas de BD
        3. Generar y enviar email
        4. Guardar estado DESPUÉS del envío exitoso
        """
        logging.info("="*70)
        logging.info("INICIANDO PROCESO DE NOTIFICACIONES")
        logging.info("="*70)
        
        for notif in self.notificadores_plantas:
            try:
                logging.info(f"\n--- Procesando: {notif.nombre_planta} ---")
                
                # 1. DETECTAR archivos (SIN guardar estado todavía)
                notif.detectar_archivos(save_state=False)
                
                # 2. OBTENER métricas de base de datos
                notif.obtener_metricas_bd()
                
                # 3. GENERAR email con datos completos
                cuerpo_html = notif.generar_html_email()
                asunto = f"Aletheia - Estado de Validación: {notif.nombre_planta}"
                
                # 4. ENVIAR email
                email_enviado = self.gestor_smtp.enviar_email(
                    destinatario=notif.email_contacto,
                    asunto=asunto,
                    cuerpo_html=cuerpo_html
                )
                
                # 5. GUARDAR estado solo si el email se envió correctamente
                if email_enviado:
                    notif.guardar_estado()
                else:
                    logging.warning(f"No se guardó el estado de {notif.nombre_planta} "
                                  "porque el email no se pudo enviar")
                
            except Exception as e:
                logging.error(f"❌ Error procesando notificación para {notif.nombre_planta}: {e}", 
                            exc_info=True)
        
        logging.info("\n" + "="*70)
        logging.info("PROCESO DE NOTIFICACIONES COMPLETADO")
        logging.info("="*70)
    
    def generar_html_auditoria(self, reportes: List[Dict[str, Any]]) -> str:
        today_str = date.today().strftime('%d/%m/%Y')
        
        rows_html = ""
        for r in reportes:
            error_count_is_positive = r["error_count"] is not None and r["error_count"] > 0
            
            if not r['has_files']:
                status_color = "#dc3545"
                status_text = "Faltante"
                icon = "⚠️"
            elif r['has_files'] and error_count_is_positive:
                status_color = "#ffc107"
                status_text = "Con Errores"
                icon = "❌"
            else:
                status_color = "#28a745"
                status_text = "Validado"
                icon = "✅"
            
            rows_html += f"""
            <tr>
                <td style="padding: 12px; border-bottom: 1px solid #ddd; color: {status_color}; text-align: center;"><strong>{icon} {status_text}</strong></td>
                <td style="padding: 12px; border-bottom: 1px solid #ddd;">{r["plant_name"]}</td>
                <td style="padding: 12px; border-bottom: 1px solid #ddd; text-align: center;">{len(r["new_files"])}</td>
                <td style="padding: 12px; border-bottom: 1px solid #ddd; text-align: center;">{r["error_count"] if r["error_count"] is not None else 'N/A'}</td>
                <td style="padding: 12px; border-bottom: 1px solid #ddd; text-align: center;">{r["excluded_periods"] if r["excluded_periods"] is not None else 'N/A'}</td>
            </tr>
            """
        
        return f"""
        <html>
        <head>
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; }}
                .container {{ max-width: 800px; margin: 20px auto; background: #fff; border: 1px solid #e9e9e9; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); }}
                .header {{ background: #0056b3; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
                .header h1 {{ margin: 0; }}
                .content {{ padding: 25px; }}
                table {{ width: 100%; border-collapse: collapse; }}
                th {{ background-color: #f2f2f2; padding: 12px; text-align: left; border-bottom: 2px solid #ddd; }}
                td {{ padding: 12px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Informe de Auditoria de Plantas</h1>
                    <p>Fecha: {today_str}</p>
                </div>
                <div class="content">
                    <table>
                        <thead>
                            <tr>
                                <th style="text-align: center;">Estado</th>
                                <th>Planta</th>
                                <th style="text-align: center;">Archivos</th>
                                <th style="text-align: center;">Errores</th>
                                <th style="text-align: center;">Excluidos</th>
                            </tr>
                        </thead>
                        <tbody>
                            {rows_html}
                        </tbody>
                    </table>
                </div>
            </div>
        </body>
        </html>
        """
    
    def enviar_resumen_auditores(self) -> None:
        logging.info("\n--- Enviando resumen de auditoría a auditores ---")
        
        if not self.config_smtp.emails_auditores:
            logging.warning("No hay emails de auditores configurados.")
            return
        
        reportes = [n.obtener_datos_auditoria() for n in self.notificadores_plantas]
        cuerpo_html = self.generar_html_auditoria(reportes)
        
        for email_auditor in self.config_smtp.emails_auditores:
            self.gestor_smtp.enviar_email(
                destinatario=email_auditor,
                asunto="Aletheia - Informe de Auditoría Diario",
                cuerpo_html=cuerpo_html
            )


def main():
    """Punto de entrada del módulo."""
    logging.info("\n🚀 INICIANDO EMAIL_NOTIFIER.PY")
    
    config_db = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD")
    }
    
    try:
        notificador = NotificadorAletheia(config_db)
        notificador.enviar_notificaciones_plantas()
        notificador.enviar_resumen_auditores()
        logging.info("\n✅ Proceso completado exitosamente")
    except Exception as e:
        logging.critical(f"\n❌ Error crítico en el proceso principal: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()