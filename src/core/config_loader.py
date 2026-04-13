import os
import json
import logging
from pathlib import Path
from dotenv import load_dotenv
from src.models.plant import Plant

class ConfigLoader:
    def __init__(self, json_path: str = "config.json"):
        # 1. Cargar el .env global automáticamente
        load_dotenv()
        
        self.json_path = json_path
        self.plants = []
        self.auditor_emails = []
        self.smtp_config = {}
        
        self.creds_path = Path("config/api_credentials.json")
        
        # 2. Inicializar configuraciones
        self._load_env_globals()
        self._load_plants()

    def _load_env_globals(self):
        """Carga las variables globales del .env (Correos y SMTP)"""
        self.smtp_config = {
            'server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
            'port': int(os.getenv('SMTP_PORT', '587')),
            'user': os.getenv('SMTP_USER', ''),
            'password': os.getenv('SMTP_PASSWORD', '')
        }
        
        raw_emails = os.getenv('AUDITOR_EMAILS', '[]')
        try:
            self.auditor_emails = json.loads(raw_emails)
        except json.JSONDecodeError:
            self.auditor_emails = [e.strip() for e in raw_emails.split(',') if e.strip()]

    def _load_plants(self):
        """Lee el config.json e instancia objetos Plant"""
        if not os.path.exists(self.json_path):
            logging.error(f"FATAL: Archivo de configuración no encontrado: {self.json_path}")
            return
            
        try:
            with open(self.json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                plant_dicts = data.get('plantas', [])
                
                for p_dict in plant_dicts:
                    self.plants.append(Plant(p_dict))
                    
            logging.info(f"ConfigLoader: {len(self.plants)} plantas cargadas exitosamente.")
        except json.JSONDecodeError as e:
            logging.error(f"FATAL: Error parseando {self.json_path}: {e}")