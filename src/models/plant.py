import os
import re
import unicodedata

class Plant:
    def __init__(self, config_dict: dict):
        self.name = config_dict.get('nombre', 'Sin Nombre')
        
        # 1. LA FUENTE DE VERDAD AHORA ES EL ID DEL JSON
        self.id = config_dict.get('id')
        if not self.id:
            self.id = self._normalize_name(self.name)
        
        self.db_host = config_dict.get('db_host', 'localhost')
        self.db_port = config_dict.get('db_port', '5432')
        self.db_name = config_dict.get('db_name')
        self.db_user = config_dict.get('db_user')
        self.db_password = config_dict.get('db_password')
        
        self.gs_sheet = config_dict.get('gs_sheet', '')
        self.emails = config_dict.get('emails', [])
        
        # 2. RUTAS BASADAS ESTRICTAMENTE EN EL ID
        self.input_path = os.path.join('data', self.id, 'input')
        self.output_path = os.path.join('data', self.id, 'output')
        self.img_path = os.path.join('data', self.id, 'imgs')
        self.rules_path = os.path.join('config', 'plants', self.id, 'rules.json')
        
        self.graficas_link = os.getenv(f"GRAFICAS_LINK_{self.id}", "#")

    def _normalize_name(self, name: str) -> str:
        name = ''.join((c for c in unicodedata.normalize('NFD', name.lower()) if unicodedata.category(c) != 'Mn'))
        return re.sub(r'[^a-z0-9]', '', name)

    def __repr__(self):
        return f"<Plant: {self.name} | DB: {self.db_name} | ID: {self.id}>"