# ![project-6](https://github.com/user-attachments/assets/c2babb71-efa1-4e29-9fc5-ec199e9b4064)

# Aletheia — Data Validator & Pipeline V2.0

---

## Resumen

**Aletheia V2.0** es una pipeline automatizado y motor de validación diseñado para garantizar la integridad, calidad y trazabilidad de los datos instrumentales en plantas solares. Su objetivo principal es automatizar de forma segura el ciclo de vida completo de la información: desde la extracción inteligente de archivos de campo (SCADA), pasando por la limpieza de anomalías, hasta el almacenamiento histórico y la notificación gerencial via email.

A diferencia de sus predecesores, esta versión introduce una **arquitectura modular orientada a servicios** que permite configurar y operar múltiples plantas en paralelo de forma centralizada. Mediante el uso de reglas de validación declarativas (JSON), algoritmos de clasificación solar dinámica y un gestor de estados inteligente (que memoriza y hereda métricas en periodos sin actividad), Aletheia V2.0 reduce drásticamente el "ruido" de falsas alarmas. El resultado es un pipeline robusto que optimiza el tiempo de los involucrados en información y análisis superficial de datos, asegurando que solo datos confiables y auditables lleguen a la fase de decisiones ejecutivas, asi como un dashboard semanal de estado y todo su ciclo de software.

---

## Índice

- Características Principales  
- Requisitos  
- Instalación  
- Configuración (.env y config.json)  
- Arquitectura del Pipeline V2.0  
- Estructura del Proyecto  
- Operación y Mantenimiento  

---

## Características Principales

- Arquitectura Orientada a Servicios  
- Soporte Multi-Planta  
- Clasificación Solar Dinámica  
- Gestión de Estados Inteligente  
- Reportes de Auditoría Global  
- Orquestación Maestra  

---

## Requisitos

- Python 3.10+
- PostgreSQL 15+
- PowerShell 5.1+
- Google Cloud Console

---

## Instalación

```powershell
git clone <repositorio>
cd aletheia-v2
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## Configuración

### .env

```env
# Credenciales SMTP
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=tu-servicio@empresa.com
SMTP_PASSWORD="tu-app-password"

# Auditores (Lista JSON en UNA sola línea)
AUDITOR_EMAILS=["auditor1@empresa.com", "auditor2@empresa.com"]

# Enlaces a Drive (Para reportes visuales)
GRAFICAS_LINK_planta1="[https://drive.google.com/](https://drive.google.com/)..."
```

### config.json

```json
{
  "plantas": [
    {
      "nombre": "Planta Norte",
      "id": "planta_norte",
      "sync_src": "\\\\servidor\\scada\\datos",
      "storage_dest": "C:\\Unidades\\Reportes\\Norte",
      "db_host": "localhost",
      "db_port": "5432",
      "db_name": "db_norte",
      "db_user": "usuario",
      "db_password": "password",
      "gs_sheet": "EXCLUSIONES_NORTE",
      "emails": ["jefe_planta@empresa.com"]
    }
  ]
}
```

---

## Arquitectura


El flujo de trabajo es orquestado por run_pipeline.bat y sigue este orden:

1. Sincronización (sync.ps1): Descarga inteligente de archivos CSV desde la red, omitiendo archivos ya procesados.

2. Ejecución Core (main.py):

 2.1 Extractor: Lectura robusta de CSV y carga en raw_data.

 2.2 ExclusionManager: Sincronización de exclusiones desde Google Sheets.

 2.3 Validator: Aplicación de reglas y clasificación solar.

 2.4 Exporter: Generación de reportes Excel de validados y errores.

 2.5 Notifier: Envío de correos diarios con KPIs a los jefes de planta.

3. Auditoría Global (auditor_report.py): Envío de reporte consolidado semanal a los auditores globales.

4. Mantenimiento (backup.bat): Respaldo .tar de bases de datos y purga de tablas de trabajo.

5. Organización (organizer.bat y storage.ps1): Archivado local por fecha y traslado final a la unidad de almacenamiento en red.

---

## Estructura

```plaintext
Aletheia V2.0/
├─ config.json              # Configuración multi-planta
├─ run_pipeline.bat         # Orquestador Maestro
├─ src/
│  ├─ main.py               # Punto de entrada Python
│  ├─ auditor_report.py     # Módulo de reporte global
│  ├─ core/
│  │  ├─ database.py        # Gestor de conexiones PostgreSQL
│  │  ├─ pipeline.py        # Lógica de estados y orquestación core
│  │  └─ config_loader.py   # Cargador de .env y JSON
│  ├─ services/
│  │  ├─ extractor.py       # ETL de CSVs
│  │  ├─ validator.py       # Motor de validación
│  │  ├─ exporter.py        # Generador de Excel
│  │  ├─ notifier.py        # Alertas por Email
│  │  └─ state_manager.py   # Persistencia de estados
│  └─ utils/
│     ├─ helpers.py         # Sanitización y robustez de lectura
│     └─ validation_tools.py# Clasificador solar y bypass handler
├─ tools/
│  ├─ sync.ps1              # PowerShell Sync (Robocopy wrapper)
│  ├─ storage.ps1           # Traslado a unidad de red
│  ├─ backup.bat            # Dump de base de datos
│  └─ organizer.bat         # Gestión de carpetas temporales
└─ data/
   └─ [plant_id]/           # Estructura aislada por planta
      ├─ input/
      ├─ output/
      ├─ archive/
      └─ backup/
```

---

## Operación y Mantenimiento

- Nuevas Plantas: Solo requiere añadir el objeto correspondiente en config.json y crear su carpeta en config/plants/[id] con el archivo rules.json.

- Logs: La salida de consola está estandarizada por planta. En caso de error crítico, el orquestador maestro interrumpe el flujo para evitar corrupción de datos.

- Backups: Se mantienen los últimos 5 respaldos de base de datos (.tar) por planta automáticamente gracias al script de mantenimiento.

---
