# Aletheia — Data Validator

![project-6](https://github.com/user-attachments/assets/c2babb71-efa1-4e29-9fc5-ec199e9b4064)

Resumen
-------
Aletheia es una colección de scripts Python orientados a la validación, limpieza y preparación de datos instrumentales (CSV/Excel) para su posterior ingestión o análisis. Aplica reglas declarativas (desde JSON), gestiona exclusiones (via Google Sheets o archivos Excel), detecta y gestiona duplicados, y almacena resultados intermedios en PostgreSQL. Genera reportes en Excel con los errores y los datos validados.

Este README describe la estructura real del proyecto, las variables de entorno requeridas, el pipeline por pasos y las opciones de despliegue (Docker / Docker Compose).

Índice
- Características
- Requisitos
- Instalación (entorno local)
- Archivo `.env` (variables y ejemplos)
- Pipeline (scripts en `src/core`)
- Estructura del proyecto
- Docker / docker-compose
- Notas de operación y mantenimiento
- Contacto y licencia

Características
---------------
- Reglas declarativas: `src/settings/rules.json` se carga en la tabla `validation_rules` via `src/core/3_load_rules_from_json.py`.
- Lectura de CSV: `src/core/4_load_csv.py` detecta las columnas de `raw_data` y carga solo filas limpias y numéricas.
- Exclusiones: `src/core/2_load_exclusiones.py` extrae rangos desde Google Sheets y escribe en la tabla `excluded_data`.
- Detección y gestión de duplicados: `src/core/5_handle_duplicates.py` mueve duplicados a `duplicated_data` y los elimina de `raw_data`.
- Validación: `src/core/6_validate_data.py` (documentar reglas) aplica las reglas por columna y persiste errores en `validation_error_by_rules`.
- Exportes: `src/core/8_export_to_excel.py` genera reportes en `data/output`.

Requisitos
----------
- Python 3.10+
- PostgreSQL (puede usarse el servicio definido en `docker-compose.yaml`)
- pip
- (Opcional) Docker y Docker Compose para despliegues o pruebas reproducibles

Instalación (Windows - PowerShell)
---------------------------------
```powershell
git clone <repositorio>
cd aletheia
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Archivo `.env` (valores clave)
------------------------------
Coloca un `.env` en la raíz del proyecto. Ejemplo mínimo con las variables usadas en los scripts:

```dotenv
# PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=aletheia_db
DB_USER=postgres
DB_PASSWORD=postgres

# Esquema y pipeline
NUM_COLUMNS=30      # Número de columnas de medida (col_1 .. col_N)
P_MAX=1000          # Parámetro inicial de planta (se inserta en plant_parameters)

# Carga de CSV
CSV_DIRECTORY=./data/input
CSV_SEPARATOR=;
CSV_ENCODING=windows-1252

# Exclusiones (Google Sheets)
GOOGLE_SHEET_NAME=MiHojaDeExclusiones
WORKSHEET_NAME=Sheet1
GOOGLE_CREDENTIALS_FILE=credentials.json  # ruta relativa bajo /app (ej: src/settings/credentials.json)

# Contenedor
KEEP_ALIVE=false

```

Notas sobre variables
- `NUM_COLUMNS` es requerida por `src/core/1_create_database.py` para construir columnas `col_1..col_N`.
- `P_MAX` se usa para insertar parámetros iniciales en la tabla `plant_parameters`.
- `GOOGLE_CREDENTIALS_FILE` debe apuntar a un archivo JSON con credenciales de service account con acceso a la hoja.

Pipeline (scripts y orden recomendable)
-------------------------------------
Los scripts están en `src/core/` y están numerados para ejecutar el pipeline por pasos. Ejecuta cada paso desde la raíz del proyecto (con `.env` presente) o dentro del contenedor.

1) Crear esquema y tablas (ejecutar 1 vez):

```powershell
python src/core/1_create_database.py
```

2) Cargar exclusiones desde Google Sheets (opcional, cuando haya exclusiones que registrar):

```powershell
python src/core/2_load_exclusiones.py
```

3) Cargar reglas desde JSON a la BD (cuando actualices `src/settings/rules.json`):

```powershell
python src/core/3_load_rules_from_json.py
```

4) Cargar CSVs (limpia e inserta en `raw_data`):

```powershell
python src/core/4_load_csv.py
```

5) Detectar y mover duplicados:

```powershell
python src/core/5_handle_duplicates.py
```

6) Ejecutar validaciones por reglas (genera entradas en `validation_error_by_rules` y `validated_data`):

```powershell
python src/core/6_validate_data.py
```

7) Eliminar filas según reglas de exclusión (si procede):

```powershell
python src/core/7_delete_exclusions.py
```

8) Exportar reportes a Excel (errores y datos validados):

```powershell
python src/core/8_export_to_excel.py
```

Atajos y utilidades
- `tool/tool-inicio.bat` y `tool/tool.bat` contienen menús / ejecuciones automatizadas para Windows.

Estructura del proyecto (extracto)
---------------------------------
```
aletheia/
├─ README.md
├─ requirements.txt
├─ docker-compose.yaml
├─ Dockerfile
├─ .env (no versionado)
├─ data/
│  ├─ input/
│  ├─ output/
│  └─ backup/
├─ scripts/
│  └─ entrypoint.sh
├─ src/
│  ├─ core/
│  │  ├─ 1_create_database.py
│  │  ├─ 2_load_exclusiones.py
│  │  ├─ 3_load_rules_from_json.py
│  │  ├─ 4_load_csv.py
│  │  ├─ 5_handle_duplicates.py
│  │  ├─ 6_validate_data.py
│  │  ├─ 7_delete_exclusions.py
│  │  └─ 8_export_to_excel.py
│  └─ settings/
│     ├─ api_credentials.json
│     └─ rules.json
└─ tool/
   ├─ tool-inicio.bat
   └─ tool.bat
```

Docker y orquestación
----------------------
El repositorio incluye `Dockerfile` y `docker-compose.yaml` para pruebas y despliegue. Resumen:
- `docker-compose.yaml` define dos servicios: `db` (Postgres) y `app` (construida desde `Dockerfile`).
- `app` monta `./src` y `./scripts` en `/app` y usa un `entrypoint` (`/app/scripts/entrypoint.sh`) que espera a la DB y ejecuta `1_create_database.py`.

Comandos básicos (PowerShell):
```powershell
docker-compose up --build -d
docker-compose logs -f app
docker-compose down
```

Notas operativas
----------------
- Asegúrate de tener `.env` con credenciales antes de ejecutar los scripts o el contenedor.
- `src/core/4_load_csv.py` espera archivos CSV en `CSV_DIRECTORY` (configurable) y usa `;` y `windows-1252` por defecto.
- `src/core/2_load_exclusiones.py` utiliza Google Sheets; configura `GOOGLE_CREDENTIALS_FILE` y permisos de la service account.
- Los scripts registran mensajes por consola; revisa `data/output` para resultados y `data/backup` para respaldos.
- No versionar `.env` ni credenciales.

Problemas conocidos y supuestos
-------------------------------
- Muchos scripts usan rutas absolutas bajo `/app` asumiendo ejecución dentro de contenedor; en local ejecuta desde la raíz del proyecto y garantiza que `.env` exista.
- El script `1_create_database.py` requiere `NUM_COLUMNS` y `P_MAX` en el entorno.
