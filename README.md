> # *Aletheia, a data validator that will protect you from Dolos*

![Imagen del proyecto](https://github.com/user-attachments/assets/2a626e58-09f7-4cf3-9207-d5de3d67839d)

![Build Status](https://img.shields.io/badge/build-pipeline-yellow.svg)
![Coverage](https://img.shields.io/badge/coverage-90%25-brightgreen.svg)
![Version](https://img.shields.io/badge/version-0.1.0-orange.svg)

## Índice
- [Descripción del Proyecto](#descripción-del-proyecto)
- [Características Principales](#características-principales)
- [Instalación](#instalación)
- [Uso](#uso)
- [Tecnologías Utilizadas](#tecnologías-utilizadas)

## Descripción del Proyecto
Aletheia es una Python Tool basada en el tipo Data Validator, es una utilidad en Python para validar y procesar archivos CSV/Excel con datos instrumentales. Su objetivo es detectar, marcar y exportar registros que incumplen reglas definidas (rangos, exclusiones, duplicados), facilitando la limpieza y preparación de datos para análisis o carga en bases de datos.

El proyecto automatiza el flujo de: carga de reglas desde JSON, lectura de CSV/Excel, aplicación de validaciones por columna, manejo de exclusiones y duplicados, almacenamiento temporal en PostgreSQL y exportación de reportes Excel con errores y registros validados.

## Características Principales
- Validaciones por reglas: carga y aplicación de reglas definidas en `rules.json` (rangos, mensajes de error, activación).
- Soporte de formatos: lectura de CSV y Excel (pandas + openpyxl/xlrd) y exportación de reportes en Excel.
- Integración con PostgreSQL: scripts para crear la base de datos y almacenar resultados temporales (`1_create_database.py`).
- Gestión de exclusiones y duplicados: herramientas para cargar exclusiones desde Excel y eliminar duplicados antes de la validación.
- Flujo automatizado: scripts numerados (`1_...` a `8_...`) para montar pipelines reproducibles de procesamiento.

## Lógica de sistema
Solamente hay `9 inputs` hechos por el usuario. Dichos inputs se insertan al principio y nunca mas se vuelve a requerir esos inputs
al usuario, ya que quedan en el archivo `.env` para poder consumir las veces que sean necesarias.
- 1 :: `Nombre` para la base de datos
- 2 :: `Usuario` de base de datos (superusuario)
- 3 :: `Contraseña` para la base de datos
- 4 :: `Puerto` de la base de datos
- 5 :: `Cantidad de columnas por` `.csv` (para poder mapearlas posteriormente)
- 6 :: `Directorio de los .csv (crudos)` para poder consumir los datos 
- 7 :: `Directorio de reportes de ERROR` lugar de reportes para datos que violen reglas de `rules.json`
- 8 :: `Directorio de reportes de VALIDACION` aquí la herramienta dejará los reportes con los datos filtrados y validados
- 9 :: `Directorio de exclusiones` debe apuntar al directorio de exclusiones con archivos `.xlsx` que llena el jefe de planta 

## Instalación
Requisitos previos:
- Python 3.10.x
- PostgreSQL 17.x
- Git

## Árbol de directorios
Árbol de directorios del proyecto

```
DataValidator/
├─ README.md
├─ rules.json                      # archivo de reglas (editable)
├─ .env                            # archivo de credenciales (generado automaticamente)
├─ requirements/
│  └─ requirements.txt
├─ src/
│  ├─ 1_create_database.py         # creación de base de datos y tablas
│  ├─ 2_load_exclusiones.py        # carga de exclusiones y potencia pico (extracción de .xlsx)
│  ├─ 3_load_rules_from_json.py    # carga de reglas desde el archivo .json
│  ├─ 4_load_csv.py                # carga de .csv crudos provenientes del SCADA
│  ├─ 5_handle_duplicates.py       # eliminación de filas duplicadas en .csv
│  ├─ 6_validate_data.py           # validación de reglas 
│  ├─ 7_delete_exclusions.py       # eliminación de filsa con exclusiones en 0 
│  └─ 8_export_to_excel.py         # exportacion a excel de errores y excel de datos validados
└─ tool/
  ├─ tool-inicio.bat               # herramienta con menú integrado para poder realizar la instalación
  └─ tool.bat                      # herramienta para ejecución ciclica mediante cron o taskschd
```


1. Clona el repositorio:
```bash
git clone https://github.com/tu-usuario/aletheia.git
```
2. Entra en el directorio del proyecto:
```bash
cd aletheia
```
3. Crea y activa un entorno virtual (recomendado):
```bash
python -m venv .venv
.\.venv\Scripts\activate
```
4. Instala las dependencias:
```bash
pip install -r requirements/requirements.txt
```
5. Configura las variables de entorno:
- Crea un archivo `.env` en la raíz con las variables necesarias. Ejemplo (ya presente en el repositorio):

6. Inicializa la base de datos ejecutando:
```bash
python src/1_create_database.py
```

7. Ejecuta el pipeline de ejemplo (por pasos):
o puedes usar la herramienta automatica para ello en \tool
```bash
python src/3_load_rules_from_json.py
python src/4_load_csv.py
python src/5_handle_duplicates.py
python src/6_validate_data.py
python src/8_export_to_excel.py
```


## Uso
Flujos típicos:

- Validación por carpetas: coloca tus CSVs en la ruta indicada por `CSV_DIRECTORY` en `.env` y ejecuta:
```bash
python src/4_load_csv.py
python src/6_validate_data.py
```

- Exportar resultados a Excel:
```bash
python src/8_export_to_excel.py
```

- Cargar reglas personalizadas:
```bash
# Edita `rules.json` y luego:
python src/3_load_rules_from_json.py
```

Los scripts están numerados para permitir ejecución paso a paso del pipeline. Cada script imprime logs básicos en consola y genera archivos Excel en los directorios configurados.

## Tecnologías Utilizadas
- Lenguaje: Python 3.10+
- Librerías principales:
  - pandas
  - python-dotenv
  - psycopg2-binary
  - openpyxl, xlrd
- Base de datos: PostgreSQL
- Herramientas de desarrollo: pytest (tests opcionales), mypy, black, isort

## Contacto
- Autor: Luciano Moreira
