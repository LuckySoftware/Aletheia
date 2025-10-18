@echo off
setlocal

REM =========================================================================
REM SCRIPT DE EJECUCIÓN AUTOMATIZADA DE ALTO RENDIMIENTO
REM =========================================================================
REM
REM Características:
REM   - CERO PAUSAS: Ejecución continua para máxima velocidad
REM   - DINÁMICO: Detecta automáticamente el nombre del contenedor
REM   - EFICIENTE: Usa start y stop para un ciclo rápido de contenedores
REM   - RESPALDADO: Incluye persistencia de datos antes del apagado
REM   - AUTÓNOMO: Diseñado para Task Scheduler sin intervención
REM
REM =========================================================================

ECHO [ %TIME% ] INICIO DEL PROCESO AUTOMATIZADO.
cd /d %~dp0

REM --- PASO 1: INICIO RÁPIDO DE SERVICIOS ---
ECHO [ %TIME% ] Iniciando servicios de Docker en segundo plano...
docker-compose start >nul

REM Pequeña pausa para que los contenedores se estabilicen
timeout /t 2 /nobreak >nul

REM --- PASO 2: DETECCIÓN DINÁMICA DEL CONTENEDOR ---
ECHO [ %TIME% ] Detectando nombre del contenedor del servicio 'app'...
FOR /f "tokens=*" %%a IN ('docker-compose ps -q app') DO (SET CONTAINER_ID=%%a)

IF NOT DEFINED CONTAINER_ID (
    ECHO [ %TIME% ] ERROR: No se pudo encontrar el contenedor del servicio 'app'. Abortando.
    goto :cleanup
)

ECHO [ %TIME% ] Contenedor detectado: %CONTAINER_ID%

REM --- PASO 3: EJECUCIÓN SECUENCIAL DE SCRIPTS ---
ECHO [ %TIME% ] --- Ejecutando la carga de trabajo ---
docker exec %CONTAINER_ID% python /app/src/core/1_create_database.py
docker exec %CONTAINER_ID% python /app/src/core/2_load_exclusiones.py
docker exec %CONTAINER_ID% python /app/src/core/3_load_rules_from_json.py
docker exec %CONTAINER_ID% python /app/src/core/4_load_csv.py
docker exec %CONTAINER_ID% python /app/src/core/5_handle_duplicates.py
docker exec %CONTAINER_ID% python /app/src/core/6_validate_data.py
docker exec %CONTAINER_ID% python /app/src/core/7_delete_exclusions.py
docker exec %CONTAINER_ID% python /app/src/core/8_export_to_excel.py
ECHO [ %TIME% ] --- Carga de trabajo finalizada ---

REM --- PASO 4: PERSISTENCIA DE DATOS (BACKUP) ---
ECHO [ %TIME% ] --- Iniciando persistencia de datos ---
REM Navega a la carpeta tools donde está maintenance.bat
cd /d "%~dp0tools"
REM Ejecuta el script de mantenimiento/backup
call maintenance.bat
REM Regresa al directorio principal
cd /d "%~dp0"
ECHO [ %TIME% ] --- Persistencia de datos completada ---

:cleanup
REM --- PASO 5: DETENCIÓN DE SERVICIOS ---
ECHO [ %TIME% ] Deteniendo servicios de Docker para liberar recursos...
docker-compose stop >nul
ECHO [ %TIME% ] Servicios detenidos.

ECHO [ %TIME% ] PROCESO AUTOMATIZADO COMPLETADO.
endlocal