@ECHO OFF
SETLOCAL ENABLEDELAYEDEXPANSION

:: 1. Configuración de Directorio
CD /D "%~dp0"
ECHO ====================================================
ECHO == ALETHEIA PIPELINE V2.0 - ORQUESTADOR MAESTRO   ==
ECHO ====================================================
ECHO Iniciado el: %DATE% a las %TIME%
ECHO.

:: --- PASO 1: SINCRONIZACIÓN INTELIGENTE ---
ECHO [PASO 1/6] Sincronizando archivos desde la red...
ECHO ----------------------------------------------------
powershell -NoProfile -ExecutionPolicy Bypass -File "tools\sync.ps1"
IF %ERRORLEVEL% GEQ 8 (
    ECHO [ERROR] Fallo critico en Robocopy durante la sincronizacion.
    GOTO :ErrorExit
)
ECHO [OK] Sincronizacion completada.
ECHO.

:: --- PASO 2: EJECUCIÓN DEL PIPELINE (COMO MÓDULO) ---
ECHO [PASO 2/6] Ejecutando Pipeline Principal (src.main)...
ECHO ----------------------------------------------------
python -m src.main
IF %ERRORLEVEL% NEQ 0 (
    ECHO [ERROR] El pipeline de datos encontro un fallo critico.
    GOTO :ErrorExit
)
ECHO [OK] Pipeline finalizado con exito.
ECHO.

:: --- PASO 3: REPORTE PARA AUDITORES ---
ECHO [PASO 3/6] Generando reporte global de auditoria...
ECHO ----------------------------------------------------
python -m src.auditor_report
IF %ERRORLEVEL% NEQ 0 (
    ECHO [ADVERTENCIA] El reporte de auditoria no pudo enviarse.
) ELSE (
    ECHO [OK] Reporte enviado a los auditores.
)
ECHO.

:: --- PASO 4: BACKUP DE BASES DE DATOS ---
ECHO [PASO 4/6] Realizando backups y limpieza de DBs...
ECHO ----------------------------------------------------
CALL tools\backup.bat
IF %ERRORLEVEL% NEQ 0 (
    ECHO [ERROR] Fallo durante el proceso de Backup/Truncate.
    GOTO :ErrorExit
)
ECHO [OK] Bases de datos respaldadas y purgadas.
ECHO.

:: --- PASO 5: ORGANIZACIÓN DE ARCHIVOS ---
ECHO [PASO 5/6] Archivando archivos procesados localmente...
ECHO ----------------------------------------------------
CALL tools\organizer.bat
ECHO [OK] Archivos movidos a la carpeta archive.
ECHO.

:: --- PASO 6: ALMACENAMIENTO FINAL ---
ECHO [PASO 6/6] Moviendo historico a la red y liberando espacio...
ECHO ----------------------------------------------------
powershell -NoProfile -ExecutionPolicy Bypass -File "tools\storage.ps1"
IF %ERRORLEVEL% NEQ 0 (
    ECHO [ERROR] Fallo al mover los archivos al destino final.
    GOTO :ErrorExit
)
ECHO [OK] Proceso de almacenamiento finalizado.
ECHO.

:SuccessExit
ECHO ====================================================
ECHO ==   ORQUESTACION COMPLETA EXITOSA (ALETHEIA)     ==
ECHO ====================================================
EXIT /B 0

:ErrorExit
ECHO.
ECHO ****************************************************
ECHO ** ERROR: LA ORQUESTACION FUE INTERRUMPIDA       **
ECHO ****************************************************
EXIT /B 1