@echo off
setlocal

REM =========================================================================
REM SCRIPT DE EJECUCIÓN AUTOMATIZADA - VERSIÓN CORREGIDA
REM =========================================================================
REM
REM CAMBIOS CLAVE:
REM   - ELIMINADA la ejecución manual de scripts (docker exec)
REM   - El ENTRYPOINT del contenedor ejecuta TODO el pipeline
REM   - run.bat solo orquesta el ciclo de vida del contenedor
REM   - SOLUCIONA el problema de doble envío de emails
REM
REM =========================================================================

ECHO.
ECHO ========================================================================
ECHO   ALETHEIA PIPELINE - EJECUCION AUTOMATIZADA
ECHO ========================================================================
ECHO [ %TIME% ] INICIO DEL PROCESO
ECHO.

cd /d %~dp0

REM --- PASO 1: RECONSTRUIR IMAGEN (Opcional, descomentar si cambias código) ---
REM ECHO [ %TIME% ] Reconstruyendo imagen Docker...
REM docker-compose build --no-cache
REM ECHO [ %TIME% ] Imagen reconstruida.
REM ECHO.

REM --- PASO 2: INICIAR SERVICIOS ---
ECHO [ %TIME% ] Iniciando servicios Docker...
ECHO [ %TIME% ] (El ENTRYPOINT ejecutará automáticamente todo el pipeline)
ECHO.

docker-compose up -d

IF %ERRORLEVEL% NEQ 0 (
    ECHO [ %TIME% ] ERROR: Fallo al iniciar servicios Docker.
    ECHO [ %TIME% ] Verifique docker-compose.yml y .env
    goto :error_exit
)

ECHO [ %TIME% ] Contenedor iniciado. Pipeline en ejecución...
ECHO.

REM --- PASO 3: MONITOREAR LOGS EN TIEMPO REAL (Opcional) ---
ECHO [ %TIME% ] Para ver los logs en tiempo real, ejecuta en otra terminal:
ECHO [ %TIME% ]   docker logs -f canahuate_app_service
ECHO.

REM --- PASO 4: ESPERAR A QUE EL PIPELINE TERMINE ---
ECHO [ %TIME% ] Esperando finalización del pipeline...
docker wait canahuate_app_service

IF %ERRORLEVEL% NEQ 0 (
    ECHO [ %TIME% ] ADVERTENCIA: El pipeline terminó con errores.
    ECHO [ %TIME% ] Revisa los logs: docker logs canahuate_app_service
) ELSE (
    ECHO [ %TIME% ] Pipeline completado exitosamente.
)
ECHO.

REM --- PASO 5: PERSISTENCIA DE DATOS (BACKUP) ---
ECHO [ %TIME% ] --- Iniciando persistencia de datos ---
cd /d "%~dp0tools"

IF EXIST backup.bat (
    call backup.bat
    ECHO [ %TIME% ] Backup completado.
) ELSE (
    ECHO [ %TIME% ] ADVERTENCIA: backup.bat no encontrado en tools/
)

cd /d "%~dp0"
ECHO.

REM --- PASO 6: LIMPIEZA ---
ECHO [ %TIME% ] Deteniendo y eliminando contenedores...
docker-compose down

ECHO.
ECHO ========================================================================
ECHO [ %TIME% ] PROCESO COMPLETADO EXITOSAMENTE
ECHO ========================================================================
ECHO.

REM --- MOSTRAR RESUMEN ---
ECHO Puedes revisar los logs completos con:
ECHO   docker logs canahuate_app_service
ECHO.
ECHO Archivos generados en:
ECHO   - ./data/output/
ECHO   - ./data/backup/
ECHO.

goto :end

:error_exit
ECHO.
ECHO ========================================================================
ECHO [ %TIME% ] ERROR CRITICO - PROCESO ABORTADO
ECHO ========================================================================
ECHO.
docker-compose down
exit /b 1

:end
endlocal
exit /b 0