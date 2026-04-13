@ECHO OFF
SETLOCAL ENABLEDELAYEDEXPANSION

:: Ir a la raíz del proyecto
CD /D "%~dp0.."

ECHO =============================================================
ECHO      ORGANIZADOR GLOBAL DE ARCHIVOS PROCESADOS
ECHO =============================================================

:: Usamos WMIC para obtener una cadena de fecha universal (Formato YYYY-MM-DD)
FOR /F "tokens=2 delims==" %%I IN ('wmic os get localdatetime /value') DO SET "datetime=%%I"
SET "FECHA_HOY=%datetime:~0,4%-%datetime:~4,2%-%datetime:~6,2%"

:: Validación de seguridad por si falla el comando del sistema
IF "!FECHA_HOY!"=="--" (
    SET "FECHA_HOY=error_fecha"
)
:: ----------------------------------------------------------

:: Recorrer plantas del JSON
FOR /F "tokens=*" %%I IN ('powershell -NoProfile -Command "$c = Get-Content 'config.json' | ConvertFrom-Json; $c.plantas.id"') DO (
    SET "P_ID=%%I"
    ECHO.
    ECHO --- Organizando Planta: !P_ID! ---
    
    SET "BASE_DIR=data\!P_ID!"
    SET "DEST_DIR=data\!P_ID!\archive\!FECHA_HOY!"
    
    :: 1. Crear estructura completa en archive
    IF NOT EXIST "!DEST_DIR!\input" MKDIR "!DEST_DIR!\input"
    IF NOT EXIST "!DEST_DIR!\reportes" MKDIR "!DEST_DIR!\reportes"
    IF NOT EXIST "!DEST_DIR!\graficas" MKDIR "!DEST_DIR!\graficas"
    IF NOT EXIST "!DEST_DIR!\backup" MKDIR "!DEST_DIR!\backup"

    :: 2. Mover INPUT (CSV procesados)
    IF EXIST "!BASE_DIR!\input\*.csv" (
        MOVE /Y "!BASE_DIR!\input\*.csv" "!DEST_DIR!\input\" >nul
    )

    :: 3. Mover OUTPUT (Solo Excels de reporte)
    IF EXIST "!BASE_DIR!\output\*.xlsx" (
        MOVE /Y "!BASE_DIR!\output\*.xlsx" "!DEST_DIR!\reportes\" >nul
    )
    
    :: 4. Mover IMÁGENES (Desde la ruta data\!P_ID!\imgs)
    IF EXIST "!BASE_DIR!\imgs\*" (
        MOVE /Y "!BASE_DIR!\imgs\*" "!DEST_DIR!\graficas\" >nul
    )

    :: 5. Mover BACKUPS (Archivos .tar creados por backup.bat)
    IF EXIST "!BASE_DIR!\backup\*.tar" (
        MOVE /Y "!BASE_DIR!\backup\*.tar" "!DEST_DIR!\backup\" >nul
    )
)

ECHO.
ECHO =============================================================
ECHO      ORGANIZACION COMPLETADA
ECHO =============================================================