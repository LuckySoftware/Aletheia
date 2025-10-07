@ECHO OFF
TITLE Herramienta de Restauracion para PostgreSQL
COLOR 0A

REM =================================================================
REM Herramienta para restaurar backups de PostgreSQL de forma interactiva.
REM =================================================================

REM --- CONFIGURACION (Ajusta solo si tu instalacion es diferente) ---
SET PG_BIN_PATH=C:\Program Files\PostgreSQL\17\bin
SET DB_USER=postgres
REM --- FIN DE LA CONFIGURACION ---

:START
CLS
ECHO.
ECHO    =======================================================
ECHO      Herramienta de Restauracion de Backups PostgreSQL
ECHO    =======================================================
ECHO.

ECHO.
ECHO Introduce los datos para la restauracion:
ECHO.

REM -- Pedir el nombre de la base de datos de destino --
SET "TARGET_DB="
SET /P TARGET_DB="1. Nombre de la base de datos de destino (ej: dt_5_restaurada): "

IF "%TARGET_DB%"=="" (
    ECHO.
    ECHO    [!] El nombre de la base de datos no puede estar vacio.
    PAUSE
    GOTO :START
)

REM -- Pedir la ruta del archivo de backup --
ECHO.
SET "BACKUP_FILE="
ECHO 2. Arrastra el archivo .dump a esta ventana y presiona Enter,
ECHO    o pega la ruta completa:
SET /P BACKUP_FILE=

IF "%BACKUP_FILE%"=="" (
    ECHO.
    ECHO    [!] La ruta del archivo no puede estar vacia.
    PAUSE
    GOTO :START
)

REM -- Limpiar las comillas si el usuario arrastro el archivo --
SET BACKUP_FILE=%BACKUP_FILE:"=%

ECHO.
ECHO ----------------------------------------------------
ECHO.
ECHO   Se restaurara el siguiente backup:
ECHO     -> %BACKUP_FILE%
ECHO.
ECHO   En la base de datos de destino:
ECHO     -> %TARGET_DB%
ECHO.
ECHO ----------------------------------------------------
ECHO.
PAUSE

ECHO.
ECHO Iniciando restauracion... (Se te pedira la contrasena para el usuario '%DB_USER%')
ECHO.

REM -- Ejecutar el comando de restauracion --
"%PG_BIN_PATH%\pg_restore.exe" --clean -U %DB_USER% -d %TARGET_DB% "%BACKUP_FILE%"

REM -- Verificar el resultado --
IF %ERRORLEVEL% NEQ 0 (
    ECHO.
    ECHO    =======================================================
    ECHO      [!] ERROR: La restauracion ha fallado.
    ECHO          Revisa los mensajes de error de arriba.
    ECHO    =======================================================
) ELSE (
    ECHO.
    ECHO    =======================================================
    ECHO      [!] EXITO: La base de datos ha sido restaurada.
    ECHO    =======================================================
)

ECHO.
PAUSE
GOTO :EOF