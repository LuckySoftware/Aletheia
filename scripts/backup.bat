@ECHO OFF
SETLOCAL ENABLEDELAYEDEXPANSION

REM ==============================================
REM SCRIPT DE ARCHIVADO Y LIMPIEZA PARA POSTGRESQL
REM ==============================================

REM --- CARGA DE CREDENCIALES DESDE ARCHIVO .ENV ---
IF NOT EXIST "%~dp0..\aletheia_container\.env" (
    ECHO [!] ERROR: No se encontro el archivo .env en la carpeta aletheia_container.
    PAUSE
    GOTO :EOF
)
FOR /F "usebackq eol=# tokens=1,* delims==" %%A IN ("%~dp0..\aletheia_container\.env") DO (
    SET "%%A=%%B"
    IF /I "%%A"=="DB_PASSWORD" SET "PGPASSWORD=%%B"
)

REM --- CONFIGURACION ADICIONAL ---
SET BACKUP_DIR=C:\Users\lmoreira\Desktop\__desarrollo__\__en__desarrollo__\Aletheia\backups
SET TABLES_TO_CLEAN=duplicated_data error_data excluded_data raw_data validated_data
SET PG_BIN_PATH=C:\Program Files\PostgreSQL\17\bin

REM --- GENERACION DE NOMBRES DE ARCHIVO ---
FOR /F "tokens=2 delims==" %%I IN ('wmic os get LocalDateTime /value') DO SET "DT=%%I"
SET "TIMESTAMP=!DT:~0,4!-!DT:~4,2!-!DT:~6,2!_!DT:~8,2!!DT:~10,2!!DT:~12,2!"
SET "BACKUP_FILE=!BACKUP_DIR!\backup_db_!DB_NAME!_!TIMESTAMP!.dump"
SET "TEMP_LOG_FILE=%TEMP%\backup_log_!TIMESTAMP!.tmp"

REM --- INICIO DEL PROCESO ---

REM Paso 1: Crear directorio de backups si no existe
IF NOT EXIST "!BACKUP_DIR!" ( MKDIR "!BACKUP_DIR!" )

REM Paso 2: Ejecutar el backup, redirigiendo solo los errores al log temporal
"!PG_BIN_PATH!\pg_dump.exe" -U !DB_USER! -d !DB_NAME! -Fc > "!BACKUP_FILE!" 2>"!TEMP_LOG_FILE!"
IF !ERRORLEVEL! NEQ 0 (
    ECHO [!] ERROR CRITICO: El backup de la base de datos fallo.
    GOTO :HANDLE_ERROR
)

REM Paso 3: Construir y ejecutar el comando de limpieza
SET "TRUNCATE_COMMAND=TRUNCATE TABLE "
FOR %%T IN (!TABLES_TO_CLEAN!) DO (
    SET "TRUNCATE_COMMAND=!TRUNCATE_COMMAND!public.%%T, "
)
SET "TRUNCATE_COMMAND=!TRUNCATE_COMMAND:~0,-2! CASCADE;"

"!PG_BIN_PATH!\psql.exe" -U !DB_USER! -d !DB_NAME! -c "!TRUNCATE_COMMAND!" >>"!TEMP_LOG_FILE!" 2>&1
IF !ERRORLEVEL! NEQ 0 (
    ECHO [!] ERROR CRITICO: El vaciado de tablas fallo. El backup se creo pero las tablas no se limpiaron.
    GOTO :HANDLE_ERROR
)

REM --- EXITO: Limpiar y salir silenciosamente ---
DEL "!TEMP_LOG_FILE!"
GOTO :CLEANUP

:HANDLE_ERROR
REM --- FALLO: Mostrar el log de error y guardarlo permanentemente ---
ECHO.
ECHO    Se ha producido un error. Detalles del log:
ECHO    -------------------------------------------------
TYPE "!TEMP_LOG_FILE!"
ECHO    -------------------------------------------------
ECHO.
REN "!TEMP_LOG_FILE!" "error_log_!TIMESTAMP!.txt"
MOVE "error_log_!TIMESTAMP!.txt" "!BACKUP_DIR!" > NUL
ECHO Un archivo de log detallado se ha guardado en: !BACKUP_DIR!\error_log_!TIMESTAMP!.txt
PAUSE

:CLEANUP
REM Limpieza de variables de entorno
SET "PGPASSWORD="
ENDLOCAL