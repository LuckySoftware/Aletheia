@ECHO OFF
SETLOCAL ENABLEDELAYEDEXPANSION

:: Ir a la raíz del proyecto
CD /D "%~dp0.."

ECHO =============================================================
ECHO      BACKUP Y LIMPIEZA GLOBAL DE BASES DE DATOS (.BAT)
ECHO =============================================================

:: Configuración de Postgres
SET "PG_PATH=C:\Program Files\PostgreSQL\17\bin"

:: Generar fecha segura sin depender del idioma/región del OS
FOR /F "usebackq tokens=*" %%D IN (`powershell -NoProfile -Command "Get-Date -Format 'yyyyMMdd'"`) DO SET DATE_STR=%%D

:: Ejecutamos la lógica para cada planta definida en el config.json
FOR /F "tokens=*" %%P IN ('powershell -NoProfile -Command "$c = Get-Content 'config.json' | ConvertFrom-Json; $c.plantas | ForEach-Object { $_.id + '|' + $_.db_name + '|' + $_.db_user + '|' + $_.db_password + '|' + $_.db_host + '|' + $_.db_port }"') DO (
    FOR /F "tokens=1-6 delims=|" %%a IN ("%%P") DO (
        SET "P_ID=%%a"
        SET "DB_NAME=%%b"
        SET "DB_USER=%%c"
        SET "DB_PASS=%%d"
        SET "DB_HOST=%%e"
        SET "DB_PORT=%%f"
        
        ECHO.
        :: Escapamos los paréntesis con ^ para que CMD no se confunda
        ECHO --- Procesando Planta: !P_ID! ^(BD: !DB_NAME!^) ---
        
        SET "BACKUP_DIR=data\!P_ID!\backup"
        IF NOT EXIST "!BACKUP_DIR!" MKDIR "!BACKUP_DIR!"
        SET "BKP_FILE=!BACKUP_DIR!\backup_!DB_NAME!_%DATE_STR%.tar"

        :: 1. Exportar Backup
        SET "PGPASSWORD=!DB_PASS!"
        ECHO    [1/3] Creando Backup...
        "!PG_PATH!\pg_dump.exe" -h !DB_HOST! -p !DB_PORT! -U !DB_USER! -F t -f "!BKP_FILE!" !DB_NAME!
        
        :: 2. Truncate Tablas (Limpieza)
        ECHO    [2/3] Truncando tablas de trabajo...
        SET "SQL=DO $body$ DECLARE r RECORD; BEGIN FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename NOT IN ('load_control', 'validation_rules', 'pipeline_status_history')) LOOP EXECUTE 'TRUNCATE TABLE ' || quote_ident(r.tablename) || ' CASCADE;'; END LOOP; END $body$;"
        "!PG_PATH!\psql.exe" -h !DB_HOST! -p !DB_PORT! -d !DB_NAME! -U !DB_USER! -c "!SQL!"
        
        :: 3. Rotación de Backups (Mantiene los últimos 5)
        ECHO    [3/3] Limpiando backups antiguos...
        powershell -NoProfile -Command "$files = Get-ChildItem '!BACKUP_DIR!\backup_*.tar' | Sort-Object CreationTime -Descending; if ($files.Count -gt 5) { $files | Select-Object -Skip 5 | Remove-Item -Force }"
    )
)

SET "PGPASSWORD="
ECHO.
ECHO =============================================================
ECHO      PROCESO DE BACKUP FINALIZADO
ECHO =============================================================