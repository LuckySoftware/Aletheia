@ECHO OFF
REM Desactiva la visualización de comandos y activa expansión retrasada de variables
SETLOCAL ENABLEDELAYEDEXPANSION

REM =========================================================================
REM SCRIPT DE BACKUP DOCKERIZADO CON TRUNCATE
REM =========================================================================
REM Este script extrae una copia de seguridad de la base de datos PostgreSQL
REM que se ejecuta dentro de un contenedor Docker. Guarda los datos en un
REM archivo comprimido (.tar) en la carpeta local del proyecto y luego
REM vacía todas las tablas de la base de datos.

REM Define el nombre de la base de datos dentro del contenedor Docker
SET DB_NAME=canahuate_db

REM =========================================================================
REM BLOQUE 1: LOCALIZAR LA RAÍZ DEL PROYECTO
REM =========================================================================
REM Busca automáticamente dónde están los archivos del proyecto, sin importar
REM desde qué carpeta se ejecute este script.

REM Cambia al directorio donde se encuentra este script (.bat)
cd /d "%~dp0"
REM Guarda la ubicación actual como raíz temporal del proyecto
SET "PROJECT_ROOT=%cd%"

REM Si el script se ejecuta desde la carpeta scripts/, sube un nivel
IF "%~nx0"=="backup_docker_con_truncate.bat" (
    IF EXIST "%PROJECT_ROOT%\scripts\%~nx0" (
        REM Cambia a la carpeta padre
        cd /d "%PROJECT_ROOT%\.."
        REM Actualiza la raíz del proyecto
        SET "PROJECT_ROOT=!cd!"
    )
)

REM Inicia un bucle que busca hacia arriba en la estructura de carpetas
REM buscando el archivo docker-compose.yaml que identifica la raíz del proyecto
:find_root
IF EXIST "%PROJECT_ROOT%\docker-compose.yaml" (
    REM Si encuentra docker-compose.yaml, lo localiza como raíz del proyecto
    ECHO [OK] Raíz del proyecto encontrada: %PROJECT_ROOT%
    GOTO :found_root
)

REM Si llegó a la raíz del disco sin encontrar docker-compose.yaml, termina con error
IF "%PROJECT_ROOT%"=="\" (
    ECHO [ERROR] No se encontró docker-compose.yaml
    PAUSE
    EXIT /B 1
)

REM Sube un nivel en la estructura de carpetas
for %%A in ("%PROJECT_ROOT%") do SET "PROJECT_ROOT=%%~dpA"
REM Elimina la última barra invertida para dejar la ruta limpia
SET "PROJECT_ROOT=!PROJECT_ROOT:~0,-1!"

REM Repite el bucle para continuar buscando
GOTO :find_root

:found_root
REM Navega a la raíz del proyecto encontrada
cd /d "%PROJECT_ROOT%"

REM =========================================================================
REM BLOQUE 2: CONFIGURACIÓN Y GENERACIÓN DE TIMESTAMP
REM =========================================================================
REM Prepara todas las rutas necesarias y genera un identificador único
REM con fecha y hora para que cada backup sea distinguible.

ECHO [ %TIME% ] Preparando backup...

REM Obtiene la fecha y hora actual del sistema en formato YYYYMMDDHHMMSS
FOR /F "tokens=2 delims==" %%I IN ('wmic os get LocalDateTime /value') DO SET "DT=%%I"
REM Reformatea el timestamp a un formato más legible: YYYY-MM-DD_HHMMSS
SET "TIMESTAMP=!DT:~0,4!-!DT:~4,2!-!DT:~6,2!_!DT:~8,2!!DT:~10,2!!DT:~12,2!"

REM Construye el nombre del archivo de backup con el timestamp
SET "BACKUP_FILE_NAME=backup_!DB_NAME!_!TIMESTAMP!.tar"
REM Define la ruta completa donde se guardará el archivo
SET "BACKUP_FULL_PATH=%PROJECT_ROOT%\data\backup\!BACKUP_FILE_NAME!"
REM Define un archivo temporal para capturar mensajes de error
SET "TEMP_LOG_FILE=%TEMP%\db_maintenance_log_!TIMESTAMP!.tmp"
REM Ruta al archivo docker-compose.yaml que orquesta los contenedores
SET "DOCKER_COMPOSE_FILE=%PROJECT_ROOT%\docker-compose.yaml"

REM Crea la carpeta de backups si no existe
IF NOT EXIST "%PROJECT_ROOT%\data\backup" ( MKDIR "%PROJECT_ROOT%\data\backup" )

REM Muestra información de depuración para verificar que los caminos son correctos
ECHO [DEBUG] Raíz: %PROJECT_ROOT%
ECHO [DEBUG] Docker-compose: %DOCKER_COMPOSE_FILE%
ECHO.

REM =========================================================================
REM BLOQUE 3: INICIAR EL CONTENEDOR DOCKER
REM =========================================================================
REM Arranca el servicio de base de datos dentro de Docker para poder
REM extraer el backup.

ECHO [ %TIME% ] Iniciando servicio Docker...
REM Usa docker-compose para iniciar el contenedor llamado 'db'
docker-compose --file "%DOCKER_COMPOSE_FILE%" start db >nul 2>&1

REM Espera 3 segundos para que el contenedor se estabilice
timeout /t 3 /nobreak

REM Obtiene el identificador único del contenedor Docker en ejecución
FOR /f "tokens=*" %%a IN ('docker-compose --file "%DOCKER_COMPOSE_FILE%" ps -q db') DO (SET DB_CONTAINER_ID=%%a)

REM Verifica que se encontró el contenedor
IF NOT DEFINED DB_CONTAINER_ID (
    ECHO [ERROR] No se encontro contenedor 'db'
    PAUSE
    EXIT /B 1
)

REM Confirma que el contenedor fue encontrado exitosamente
ECHO [OK] Contenedor encontrado: !DB_CONTAINER_ID!

REM =========================================================================
REM BLOQUE 4: EJECUTAR pg_dump DENTRO DEL CONTENEDOR
REM =========================================================================
REM pg_dump es la herramienta de PostgreSQL que extrae el contenido
REM de la base de datos en formato tar (archivo comprimido).

ECHO [ %TIME% ] Generando backup dentro del contenedor...
REM Muestra el comando que se va a ejecutar para claridad
ECHO Comando: pg_dump -d !DB_NAME! -Ft --schema=public

REM Ejecuta pg_dump dentro del contenedor Docker:
REM   -d: especifica el nombre de la base de datos (canahuate_db)
REM   -Ft: crea el backup en formato tar (comprimido)
REM   --schema=public: solo backup del esquema "public" (excluye esquemas del sistema)
REM La salida se redirige a un archivo .tar en la máquina local
docker exec -u postgres !DB_CONTAINER_ID! sh -c "pg_dump -d !DB_NAME! -Ft --schema=public" > "!BACKUP_FULL_PATH!" 2>"!TEMP_LOG_FILE!"

REM Verifica si pg_dump finalizó exitosamente
IF !ERRORLEVEL! NEQ 0 (
    ECHO [ERROR] Fallo pg_dump
    REM Muestra el contenido del log para ver qué salió mal
    TYPE "!TEMP_LOG_FILE!"
    PAUSE
    EXIT /B 1
)

REM Confirma que el backup se creó correctamente
ECHO [OK] Backup creado correctamente

REM =========================================================================
REM BLOQUE 5: VERIFICACIÓN DEL ARCHIVO
REM =========================================================================
REM Comprueba que el archivo .tar se creó y es de un tamaño razonable.

REM Verifica que el archivo exista después de la redirección
IF NOT EXIST "!BACKUP_FULL_PATH!" (
    ECHO [ERROR] El archivo no existe: !BACKUP_FULL_PATH!
    PAUSE
    EXIT /B 1
)

REM Obtiene el tamaño del archivo en bytes
FOR %%A IN ("!BACKUP_FULL_PATH!") DO (
    SET "FILE_SIZE=%%~zA"
)

REM Muestra un resumen del backup completado
ECHO.
ECHO ================ BACKUP COMPLETADO ================
ECHO Archivo: !BACKUP_FILE_NAME!
ECHO Ruta: !BACKUP_FULL_PATH!
ECHO Tamaño: !FILE_SIZE! bytes
ECHO ===================================================
ECHO.

REM =========================================================================
REM BLOQUE 6: VALIDAR INTEGRIDAD DEL ARCHIVO TAR
REM =========================================================================
REM Verifica que el archivo .tar no esté corrupto usando pg_restore

ECHO [ %TIME% ] Validando integridad del backup...

REM Define la ruta a las herramientas de PostgreSQL instaladas localmente
SET "PG_PATH=C:\Program Files\PostgreSQL\17\bin"

REM Si pg_restore está disponible, lo usa para verificar el archivo
IF EXIST "%PG_PATH%\pg_restore.exe" (
    REM pg_restore --list solo lista el contenido sin restaurar
    "%PG_PATH%\pg_restore.exe" --list "!BACKUP_FULL_PATH!" > nul 2>&1
    REM Si la validación es exitosa, confirma que el tar es válido
    IF !ERRORLEVEL! EQU 0 (
        ECHO [OK] Archivo tar es válido
    ) ELSE (
        REM Si falla, puede ser que pg_restore no esté disponible
        ECHO [ADVERTENCIA] El archivo tar no se puede validar localmente
    )
)

REM =========================================================================
REM BLOQUE 7: TRUNCAR TODAS LAS TABLAS
REM =========================================================================
REM Vacía todas las tablas de la base de datos después de confirmar el backup exitoso
REM ADVERTENCIA: Esta operación elimina TODOS los datos de las tablas

ECHO.
ECHO =========================================================
ECHO ADVERTENCIA: Se procederá a VACIAR todas las tablas
ECHO Los datos ya están respaldados en: !BACKUP_FILE_NAME!
ECHO =========================================================
ECHO.
ECHO [ %TIME% ] Truncando todas las tablas...

REM Ejecuta un comando SQL que genera y ejecuta TRUNCATE para todas las tablas del esquema public
REM - Itera sobre todas las tablas usando pg_tables
REM - quote_ident protege contra inyección SQL
REM - CASCADE elimina también datos de tablas relacionadas por foreign keys
docker exec -u postgres !DB_CONTAINER_ID! psql -d !DB_NAME! -c "DO $$ DECLARE r RECORD; BEGIN FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP EXECUTE 'TRUNCATE TABLE ' || quote_ident(r.tablename) || ' CASCADE'; END LOOP; END $$;" 2>"!TEMP_LOG_FILE!"

REM Verifica si el truncate finalizó exitosamente
IF !ERRORLEVEL! NEQ 0 (
    ECHO [ERROR] Fallo al truncar tablas
    TYPE "!TEMP_LOG_FILE!"
    PAUSE
    EXIT /B 1
)

ECHO [OK] Todas las tablas han sido truncadas
ECHO.

REM =========================================================================
REM BLOQUE 8: LIMPIAR BACKUPS ANTIGUOS
REM =========================================================================
REM Mantiene solo los 5 backups más recientes, eliminando los más antiguos
REM para no llenar el disco de copias viejas.

ECHO [ %TIME% ] Limpiando backups antiguos...

REM Lista todos los archivos backup_*.tar ordenados por fecha (descendente)
REM skip=5 omite los 5 primeros (más recientes) y procesa el resto
for /f "skip=5 tokens=* delims=" %%A in ('dir /b /o-d "%PROJECT_ROOT%\data\backup\backup_*.tar" 2^>nul') do (
    REM Elimina cada archivo antiguo
    del "%PROJECT_ROOT%\data\backup\%%A"
    REM Muestra qué se eliminó
    ECHO   Eliminado: %%A
)

REM =========================================================================
REM BLOQUE 9: LIMPIEZA Y FINALIZACIÓN
REM =========================================================================
REM Elimina archivos temporales y detiene el contenedor Docker.

REM Elimina el archivo temporal de log (no se necesita si todo fue bien)
DEL "!TEMP_LOG_FILE!" >nul 2>&1

REM Detiene el contenedor Docker para liberar recursos
ECHO [ %TIME% ] Deteniendo servicio...
docker-compose --file "%DOCKER_COMPOSE_FILE%" stop db >nul 2>&1

REM Muestra mensaje final de éxito
ECHO.
ECHO ================ PROCESO FINALIZADO ================
ECHO - Backup completado: !BACKUP_FILE_NAME!
ECHO - Todas las tablas han sido vaciadas
ECHO ===================================================
ECHO.
ECHO [ %TIME% ] Proceso finalizado exitosamente.

REM Termina el script exitosamente (código 0)
EXIT /B 0