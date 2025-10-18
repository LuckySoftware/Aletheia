@echo off
REM Desactiva la visualización de comandos en la consola y activa expansión de variables retrasada
setlocal EnableDelayedExpansion
REM Configura la página de códigos a UTF-8 para soportar caracteres especiales
chcp 65001 >nul
REM Establece el título de la ventana de consola
title Restaurar Backup PostgreSQL

echo.
echo ==============================================
echo        RESTAURAR BACKUP LOCAL POSTGRESQL
echo ==============================================
echo.

REM =========================================================================
REM BLOQUE 1: LOCALIZAR LA RAÍZ DEL PROYECTO
REM =========================================================================
REM El script necesita encontrar el archivo docker-compose.yaml para ubicar
REM correctamente los backups, sin importar desde dónde se ejecute.

REM Cambia al directorio donde se encuentra este script
cd /d "%~dp0"
SET "PROJECT_ROOT=%cd%"

REM Inicia un bucle que busca hacia arriba en la estructura de carpetas
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
REM BLOQUE 2: CONFIGURACIÓN DE CREDENCIALES Y RUTAS
REM =========================================================================
REM Define todos los parámetros necesarios para conectarse a PostgreSQL
REM y acceder a los archivos de backup.

set "PGUSER=postgres"
set "PGHOST=localhost"
set "PGPORT=5432"
set "PGPASSWORD=postgres"
set "PG_DEFAULT_DB=postgres"
REM La carpeta de backup se asume que está en \data\backup relativo a la raíz
set "BACKUP_DIR=%PROJECT_ROOT%\data\backup"
REM Ruta a las herramientas de PostgreSQL instaladas localmente
set "PG_PATH=C:\Program Files\PostgreSQL\17\bin"

REM =========================================================================
REM BLOQUE 3: VALIDACIÓN DE DIRECTORIO DE BACKUP
REM =========================================================================
REM Verifica que la carpeta de backups exista antes de continuar

if not exist "%BACKUP_DIR%" (
    echo [ERROR] Directorio de backup no existe: "%BACKUP_DIR%"
    pause
    exit /b
)

REM =========================================================================
REM BLOQUE 4: PASO 1 - LISTAR BACKUPS DISPONIBLES
REM =========================================================================
REM Muestra al usuario qué archivos .tar están disponibles para restaurar

echo [PASO 1] Listando backups disponibles...
echo.
REM Usa dir para listar solo archivos .tar en el directorio de backup
dir /b "%BACKUP_DIR%\*.tar" 2>nul

REM Si no hay archivos .tar, termina el script
if errorlevel 1 (
    echo [ERROR] No hay backups disponibles
    pause
    exit /b
)

echo.
REM Solicita al usuario que ingrese el nombre exacto del archivo
set /p backup_name=Ingrese el nombre del archivo .tar:

REM Construye la ruta completa del archivo seleccionado
set "backup_path=%BACKUP_DIR%\%backup_name%"

REM Verifica que el archivo seleccionado exista
if not exist "%backup_path%" (
    echo [ERROR] Archivo no encontrado: "%backup_path%"
    pause
    exit /b
)

REM =========================================================================
REM BLOQUE 5: PASO 2 - VALIDAR INTEGRIDAD DEL ARCHIVO TAR
REM =========================================================================
REM Comprueba que el archivo .tar no esté corrupto antes de intentar restaurar

echo [PASO 2] Validando archivo tar...
REM pg_restore --list verifica la estructura del archivo sin restaurar
"%PG_PATH%\pg_restore.exe" --list "%backup_path%" > nul 2>&1

if errorlevel 1 (
    echo [ERROR] El archivo no es un tar válido
    pause
    exit /b
)

echo [OK] Archivo válido

REM =========================================================================
REM BLOQUE 6: PASO 3 Y 4 - CREAR NUEVA BASE DE DATOS
REM =========================================================================
REM Solicita nombre para la nueva BD y la crea, eliminando previamente
REM cualquier BD existente con el mismo nombre.

echo.
set /p dbname=Ingrese nombre de la BD (será creada):

REM Valida que el usuario ingresó un nombre no vacío
if "%dbname%"=="" (
    echo [ERROR] Nombre vacío
    pause
    exit /b
)

echo.
echo [PASO 3] Eliminando BD existente si la hay...
REM Si existe una BD con este nombre, la elimina para empezar limpio
"%PG_PATH%\psql.exe" -U %PGUSER% -h %PGHOST% -p %PGPORT% -d %PG_DEFAULT_DB% -c "DROP DATABASE IF EXISTS \"%dbname%\";" >nul 2>&1

echo [PASO 4] Creando BD "%dbname%"...
REM Crea una base de datos nueva vacía
"%PG_PATH%\psql.exe" -U %PGUSER% -h %PGHOST% -p %PGPORT% -d %PG_DEFAULT_DB% -c "CREATE DATABASE \"%dbname%\";" >nul 2>&1

REM Si la creación falla, termina el script
if errorlevel 1 (
    echo [ERROR] No se pudo crear la BD
    pause
    exit /b
)

echo [OK] BD creada

REM =========================================================================
REM BLOQUE 7: PASO 5 - CREAR TIPOS PERSONALIZADOS
REM =========================================================================
REM PostgreSQL usa tipos ENUM personalizados. Se crean antes de restaurar
REM para que la importación de datos sea compatible con la estructura.

echo [PASO 5] Creando tipos personalizados...
REM Crea el tipo data_status con los valores posibles que usan las tablas
"%PG_PATH%\psql.exe" -U %PGUSER% -h %PGHOST% -p %PGPORT% -d "%dbname%" -c "CREATE TYPE public.data_status AS ENUM ('pending', 'processing', 'success', 'error');" >nul 2>&1
REM Crea el tipo rule_type_enum usado en validaciones
"%PG_PATH%\psql.exe" -U %PGUSER% -h %PGHOST% -p %PGPORT% -d "%dbname%" -c "CREATE TYPE public.rule_type_enum AS ENUM ('not_null', 'range', 'NOT_POSITIVE_IN_RANGE');" >nul 2>&1

REM =========================================================================
REM BLOQUE 8: PASO 6 - RESTAURAR EL BACKUP
REM =========================================================================
REM Extrae todas las tablas, secuencias, índices y restricciones del archivo
REM .tar y los recrea en la nueva base de datos. El parámetro -v muestra
REM el progreso en tiempo real.

echo [PASO 6] RESTAURANDO BACKUP - Esto puede tomar minutos...
echo.

REM Ejecuta pg_restore con modo verbose para ver qué se está restaurando
"%PG_PATH%\pg_restore.exe" -U %PGUSER% -h %PGHOST% -p %PGPORT% -d "%dbname%" -v "%backup_path%"

REM Verifica si pg_restore terminó sin errores
if errorlevel 1 (
    echo.
    echo [ADVERTENCIA] pg_restore retornó código de error
    echo Verificando si se restauró parcialmente...
    echo.
) else (
    echo.
    echo [OK] Restauración completada
)

REM =========================================================================
REM BLOQUE 9: PASO 7 - VERIFICAR RESULTADO FINAL
REM =========================================================================
REM Consulta la base de datos para mostrar qué tablas se restauraron
REM exitosamente, confiriendo al usuario que la operación funcionó.

echo.
echo [PASO 7] Verificando tablas restauradas...
echo.

REM Query SQL que lista todas las tablas públicas en orden alfabético
"%PG_PATH%\psql.exe" -U %PGUSER% -h %PGHOST% -p %PGPORT% -d "%dbname%" -c "SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;"

REM =========================================================================
REM BLOQUE 10: RESUMEN Y FINALIZACIÓN
REM =========================================================================
REM Muestra un resumen final con la información del backup restaurado

echo.
echo ============================================
echo Base de datos: %dbname%
echo Backup: %backup_name%
echo ============================================
echo.
REM Pausa para que el usuario pueda revisar la salida antes de cerrar
pause
exit /b