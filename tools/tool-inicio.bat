@echo off
title Menu de Ejecucion Manual
color 0B

:menu
cls
echo =======================================================
echo.
echo           MENU DE EJECUCION DE SCRIPTS
echo.
echo =======================================================
echo.
echo  [1] Ejecutar: 1_create_database.py
echo  [2] Ejecutar: 2_load_exclusiones.py
echo  [3] Ejecutar: 3_load_rules_from_json.py
echo  [4] Ejecutar: 4_load_csv.py
echo  [5] Ejecutar: 5_handle_duplicates.py
echo  [6] Ejecutar: 6_validate_data.py
echo  [7] Ejecutar: 7_delete_exclusions.py
echo  [8] Ejecutar: 8_export_to_excel.py
echo.
echo  [9] Salir
echo.
echo =======================================================
echo.

set /p opcion="Por favor, selecciona una opcion y presiona Enter: "

if "%opcion%"=="1" (
    echo.
    echo --- Ejecutando 1_create_database.py ---
    python ..\src\1_create_database.py
    goto end_script
)
if "%opcion%"=="2" (
    echo.
    echo --- Ejecutando 2_load_exclusiones.py ---
    python ..\src\2_load_exclusiones.py
    goto end_script
)
if "%opcion%"=="3" (
    echo.
    echo --- Ejecutando 3_load_rules_from_json.py ---
    python ..\src\3_load_rules_from_json.py
    goto end_script
)
if "%opcion%"=="4" (
    echo.
    echo --- Ejecutando 4_load_csv.py ---
    python ..\src\4_load_csv.py
    goto end_script
)
if "%opcion%"=="5" (
    echo.
    echo --- Ejecutando 5_handle_duplicates.py ---
    python ..\src\5_handle_duplicates.py
    goto end_script
)
if "%opcion%"=="6" (
    echo.
    echo --- Ejecutando 6_validate_data.py ---
    python ..\src\6_validate_data.py
    goto end_script
)
if "%opcion%"=="7" (
    echo.
    echo --- Ejecutando 7_delete_exclusions.py ---
    python ..\src\7_delete_exclusions.py
    goto end_script
)
if "%opcion%"=="8" (
    echo.
    echo --- Ejecutando 8_export_to_excel.py ---
    python ..\src\8_export_to_excel.py
    goto end_script
)
if "%opcion%"=="9" (
    exit
)

echo.
echo Opcion invalida. Intentalo de nuevo.
pause
goto menu

:end_script
echo.
echo --- Script finalizado. Presiona una tecla para volver al menu. ---
pause
goto menu