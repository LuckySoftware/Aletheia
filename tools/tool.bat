@echo off
REM Este script se ejecuta desde la carpeta 'tool' y llama a los scripts de Python en la carpeta 'src'.

REM Guarda el directorio actual y cambia al directorio donde estan los scripts de Python.
pushd ..\src

echo Iniciando la ejecucion de los scripts en la carpeta '%CD%'
echo.

echo =========================================
echo  1. Ejecutando: 2_load_exclusiones.py
echo =========================================
python 2_load_exclusiones.py
echo.

echo =========================================
echo  2. Ejecutando: 4_load_csv.py
echo =========================================
python 4_load_csv.py
echo.

echo =========================================
echo  3. Ejecutando: 5_handle_duplicates.py
echo =========================================
python 5_handle_duplicates.py
echo.

echo =========================================
echo  4. Ejecutando: 6_validate_data.py
echo =========================================
python 6_validate_data.py
echo.

echo =========================================
echo  5. Ejecutando: 7_delete_exclusions.py
echo =========================================
python 7_delete_exclusions.py
echo.

echo =========================================
echo  6. Ejecutando: 8_export_to_excel.py
echo =========================================
python 8_export_to_excel.py
echo.

echo =========================================
echo.
echo  Todos los scripts se han ejecutado.
echo =========================================
echo.

REM Regresa al directorio original desde donde se ejecuto el batch.
popd