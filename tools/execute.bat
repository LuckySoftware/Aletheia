@ECHO OFF
SETLOCAL ENABLEDELAYEDEXPANSION

:: Cambiar al directorio donde se encuentra este script
CD /D "%~dp0"
ECHO [INFO] Directorio de trabajo: %CD%
ECHO.
ECHO ====================================================
ECHO == INICIANDO ALETHEIA PIPELINE V2 (Global)        ==
ECHO ====================================================
ECHO.

:: Se asume que 'python' esta en el PATH del sistema
SET "PYTHON_EXE=python"

%PYTHON_EXE% src\main.py

IF !ERRORLEVEL! NEQ 0 (
    ECHO.
    ECHO [ERROR CRITICO] El pipeline global fallo. Revise los logs arriba.
    PAUSE
    EXIT /B 1
)

ECHO.
ECHO [OK] Ejecucion de todas las plantas completada exitosamente.
PAUSE