#!/bin/sh

# =================================================================================================
# SCRIPT DE ENTRADA (ENTRYPOINT) - ORQUESTADOR DE INICIO DEL CONTENEDOR
# -------------------------------------------------------------------------------------------------
#
# Descripción:
#   Este script actúa como el "director de orquesta" para el arranque del contenedor.
#   Su responsabilidad principal es asegurar que todas las dependencias externas (como la
#   base de datos) estén listas y que el estado inicial de la aplicación (esquema de BD)
#   esté correctamente configurado ANTES de que el contenedor se considere operativo
#   o ejecute cualquier tarea principal.
# =================================================================================================

# --- Configuración de Seguridad del Script ---
# 'set -e' instruye al script para que termine inmediatamente si cualquier comando falla
# (es decir, devuelve un código de salida distinto de cero). Esto previene comportamientos
# inesperados y asegura que el script no continúe en un estado de error. Es una
# práctica fundamental para la creación de scripts robustos.
set -e

# --- PASO 1: ESPERA ACTIVA DE LA BASE DE DATOS ---
# El contenedor de la aplicación a menudo arranca más rápido que el de la base de datos.
# Este bloque soluciona esa condición de carrera, pausando la ejecución hasta que la BD
# esté completamente lista para aceptar conexiones.

# Imprime un mensaje informativo en la consola para indicar el inicio del proceso de verificación.
echo "Verificando la disponibilidad de la base de datos en $DB_HOST:$DB_PORT..."

# Inicia un bucle 'until'. Este bucle se ejecutará repetidamente HASTA QUE el comando
# 'pg_isready' devuelva un código de éxito (0).
# Las variables ($DB_HOST, $DB_PORT, $DB_USER) son inyectadas desde el entorno (docker-compose).
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER"; do
  # Si 'pg_isready' falla, imprime un mensaje de espera en el flujo de error estándar (stderr).
  # Redirigir a '>&2' es una buena práctica para logs de estado, separándolos de la salida estándar (stdout).
  >&2 echo "Postgres no está disponible todavía - esperando..."
  # Pausa la ejecución durante 1 segundo para evitar saturar la red con comprobaciones constantes.
  sleep 1
done

# Una vez que el bucle termina, significa que la base de datos ha respondido exitosamente.
>&2 echo "¡PostgreSQL está activo y listo para aceptar conexiones!"

# --- PASO 2: INICIALIZACIÓN DEL ESQUEMA DE LA BASE DE DATOS ---
# Con la garantía de que la base de datos está disponible, procedemos a ejecutar
# los scripts de inicialización que definen la estructura (tablas, vistas, etc.).

echo "Ejecutando script de creación de esquema de la base de datos..."

# Ejecuta el script de Python encargado de crear la estructura inicial de la base de datos.
# Se utiliza la ruta absoluta dentro del contenedor, establecida por el Dockerfile.
python /app/src/core/1_create_database.py

# --- PASO 3: LÓGICA DE EJECUCIÓN FINAL Y PERSISTENCIA DEL CONTENEDOR ---
# Después de completar todas las tareas de inicialización, este bloque decide si el
# contenedor debe terminar su ejecución o permanecer activo.

echo "Inicialización completada."

# Se verifica el valor de la variable de entorno 'KEEP_ALIVE'.
# La sintaxis '${KEEP_ALIVE:-false}' es una expansión de parámetros que provee un valor por
# defecto ('false') si la variable no está definida. Esto hace el comportamiento predecible.
if [ "${KEEP_ALIVE:-false}" = "true" ]; then
  # Si la variable es explícitamente 'true', el contenedor debe permanecer en ejecución.
  echo "La variable KEEP_ALIVE está en 'true'. El contenedor permanecerá activo."
  # 'tail -f /dev/null' es un comando que nunca termina, manteniendo el proceso principal
  # del contenedor vivo y, por lo tanto, el contenedor en estado 'running'.
  tail -f /dev/null
else
  # Si la variable no es 'true', se asume que el contenedor ha cumplido su propósito (por ejemplo,
  # un trabajo por lotes o una migración) y debe detenerse.
  echo "La variable KEEP_ALIVE no es 'true'. El contenedor ha finalizado sus tareas."
  # 'exit 0' finaliza el script (y por ende el contenedor) con un código de éxito.
  exit 0
fi