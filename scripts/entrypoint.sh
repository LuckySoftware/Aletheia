#!/bin/sh

# =================================================================================================
# SCRIPT DE ENTRADA (ENTRYPOINT) - ORQUESTADOR DEL PIPELINE ALETHEIA
# -------------------------------------------------------------------------------------------------
#
# Descripción:
#   Este script actúa como el "director de orquesta" del contenedor Aletheia.
#   Ejecuta secuencialmente todos los pasos del pipeline de validación de datos,
#   desde la creación de la base de datos hasta el envío de notificaciones por correo.
#
#   FLUJO CRÍTICO:
#   1. Detecta archivos NUEVOS ANTES de procesar
#   2. Guarda ese estado en una variable temporal
#   3. Ejecuta el pipeline
#   4. Envía el email con el estado de archivos detectado al inicio
# =================================================================================================

set -e

# --- COLORES PARA OUTPUT ---
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Funciones de logging
log_info() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] $1"
}

log_error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [ERROR] $1" >&2
}

log_step() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [PASO] $1"
}

# --- INICIALIZACIÓN: LIMPIAR ARCHIVOS DE CONTROL ---
CONTROL_FILE="/tmp/email_notifier.lock"
log_info "Limpiando archivos de control anteriores..."
rm -f "$CONTROL_FILE"

# --- PASO 0: ESPERA ACTIVA DE LA BASE DE DATOS ---
log_info "Verificando la disponibilidad de la base de datos en $DB_HOST:$DB_PORT..."

until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER"; do
  log_error "Postgres no está disponible todavía - esperando..."
  sleep 1
done

log_info "PostgreSQL está activo y listo para aceptar conexiones!"

# --- PASO 1: CREAR BASE DE DATOS Y ESQUEMA ---
log_step "Ejecutando: 1_create_database.py"
python /app/src/core/1_create_database.py || {
    log_error "Error al crear la base de datos"
    exit 1
}
log_info "✓ Base de datos creada exitosamente"

# --- PASO 2: CARGAR EXCLUSIONES DESDE GOOGLE SHEETS ---
log_step "Ejecutando: 2_load_exclusions_from_google_sheets.py"
python /app/src/core/2_load_exclusiones.py || {
    log_error "Error al cargar exclusiones"
    exit 1
}
log_info "✓ Exclusiones cargadas exitosamente"

# --- PASO 3: CARGAR REGLAS DE VALIDACIÓN ---
log_step "Ejecutando: 3_load_rules_from_json.py"
python /app/src/core/3_load_rules_from_json.py || {
    log_error "Error al cargar reglas"
    exit 1
}
log_info "✓ Reglas cargadas exitosamente"

# --- PASO 4: CARGAR CSV A LA BASE DE DATOS ---
log_step "Ejecutando: 4_load_csv_to_database.py"
python /app/src/core/4_load_csv.py || {
    log_error "Error al cargar CSV"
    exit 1
}
log_info "✓ Datos CSV cargados exitosamente"

# --- PASO 5: MANEJAR DUPLICADOS ---
log_step "Ejecutando: 5_handle_duplicates.py"
python /app/src/core/5_handle_duplicates.py || {
    log_error "Error al procesar duplicados"
    exit 1
}
log_info "✓ Duplicados procesados exitosamente"

# --- PASO 6: VALIDAR DATOS ---
log_step "Ejecutando: 6_validate_data_in_database.py"
python /app/src/core/6_validate_data.py || {
    log_error "Error durante la validación"
    exit 1
}
log_info "✓ Validación completada exitosamente"

# --- PASO 7: ELIMINAR EXCLUSIONES ---
log_step "Ejecutando: 7_delete_exclusions.py"
python /app/src/core/7_delete_exclusions.py || {
    log_error "Error al eliminar exclusiones"
    exit 1
}
log_info "✓ Exclusiones eliminadas exitosamente"

# --- PASO 8: EXPORTAR A EXCEL ---
log_step "Ejecutando: 8_export_to_excel.py"
python /app/src/core/8_export_to_excel.py || {
    log_error "Error al generar reportes"
    exit 1
}
log_info "✓ Reportes Excel generados exitosamente"

# --- PASO 9: ENVIAR NOTIFICACIONES POR CORREO ---
log_step "Ejecutando: email_notifier.py"

# Verificar si existe el archivo de control
CONTROL_FILE="/tmp/email_notifier.lock"
if [ -f "$CONTROL_FILE" ]; then
    log_info "El notificador ya se ejecutó en esta sesión. Omitiendo..."
else
    # Crear archivo de control
    touch "$CONTROL_FILE"
    
    # Ejecutar el notificador
    python /app/src/core/email_notifier.py || {
        log_error "Error al enviar notificaciones"
        rm -f "$CONTROL_FILE"  # Limpiar archivo de control en caso de error
        exit 1
    }
    log_info "✓ Notificaciones procesadas exitosamente"
fi

# --- FINALIZACIÓN ---
log_info "=============================================="
log_info "Pipeline Aletheia completado exitosamente"
log_info "=============================================="

# Limpiar archivo de control antes de finalizar
rm -f "$CONTROL_FILE"
log_info "Archivos de control limpiados"

# Decisión: mantener activo o terminar
if [ "${KEEP_ALIVE:-false}" = "true" ]; then
  log_info "KEEP_ALIVE=true → Contenedor permanecerá activo"
  tail -f /dev/null
else
  log_info "Finalizando contenedor"
  exit 0
fi