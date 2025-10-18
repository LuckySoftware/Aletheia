# =================================================================================================
# DOCKERFILE - IMAGEN DE PRODUCCIÓN PARA APLICACIÓN PYTHON
# -------------------------------------------------------------------------------------------------
#
# Descripción:
#   Este Dockerfile encapsula la aplicación en una imagen de contenedor optimizada.
#   Cada paso está diseñado para maximizar la eficiencia del caché de construcción,
#   minimizar el tamaño final de la imagen y fortalecer la seguridad al operar
#   como un usuario sin privilegios de root.
# =================================================================================================

# --- ETAPA 1: BASE Y DEPENDENCIAS ---

# Se establece la imagen de partida. 'python:3.9-slim' provee un entorno Python 3.9
# mínimo y optimizado, reduciendo la superficie de ataque y el tamaño de la imagen.
FROM python:3.9-slim

# Se define el directorio de trabajo dentro del contenedor. Todas las rutas relativas
# en comandos subsiguientes (COPY, RUN, etc.) se resolverán desde /app.
WORKDIR /app

#
# --- Optimización del Caché de Capas (Paso Crítico) ---
# Copiamos únicamente el listado de dependencias antes que cualquier otro archivo.
# Docker almacenará en caché esta capa. La costosa reinstalación de paquetes
# solo se ejecutará de nuevo si el contenido de 'requirements.txt' cambia.
COPY requirements.txt .

#
# --- Instalación y Limpieza en una Sola Capa ---
# Se encadenan todos los comandos de instalación y limpieza con '&&' para crear una
# única capa en la imagen, manteniendo su tamaño final lo más reducido posible.
RUN apt-get update && \
    # Se instalan dependencias del sistema. '--no-install-recommends' evita paquetes no esenciales.
    #   - gcc, libpq-dev: Compiladores necesarios para algunas librerías de Python (ej. psycopg2).
    #   - postgresql-client: Provee herramientas como 'pg_isready', útil para scripts de espera.
    apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        postgresql-client \
    # Se instalan las dependencias de Python. '--no-cache-dir' previene que pip almacene
    # el caché de descargas, ahorrando espacio en la imagen final.
    && pip install --no-cache-dir -r requirements.txt \
    # --- Fase de Limpieza Post-Instalación ---
    # Una vez que las librerías de Python están compiladas e instaladas, los compiladores
    # ya no son necesarios en la imagen final. Se eliminan para reducir vulnerabilidades y tamaño.
    && apt-get purge -y --auto-remove gcc libpq-dev \
    # Se elimina el caché del gestor de paquetes 'apt' para liberar más espacio.
    && rm -rf /var/lib/apt/lists/*

#
# --- ETAPA 2: CONFIGURACIÓN DE LA APLICACIÓN ---

# Con las dependencias ya instaladas y en caché, ahora copiamos el resto del código fuente
# de la aplicación al directorio de trabajo del contenedor.
COPY . .

#
# --- Fortalecimiento de la Seguridad (Principio de Menor Privilegio) ---
# Se crea un usuario específico para la aplicación, sin privilegios de root.
# Ejecutar el contenedor como no-root es una defensa fundamental contra ataques de escalada de privilegios.
RUN useradd --create-home --shell /bin/bash appuser

# Se transfiere la propiedad de todo el código de la aplicación al usuario recién creado.
# Esto asegura que el proceso de la aplicación tenga los permisos necesarios para leer sus propios archivos.
RUN chown -R appuser:appuser /app

# Se cambia el contexto de ejecución. Todas las instrucciones posteriores (y la ejecución final
# del contenedor) se realizarán como 'appuser', no como 'root'.
USER appuser

#
# --- ETAPA 3: CONTRATO DE EJECUCIÓN DEL CONTENEDOR ---

# Se asegura que el script de entrada tenga permisos de ejecución. Este paso es vital, ya que los
# sistemas de archivos (especialmente en Windows) no siempre preservan este bit de permiso.
RUN chmod +x /app/scripts/entrypoint.sh

# Define el punto de entrada principal del contenedor. Este script se ejecutará INCONDICIONALMENTE
# cada vez que se inicie un contenedor a partir de esta imagen. Es ideal para tareas de
# inicialización, como esperar a la base de datos o ejecutar migraciones.
ENTRYPOINT ["/app/scripts/entrypoint.sh"]

# Provee el comando por defecto que se pasará como argumento al ENTRYPOINT.
# Un buen script de entrypoint suele terminar con 'exec "$@"', ejecutando lo que recibe del CMD.
# El uso de 'tail -f /dev/null' es una técnica estándar para mantener el contenedor en ejecución
# de forma indefinida después de que el entrypoint finalice sus tareas de inicialización,
# permitiendo que el servicio principal (si lo hay) siga corriendo o facilitando la depuración.
CMD ["tail", "-f", "/dev/null"]