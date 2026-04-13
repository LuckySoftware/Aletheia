import sys
import logging
from src.core.config_loader import ConfigLoader
from src.core.pipeline import AletheiaPipeline

# Configuración Global de Consola
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def main():
    config = ConfigLoader("config.json")
    if not config.plants:
        logging.error("No se encontraron plantas configuradas. Saliendo.")
        sys.exit(1)

    pipeline = AletheiaPipeline(config)

    for plant in config.plants:
        pipeline.run(plant)

if __name__ == "__main__":
    main()