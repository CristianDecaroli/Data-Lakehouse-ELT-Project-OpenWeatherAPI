import logging
from configparser import ConfigParser

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Leer archivo de configuración
config = ConfigParser()
config.read('./pipeline.conf')  # Ruta en Airflow

# Exponer variables de configuración
API_KEY = config["API"]["api_key"]
BASE_URL = config["API"]["url_base"]
BRONZE_PATH = config["STORAGE"]["bronze_path"]
SILVER_PATH = config["STORAGE"]["silver_path"]

# Construcción de la URL de la API
ENDPOINT = f"/weather?lat=-31.135&lon=-64.1811&appid={API_KEY}&units=metric"
URL = f"{BASE_URL}{ENDPOINT}"