import pandas as pd
from deltalake import write_deltalake, DeltaTable
from datetime import datetime
from configparser import ConfigParser
import os
import logging
import requests
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Leer archivo de configuración
config = ConfigParser()
config.read('./pipeline.conf')

# Obtener valores de la configuración
api_key = config["API"]["api_key"]
base_url = config["API"]["url_base"]
bronze_path = config["STORAGE"]["bronze_path"]
silver_path = config["STORAGE"]["silver_path"]

# Endpoint de la API
endpoint = f"/weather?lat=-31.135&lon=-64.1811&appid={api_key}&units=metric"

# Construir URL
url = f"{base_url}{endpoint}"

def extract(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        if response.status_code == 200:
            logging.info("Conexión exitosa con la API")
            data = response.json()
            data['datetime'] = datetime.now().strftime("%Y-%m-%d %H")
            logging.info("Datos extraídos correctamente")
            return data

    except requests.exceptions.HTTPError as e:
        logging.error(f"Error HTTP al extraer los datos: {e}")
    except requests.exceptions.ConnectionError:
        logging.error("Error de conexión: no se pudo conectar a la API")
    except requests.exceptions.Timeout:
        logging.error("Error de timeout: la solicitud tardó demasiado")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error inesperado en la solicitud: {e}")
    return None

raw_data = extract(url) # Esto

def load(bronze_path, raw_data = extract(url)): # O esto?
    raw_data = pd.DataFrame(raw_data)
    new_dt = raw_data["datetime"].max()
    if not os.path.exists(bronze_path):
        write_deltalake(
            table_or_uri=bronze_path,
            data=raw_data,
            mode="append",
            partition_by=["datetime"]
        )
        logging.info("deltalake/bronze inicializado con los datos actuales.")
    else:
        try:
            existing_data = DeltaTable(bronze_path).to_pandas()
            if new_dt in existing_data["datetime"].values:
                logging.info("No hay nuevos datos para cargar.")
            else:
                write_deltalake(
                    table_or_uri=bronze_path,
                    data=raw_data,
                    mode="append",
                    partition_by=["datetime"]
                )
                logging.info("Datos cargados correctamente.")
        except Exception as e:
            logging.error(f"Error al cargar los datos: {e}")
    logging.info("Cantidad de registros en deltalake/bronze: " + str(DeltaTable(bronze_path).to_pandas().shape[0]))

load(bronze_path, raw_data) # TO AIRFLOW DAG

def transform(silver_path, bronze_path):
    final_df = DeltaTable(bronze_path).to_pandas()

    final_df.columns = final_df.columns.str.replace('main.', '', regex=False)

    conversion_mapping  = {
        "temp": 'int8',         
        "feels_like": 'int8',   
        "temp_min": 'int8',
        "pressure": 'int8',
        "humidity": 'int8',
        "sea_level": 'int8',
        "grnd_level": 'int8',
        "datetime": 'datetime64[ns]'    
    }
    final_df = final_df.astype(conversion_mapping)
    
    new_dt = final_df["datetime"].max()
    
    if not os.path.exists(silver_path):
        write_deltalake(
            table_or_uri=silver_path,
            data=final_df,
            mode="append",
            partition_by=["datetime"]
        )
        logging.info("deltalake/silver inicializado con los datos actuales.")
    else:
        try:
            existing_data = DeltaTable(silver_path).to_pandas()
            if new_dt in existing_data["datetime"].values:
                logging.info("No hay nuevos datos para cargar.")
            else:
                write_deltalake(
                    table_or_uri=silver_path,
                    data=final_df,
                    mode="append",
                    partition_by=["datetime"]
                )
                logging.info("Datos cargados correctamente.")
        except Exception as e:
            logging.error(f"Error al cargar los datos: {e}")
        logging.info("Cantidad de registros en deltalake/silver: " + str(DeltaTable(silver_path).to_pandas().shape[0]))

transform(silver_path, bronze_path) # TO AIRFLOW DAG
