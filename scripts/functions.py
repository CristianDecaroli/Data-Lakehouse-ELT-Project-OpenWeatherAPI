import pandas as pd
from deltalake import write_deltalake, DeltaTable
from datetime import datetime
import requests
import logging
import os

# EXTRACT
def extract(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        data['main']['datetime'] = datetime.now().strftime("%Y-%m-%d %H")
        logging.info("Datos extraídos correctamente")
        return data  # Se almacena en XCom automáticamente
    except requests.exceptions.RequestException as e:
        logging.error(f"Error en la extracción: {e}")
        return None  # Para evitar errores si la extracción falla

# LOAD
def load(bronze_path, **kwargs):
    ti = kwargs["ti"]  # Obtener task instance
    raw_data = ti.xcom_pull(task_ids="extract_data")  # Recuperar datos desde XComs

    if not raw_data:
        logging.error("No hay datos para cargar en Bronze.")
        return

    raw_data = pd.json_normalize(raw_data)
    
    if not os.path.exists(bronze_path):
        write_deltalake(bronze_path, raw_data, mode="append", partition_by=["main.datetime"])
        logging.info("Bronze inicializado con los datos actuales.")
    else:
        new_dt = raw_data["main.datetime"].max()
        existing_data = DeltaTable(bronze_path).to_pandas()
        if new_dt in existing_data["main.datetime"].values:
            logging.warning("Ya existen datos agregados en Bronze para esta fecha y hora.")
        else:
            write_deltalake(bronze_path, raw_data, mode="append", partition_by=["main.datetime"])
            logging.info("Datos cargados en Bronze.")

# TRANSFORM
def transform(bronze_path, silver_path):
    if not os.path.exists(bronze_path):
        logging.error("No se encontró la capa Bronze.")
        return
    else:
        df = DeltaTable(bronze_path).to_pandas()
        df = df.filter(like='main', axis=1)
        df.columns = df.columns.str.replace('main.', '', regex=False)
        conversion_mapping = {
            "temp": 'int8', "feels_like": 'int8', "temp_min": 'int8',
            "pressure": 'int8', "humidity": 'int8', "sea_level": 'int8',
            "grnd_level": 'int8', "datetime": 'datetime64[ns]'
        }
        df = df.astype(conversion_mapping)
        if not os.path.exists(silver_path):
            write_deltalake(silver_path, df, mode="append", partition_by=["datetime"])
            logging.info("Silver inicializado con los datos actuales.")
        else:
            new_dt = df["datetime"].max()
            existing_data = DeltaTable(silver_path).to_pandas()
            if new_dt in existing_data["datetime"].values:
                logging.info("No hay nuevos datos para cargar en Silver.")
            else:
                write_deltalake(silver_path, df, mode="append", partition_by=["datetime"])
                logging.info("Datos cargados en Silver.")