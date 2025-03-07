import logging
import os
import pandas as pd
from deltalake import write_deltalake, DeltaTable


def transform(bronze_path, silver_path):
    """Transforma los datos de Bronze a Silver"""
    if not os.path.exists(bronze_path):
        logging.error("No se encontró la capa Bronze.")
        return

    df = DeltaTable(bronze_path).to_pandas()
    df.columns = df.columns.str.replace('main.', '', regex=False)

    conversion_mapping = {
        "temp": 'int8', "feels_like": 'int8', "temp_min": 'int8',
        "pressure": 'int8', "humidity": 'int8', "sea_level": 'int8',
        "grnd_level": 'int8', "datetime": 'datetime64[ns]'
    }
    df = df.astype(conversion_mapping)

    new_dt = df["datetime"].max()

    if not os.path.exists(silver_path):
        write_deltalake(silver_path, df, mode="append", partition_by=["datetime"])
        logging.info("Silver inicializado con los datos actuales.")
    else:
        existing_data = DeltaTable(silver_path).to_pandas()
        if new_dt in existing_data["datetime"].values:
            logging.info("No hay nuevos datos para cargar en Silver.")
        else:
            write_deltalake(silver_path, df, mode="append", partition_by=["datetime"])
            logging.info("Datos cargados en Silver.")