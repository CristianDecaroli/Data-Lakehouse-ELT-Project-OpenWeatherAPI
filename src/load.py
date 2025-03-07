import logging
import os
import pandas as pd
from deltalake import write_deltalake, DeltaTable
from src.extract import extract

def load(bronze_path):
    """Carga los datos extraídos en la capa Bronze"""
    raw_data = extract()
    if raw_data is None:
        logging.error("No hay datos para cargar en Bronze")
        return

    df = pd.DataFrame([raw_data])  # Convertir JSON en DataFrame
    new_dt = df["datetime"].max()

    if not os.path.exists(bronze_path):
        write_deltalake(bronze_path, df, mode="append", partition_by=["datetime"])
        logging.info("Bronze inicializado con los datos actuales.")
    else:
        existing_data = DeltaTable(bronze_path).to_pandas()
        if new_dt in existing_data["datetime"].values:
            logging.info("No hay nuevos datos para cargar.")
        else:
            write_deltalake(bronze_path, df, mode="append", partition_by=["datetime"])
            logging.info("Datos cargados en Bronze.")