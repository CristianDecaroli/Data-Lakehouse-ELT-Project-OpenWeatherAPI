import requests
import logging
from datetime import datetime

def extract(url):
    """Extrae datos desde la API y los devuelve en formato JSON"""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        data['datetime'] = datetime.now().strftime("%Y-%m-%d %H")
        logging.info("Datos extraídos correctamente")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error en la extracción: {e}")
        return None