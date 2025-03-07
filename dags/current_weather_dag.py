from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from src.config import BRONZE_PATH, SILVER_PATH, URL
from src.extract import extract
from src.load import load
from src.transform import transform

# Configuración del DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": True, # Dependencia de la tarea anterior
    "email": ["cristiandecarol19@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "start_date": None,  # Inicia con la primera ejecución manual
    "retries": 1, # Número de reintentos tras fallos
    "retry_delay": timedelta(minutes=1), # Tiempo de espera entre reintentos
}

with DAG(
    "weather_pipeline",
    default_args=default_args,
    description="Pipeline de datos climáticos con Delta Lake",
    schedule_interval="2 * * * *",  # Se ejecuta cada hora con 2 minutos de margen
    catchup=False,  # Evita ejecutar tareas pasadas cuando el dag se reinicia tras haberlo pausado
) as dag:
    # Definir tareas
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract,
        op_args=[URL]
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load,
        op_args=[BRONZE_PATH]
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
        op_args=[BRONZE_PATH, SILVER_PATH]  # Pasas las rutas como argumentos
    )

    # Definir orden de ejecución
    extract_task >> load_task >> transform_task