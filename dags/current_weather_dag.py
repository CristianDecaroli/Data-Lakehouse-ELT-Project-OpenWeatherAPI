from airflow import DAG
from airflow.operators.python import PythonOperator
from config import *
from functions import extract, load, transform
from datetime import timedelta


# Configuración del DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": True,  # Dependencia de la tarea anterior
    "start_date": None,  # Inicia con la primera ejecución manual
    "retries": 1,  # Número de reintentos tras fallos
    "retry_delay": timedelta(seconds=30),  # Tiempo de espera entre reintentos
}

with DAG(
    "current_weather_dag",
    default_args=default_args,
    description="Pipeline de datos climáticos con Delta Lake",
    schedule_interval="2 * * * *",  # Se ejecuta cada hora con 2 minutos de margen
    catchup=False,  # Evita ejecutar tareas pasadas cuando el dag se reinicia tras haberlo pausado
) as dag:
    
    # Tarea de extracción
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract,
        op_args=[URL],  # URL de la API
    )

    # Tarea de carga con XComs
    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load,
        op_kwargs={"bronze_path": BRONZE_PATH},  # Pasar argumentos con op_kwargs
    )

    # Tarea de transformación
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
        op_args=[BRONZE_PATH, SILVER_PATH]
    )

    # Definir orden de ejecución
    extract_task >> load_task >> transform_task