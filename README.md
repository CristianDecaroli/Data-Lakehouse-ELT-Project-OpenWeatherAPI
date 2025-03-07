```txt
dag/
│── current_weather_dag.py  # DAG de Airflow

src/
│── __init__.py  # Indica que es un paquete
│── extract.py  # Extracción de datos
│── load.py  # Carga de datos en Bronze
│── transform.py  # Transformación de datos a Silver
│── config.py  # Configuración centralizada

pipeline.conf  # Archivo de configuración con credenciales y rutas
```