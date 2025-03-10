# Proyecto ELT para Data Lakehouse - OpenWeatherAPI

Este proyecto implementa un pipeline de datos climáticos utilizando Apache Airflow. Extrae datos de una API de clima, los carga de manera incremental en un Datalake y los transforma para su posterior análisis. El proyecto está desarrollado en Python y utiliza varias librerías como requests, pandas y deltalake para el procesamiento de datos.

## Estructura del Proyecto

```plaintext
Data-Lakehouse-ELT-Project-OpenWeatherAPI/
│
├── dag/                 # Directorio principal de Airflow donde se encuentran los DAGs
│   └── current_weather_dag.py  # Archivo principal y orquestador de tasks/funciones
│
├── config/              # Directorio de configuración de Airflow
│
├── logs/                # Directorio de logs de Airflow
│
├── plugins/             # Directorio de plugins de Airflow
│
├── scripts/             # Directorio donde se encuentran los scripts de Python
│   ├── __init__.py      # Inicializa el contenido de scripts/
│   ├── config.py        # Lee archivo de configuración pipeline.conf y convierte en variables Python
│   ├── functions.py     # Contiene las funciones principales del proyecto (extract, load, transform)
│   └── test.ipynb       # Jupyter Notebook para probar los scripts y analizar los datos procesados
│
├── datalake/            # Directorio donde se guarda la data extraída en formato .parquet, particionada por hora
│
├── demo_images/         # Directorio dodne se guardan imágenes que demuestran el correcto funcionamiento del proyecto
├── .env                 # Variable de entorno para que Airflow instale las dependencias dentro de Docker Compose
├── .gitignore           # Archivo de Git para ignorar archivos y/o directorios no deseados
├── pipeline.conf        # Archivo de configuración que contiene datos de API y rutas de directorios
├── README.md            # Información y documentación del proyecto
```
## Instrucciones para Clonar y Ejecutar el Proyecto

### 1. Requisitos Previos

Antes de comenzar, asegúrate de tener instalados los siguientes programas en tu máquina:

- **Python 3.x**: [Descargar Python](https://www.python.org/downloads/)
- **Docker**: [Descargar Docker](https://www.docker.com/get-started)
- **Docker Compose**: Viene incluido con Docker, pero si necesitas instrucciones adicionales, puedes encontrarlas [aquí](https://docs.docker.com/compose/install/).

### 2. Clonar el Repositorio

Abre una terminal y ejecuta el siguiente comando para clonar el repositorio:

```bash
git clone https://github.com/CristianDecaroli/Data-Lakehouse-ELT-Project-OpenWeatherAPI.git
```

### 3. Acceder al Repositorio

Navega al directorio del repositorio clonado:

```bash
cd Data-Lakehouse-ELT-Project-OpenWeatherAPI
```

### 4. Crear una cuenta gratuita y obten tu API KEY en OpenWeather

- **OpenWeather**: [https://openweathermap.org/](https://openweathermap.org/)


### 5. Agregar tu API KEY al archivo de configuración `pipeline.conf`

Revisa el archivo de configuración. Debes agregar tu API KEY.
```conf
[API]
api_key = YOUR API KEY
url_base = https://api.openweathermap.org/data/2.5

[STORAGE]
bronze_path = ./datalake/bronze/weather_data
silver_path = ./datalake/silver/weather_data
```

### 6. Ejecutar Docker Compose

Docker Compose está configurado para instalar todas las dependencias necesarias para el proyecto. Ejecuta el siguiente comando para levantar los servicios:

```bash
docker-compose up -d
```

Este comando descargará las imágenes, levantará los contenedores necesarios e instalará las depedencias para el proyecto.

### 7. Acceder a la Interfaz Web de Airflow

Una vez que Docker Compose haya levantado los contenedores, podrás abrir tu navegador y acceder a la siguiente URL para ver la interfaz web de Airflow:

```plaintext
http://localhost:8080
```

### 8. Correr el DAG de Airflow

En la interfaz web de Airflow, encontrarás el DAG llamado **current_weather_dag**. Simplemente habilítalo y ejecuta el DAG para comenzar a procesar los datos climáticos.

---

## Desafíos Resueltos y Lecciones Aprendidas

A lo largo del desarrollo de este proyecto, he abordado y resuelto varios desafíos técnicos que han enriquecido mi comprensión de **Docker**, **Airflow**, y el flujo de trabajo general de un pipeline ELT en Python.

- **Configuración de Docker Compose**: Realicé las configuraciones necesarias dentro de Docker Compose para garantizar que los directorios del proyecto sean correctamente montados y accesibles por los contenedores. Específicamente, monté los volúmenes correspondientes para que las carpetas de **dags**, **scripts** y otras sean leídas y procesadas adecuadamente.

- **Modularización del Código**: Para mejorar la organización y reutilización del código, modularicé el contenido dentro de la carpeta **scripts**, lo que permitió que los archivos de esta carpeta sean fácilmente accesibles y gestionables desde diferentes partes del proyecto.

- **Configuración de Pythonpath**: Definí el directorio **scripts** como parte del **PYTHONPATH** para asegurar que Airflow y otros componentes del sistema puedan importar y ejecutar el código de manera eficiente.

- **Integración con Airflow**: Configuré una variable de entorno en Airflow para permitir la lectura de archivos `.env` y, por ende, asegurar que las dependencias del proyecto sean instaladas correctamente dentro del entorno de Docker. Esto facilita la gestión de configuraciones y la instalación de librerías necesarias como **requests**, **pandas**, y **deltalake**.

- **Uso de XComs**: Implementé la funcionalidad de **XComs** en Airflow para pasar datos entre tareas dentro del DAG. Esto permitió que el resultado de una tarea, como la extracción de datos, fuera utilizado como entrada para la siguiente tarea, garantizando un flujo de trabajo fluido y sin interrupciones.

- **Manejo de errores y excepciones**: Implementé **Try** y **Except** para realizar comprobaciones de conección exitosa o no, **if** y **else** para validar acciones. Utilicé la librería **logging** para mostrar logs y no prints.

- **Eliminación de Ejemplos Predeterminados en Airflow**: Para evitar la sobrecarga de ejemplos innecesarios, configuré Airflow para anular la visualización de los DAGs predeterminados, asegurando un entorno más limpio y enfocado en las tareas relevantes.

- **Programación de Tareas con CRON**: Utilicé expresiones **CRON** para definir el intervalo de ejecución del DAG en Airflow, programando la ejecución cada hora con 2 minutos de margen. Esto permitió asegurar que los datos de la API estén disponibles antes de que se ejecute el proceso de extracción, considerando que la API carga nuevos datos cada hora.
