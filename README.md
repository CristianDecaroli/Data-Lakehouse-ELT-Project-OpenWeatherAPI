# Proyecto ELT para un Data Lakehouse

Este proyecto implementa un proceso **ELT** (Extract, Load, Transform) para gestionar y analizar datos meteorológicos, utilizando la API de **OpenWeather**. El flujo de trabajo está organizado en dos notebooks principales:

1. **`extract-load`**: Responsable de la extracción y carga inicial de los datos en la capa Bronze.
2. **`transform`**: Encargado de transformar los datos y cargarlos en la capa Silver.

## Fuentes de Datos

Se emplean los siguientes endpoints de la API OpenWeather para la extracción de datos:

### 1. **`Current Weather`**
- **API URL**: [https://openweathermap.org/current](https://openweathermap.org/current)
- **Técnica de extracción**: Incremental.
- **Método utilizado**: `append`.
- **Frecuencia de ejecución**: Cada una hora.
- **Descripción**: 
  - Los datos se almacenan de forma particionada por fecha y hora.
  - Se realizan validaciones para garantizar que no haya duplicados en los datos agregados.
  - Este enfoque permite mantener un control estricto sobre los datos meteorológicos actuales en tiempo real.

### 2. **`History Weather`**
- **API URL**: [https://openweathermap.org/forecast5](https://openweathermap.org/forecast5)
- **Técnica de extracción**: Completa (**full**, Tipo 1).
- **Método utilizado**: `overwrite`.
- **Frecuencia de actualización**: La API genera datos nuevos cada 3 horas.
- **Descripción**: 
  - El programa está pensando para que en cada ejecución sobrescriba los datos existentes en el directorio de almacenamiento.
  - Esto permite obtener el pronóstico meteorológico para los próximos 5 días de manera precisa y actualizada.

## Archivos Complementarios

- **`requirements.txt`**: Este archivo contiene la lista de librerías necesarias para ejecutar el proyecto. Puedes instalar las dependencias ejecutando:
  ```bash
  pip install -r requirements.txt
  ```
- **`pipeline.conf`**: Este archivo almacena datos sensibles, como las credenciales necesarias para acceder a los endpoints de la API. 

Para probar el código, deberas crear el archivo pipeline.conf y agregar las APIkeys de tu cuenta de OpenWeather, manteniendo la variable 'url_base' que figura en el próximo ejemplo. El archivo pipeline.conf debería quedarte de la siguiente manera:
```plaintext
[API]
api_key = coloca aquí tu apikey

[URL]
url_base = https://api.openweathermap.org/data/2.5
```