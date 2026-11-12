# Proyecto para el curso de Big Data: Procesamiento de datos con Apache Spark

## Descripción de la Solución
Este proyecto se basa en la realización de un procesamiento de datos utilizando Apache spark(PySpark) y
su posterior visualización en tiempo real (Streaming) sobre la arquitectura de una maquina local optimizada para
este fin con las características de macOS Catalina, Miniconda, Python 3.9 cuyo objetivo es procesar y 
analizar un conjunto de datos histórico del Titanic.

## Estructura del Repositorio
- `batch_processing.py`: Script principal de Python para el procesamiento y generación del gráfico.
- `streaming_processing.py`: Script para el procesamiento en tiempo real basado en monitoreo de directorios.
- `datos_prueba.csv`: Datos en CSV del Titanic utilizado como fuente de datos.
- `resultado_batch.png`: Gráfico exportado que muestra la distribución de pasajeros por clase.

## Instrucciones de la Ejecución

### 1. Requisitos previos
- Tener instalado **Miniconda** y Python 3.9.

### 2. Configuración del entorno virtual
Abrir el terminal de la mac y activar el entorno:
  ´´´bash
  conda activate bigdata_env
  cd ~/proyecto_bigdata

### 3. Ejecución del procesamiento Batch
  bash
  python batch_processing.py

### 4. Ejecución del procesamiento en Streaming
  bash
  python streaming_processing.py

### 5. Abrir una Segunda Terminal
  ´´´bash
  conda activate bigdata_env
  cd ~/proyecto_bigdata

### 6. Simular la llegada de un lote de datos en vivo en la segunda terminal 
  bash
  head -n 15 datos_prueba.csv > stream_input/lote1.csv


  
