# Prueba DE Wallmart


Antes de comenzar con la explicación del proceso, se tomaron las siguientes decisiones y supuestos:

* La próblematica se abordó principalmente por un punto de vista más análitico a nivel de zonas conceptuales en un entorno cloud.
* Desde un punto de vista funcional se toma la decisión de trabajar con python, para poder cumplir de mejor forma con las necesidades de negocio
* Se trabaja en un entorno cloud, para que la integracion de los servicios tales como: proceso CI, ejecución end-to-end, creación de zonas conceptuales,
  futuros requerimientos desde el área de analytics, exploracion de datos mediante lenguaje declarativo sql, entre otros. Además, de la posibilidad de poder migrar los procesos actuales de Wallmart a GCP.
* Los reportes se entregan en la zona de analytics mediante tablas en el servicio BigQuery
* Los reportes no involucran los ranking con valores "tbd"
* Los reportes asumen de que el metascore es un valor más significativo que el userscore al momento de buscar los mejores o peores juegos


#### 1) Origenes de datos, exploración e información del negocio      

       
      Se cuenta con dos fuentes de información para poder lograr los objetivos de la casuística. 
        1.1) Se cuenta con un origen de datos llamado consoles.csv que contiene la información de la compañía de videojuegos y su respectiva consola de ésta
        1.2) Se cuenta con un origen de datos llamado result.csv que contiene la información del score de un juego para una determinada consola
      Se añade catalogo de datos para la zona raw de la información, en conjunto con las definiciones principales de los campos
      https://github.com/Jcartesfs/de_wallmart/blob/main/DataGovernance/DataRaw/data_catalog_metacritics%20-%20catalog.pdf
      Se añade link del proceso de exploración mediante un notebook:
      https://github.com/Jcartesfs/de_wallmart/blob/main/DataExploration/data_exploration.ipynb
      
#### 2) Visión macro del proceso end to end basado en de diagramas conceptuales tales como arquitectura GCP, zonas de almacenamiento de datos y modelamiento de datos a nivel de BI
      Se trabajo con 3 zonas conceptuales que están representadas físicamente en GCP:

      2.1) Zona raw: Se almacena la data en su forma original sin sufrir modificaciones. 
      2.2) Zona dwh: Se almacena el modelo en 3FN tipo dwh
      2.3) Zona analytics: Se almacena los reportes a nivel de tablas SQL
      
      
#### 3) Proceso de extracción

      El proceso de extracción tiene como primer input el siguiente bucket gs://data_raw_metacritic_games que contiene los datos de los archivos result.csv, consoles.csv
      
#### 4) Proceso de transformacion

      En el proceso de transformación se separa la logica de negocio, básicamente añadida al proceso de BI el cuál genera las tablas dimensionales y su respectiva ft para que 
      puedan ser consumidas desde la capa de analytics
#### 5) Proceso de carga
      Las cargas se realizan tanto en Storage como BQ. Se apertura de la siguiente forma
      
      5.1) Storage
        * Zona raw: gs://data_raw_metacritic_games
        * Zona dwh: gs://data_dwh_metacritic_games
      5.2) BigQuery:
        * Dataset zona raw: https://console.cloud.google.com/bigquery?project=de-wallmart&authuser=1&p=de-wallmart&d=data_raw_metacritic_games&page=dataset
        * Dataset zona dwh: https://console.cloud.google.com/bigquery?project=de-wallmart&authuser=1&p=de-wallmart&d=data_dwh_metacritic_games&page=dataset
        * Dataset zona analytics (reporteria diaria): https://console.cloud.google.com/bigquery?project=de-wallmart&authuser=1&p=de-wallmart&d=data_analytics_metacritic_games&page=dataset
#### 6) Gobierno de datos
       Se trabaja con un catalogo de datos inicial https://github.com/Jcartesfs/de_wallmart/blob/main/DataGovernance/DataRaw/data_catalog_metacritics%20-%20catalog.pdf
   
      
#### 7) Devops

      Se genera una integracion continua mediante un trigger que escucha éste repositorio github desde el servicio Cloud Build de GCP.
      La imagen generada se puede descargar ya que por el momento está en estado public. Su ruta es la siguiente: gcr.io/de-wallmart/github.com/jcartesfs/de_wallmart
#### 8) Deuda Técnica y mejoras futuras
      * Linajes de datos para las zonas dwh y analytics
      * Catalogo de datos para las zonas dwh y analytics
      * Transformaciones entre transición de zonas
      * Variables de entorno del desarrollo Docker
      * Aplicar transformaciones tipo CAST para las zonas dwh y analytics en BQ
      * Creación metadata en las etapas ETL para posterior almacenamiento en servicio loggging y generar alertas con monitoring
      * CD a nivel de devops para el servicio que mantiene el microservicio  (Cloud Run)
      * Calendarizacion mediante airflow, cloud scheduler, argo u otro, para su ejecución de forma diaria
      * Migracion del proceso .py a lenguajes .scala o .java

#### 9) Ejecución de proceso

        Por el momento, el proceso puede ser ejecutado accediendo a la siguiente ruta del microservicio https://cloud-run-etl-process-de-6tqyabwxua-uc.a.run.app/exec.
        El proceso realiza la extraccion de datos raw desde el bucket de la zona raw, realiza las transformaciones correspondientes para actualizar los datos en BQ bajo la       modalidad WRITE_TRUNCATE almacenando éstas en la zona dwh. Posterior a este paso, se genera la reportería en la zona analytics de BQ, también bajo una modalidad WRITE_TRUNCATE y generando los 4 reportes esperados
