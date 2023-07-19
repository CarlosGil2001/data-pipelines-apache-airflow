# Data Pipeline Airflow - Snowflake

### Iniciar los servicio definidos en "docker-compose.json"
      docker-compose up -d

### Detener y eliminar los contenedores, redes y volumenes creados
      docker-compose down

### Ver el estado de los servicios (healthy)
      docker-compose ps


### Siempre cuando agreguemos una nueva Tarea hay que probar su flujo de trabajo en el DAG

#### 1) Entramos al contenedor del Programador (scheduler)
      docker-compose ps
      docker exec -it <dir>-airflow-scheduler-1 /bin/bash

### 2) Ejecutamos la Tarea del DAG para probarlo
      airflow tasks test <name_dag> <tasks_name> <fecha_simular <= fecha_actual>
      airflow tasks test processing_ligas get_url_country 2023-07-15
      airflow tasks test processing_ligas extract_data 2023-07-15
      airflow tasks test processing_ligas create_snowflake_objects 2023-07-15
      airflow tasks test processing_ligas upload_stage 2023-07-15
      airflow tasks test processing_ligas ingest_table 2023-07-15
