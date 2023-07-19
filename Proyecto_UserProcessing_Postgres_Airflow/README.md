# Data Pipeline Airflow - API - Postgres
![Airflow_API_Postgres](https://github.com/CarlosGil2001/data-pipelines-apache-airflow/assets/101606140/a247e799-3d1f-484a-8c3e-65e3c171ad63)
![image](https://github.com/CarlosGil2001/data-pipelines-apache-airflow/assets/101606140/1dae1e1e-5115-477f-b6ff-166aabb66f4c)

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

### 2) Testeamos las Tareas del DAG para probarlo
      airflow tasks test <name_dag> <tasks_name> <fecha_simular <= fecha_actual>
      airflow tasks test user_processing create_table 2023-07-13
      airflow tasks test user_processing is_api_available 2023-07-13
      airflow tasks test user_processing extract_user 2023-07-13
      airflow tasks test user_processing process_user 2023-07-13
      airflow tasks test user_processing store_user 2023-07-13
