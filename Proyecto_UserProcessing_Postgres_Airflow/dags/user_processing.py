# Importamos el objeto DAG
from airflow import DAG
# Importamos para el uso de fechas
from datetime import datetime
# Proveedor para utilizar el operador Postgres
from airflow.providers.postgres.operators.postgres import PostgresOperator
# Proveedor para utilizar el Operador HttpSensor
from airflow.providers.http.sensors.http import HttpSensor
# Proveedor para utilizar el operador SimpleHttpOperator (solicitudes de API)
from airflow.providers.http.operators.http import SimpleHttpOperator
# Proveedor para utilizar el operador PythonOperator
from airflow.operators.python import PythonOperator
# Hook PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Para trabajar con archivos JSON
import json
# Útil para normalizar datos JSON
from pandas import json_normalize

# Función de python - Almacenar los datos de la API en un CSV
def _process_user(ti):
    user= ti.xcom_pull(task_ids="extract_user") # extraer los datos obtenidos de "extract_user"
    user= user['results'][0] # se accede al primer resultado en la lista results obtenida. Esto asume que los datos extraídos de la tarea "extract_user" están estructurados de tal manera que hay una lista llamada "results" que contiene objetos de usuario
    processed_user = json_normalize({ # normalizar los datos extraídos en un formato tabular (DF)
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    })
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False) # se guarda en un archivo CSV

# Función de python - Carga masiva de datos desde un archivo CSV a una tabla
def _store_user():
    hook= PostgresHook(postgres_conn_id='postgres') # establecer conexión con la DB
    hook.copy_expert( # para realizar cargas masivas de datos en la tabla de la DB
        sql="COPY users FROM stdin WITH DELIMITER as ','", # destino de la crga masiva en la tabla "users"
        filename='/tmp/processed_user.csv' # archivo CSV que se cargará en la tabla
    )


# Instancia DAG
with DAG(
    'user_processing', # nombre del flujo de trabajo
    start_date=datetime(2023,7,20),  # fecha de inicio del flujo de trabajo
    schedule_interval='@daily', # ejecución diaria
    catchup=False #indicador que controla si las tareas anteriores se deben poner al día o no
    ) as dag:
   # cuerpo del flujo de trabajo DAG

   # Operador Postgres (tasks 1)
    create_table= PostgresOperator( # crear un Tabla en Postgres
        task_id='create_table',   # nombre de la tarea
        postgres_conn_id='postgres', # cadena de conexión
        sql='''
            CREATE TABLE IF NOT EXISTS users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
            '''
    )

    # Operador Sensor (tasks 2)

    # Host: https://randomuser.me/    -- definida en la conexión de Airflow UI
    # endpoint: api//
    # URL (Host + endpoint): https://randomuser.me/api/

    is_api_available = HttpSensor (   # verificar si la URL está activa o no
            task_id='is_api_available', # id de la tarea
            http_conn_id='user_api', # id de la conexión HTTP
            endpoint='api/' # punto final de la URL que se verificará
            )       


    # Operador SimpleHttpOperator (tasks 3)
    extract_user = SimpleHttpOperator( # para realizar una solicitud HTTP a la API y extraer datos
            task_id='extract_user', # id de la tarea
            http_conn_id = 'user_api', # id de la conexión HTTP para realizar la petición
            endpoint='api/', # punto final de la URL a la cual se realizará la solicitud
            method='GET', # método de la solicitud HTTP
            response_filter= lambda response: json.loads(response.text), # función lambda convierte el texto de la respuesta en un objeto JSON
            log_response= True # mostrar la respuesta de la solicitud HTTP en los registros de Airflow. 
    )

    # Operador PythonOperator (tasks 4)
    process_user= PythonOperator( # permite ejecutar una función de Python como una tarea en tu flujo de trabajo.
           task_id='process_user', # id de la tarea
           python_callable=_process_user # función de Python que se ejecutará como tarea (definida al inicio)
    )

    # Operador PythonOperator (tasks 4)
    store_user= PythonOperator( # permite ejecutar una función de Python como una tarea en tu flujo de trabajo.
           task_id='store_user', # id de la tarea
           python_callable=_store_user # función de Python que se ejecutará como tarea (definida al inicio)
    )


    # Definir Dependencias
    create_table >> is_api_available >> extract_user >> process_user >> store_user

