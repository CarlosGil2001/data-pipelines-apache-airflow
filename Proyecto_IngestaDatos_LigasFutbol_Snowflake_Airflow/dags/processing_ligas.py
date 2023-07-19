from datetime import datetime
from airflow.models import Variable
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator  # Operador para usar funciones en Python
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook # para conectarse a una instancia de Snowflake y ejecutar consultas (hook)
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator  # para realizar acciones en Snowflake, como crear tablas, cargar datos, extraer resultados.
import snowflake.connector as sf 
import pandas as pd
from datetime import datetime
import os
import sys

# Ruta al directorio que contiene los módulos personalizados
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'scripts'))
sys.path.append(module_path)
# Para reutilizar las funciones
from functions import data_processing, get_url


with DAG('processing_ligas', # Nombre del DAG
         description='Extracting Data Footbal League',
         start_date = datetime(2023, 7, 25), # fecha de inicio del DAG
         schedule_interval ='0 0 * * 1,6,7',  # Ejecutar a medianoche (00:00) los días sabados, domingos y lunes,
         tags=['tabla_ligas'],  # etiqueta
         catchup=False
         ) as dag :

        # Traer la información de las variables de "feature_info"
        params_info = Variable.get("feature_info", deserialize_json=True)
        
        # Obtener las URL y país de cada LIGA
        def get_urls_ligas():
              df_urls= get_url()
              df_urls.to_csv('/opt/airflow/dags/dag_lg/data/ligas_url.csv',index=False)
        
        # Extraer la información de los equipo y limpiar los datos
        def extract_ligas():
            df_all = data_processing()
            df_all = df_all[['ID_TEAM','EQUIPO', 'J', 'G', 'E', 'P', 'GF', 'GC', 'DIF', 'PTS', 'LIGA','CREATED_AT']]
            df_all.to_csv('/opt/airflow/dags/dag_lg/data/teams_ligas.csv',index=False)
         
         # Crear los objetos en Snowflake
        def create_snowflake_obj():
            # Crear una instancia en nowflake
            snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_lf')
            # Creación del objeto de almacenamiento (stage)
            create_stage_sql = "use snowflake_ligasfutbol; CREATE STAGE IF NOT EXISTS stage_lf;"
            snowflake_hook.run(create_stage_sql)
            # Creación de la tabla
            create_table_sql = """  use snowflake_ligasfutbol;
                                    CREATE OR REPLACE TABLE ligas_futbol(
                                    id         VARCHAR(30) NOT NULL,
                                    equipo     VARCHAR(30) NOT NULL,
                                    jugados    INTEGER NOT NULL,
                                    ganados    INTEGER NOT NULL,
                                    empatados  INTEGER NOT NULL,
                                    perdidos   INTEGER NOT NULL,
                                    GF         INTEGER NOT NULL,
                                    GC         INTEGER NOT NULL,
                                    diff       INTEGER NOT NULL,
                                    puntos     INTEGER NOT NULL,
                                    liga       VARCHAR(30) NOT NULL,
                                    created_at VARCHAR(30) NOT NULL
                                    );"""
            snowflake_hook.run(create_table_sql)

        # Tarea para obtiene las URL y paises de las ligas
        get_url_country = PythonOperator(task_id='get_url_country',
                                    provide_context=True,
                                    python_callable=get_urls_ligas)
        
        # Tarea para extraer la información
        extract_data = PythonOperator(task_id='extract_data',
                                    provide_context=True,
                                    python_callable=extract_ligas)
        
        # Tarea para crear los objetos en snowflake
        create_snowflake_objects = PythonOperator( task_id='create_snowflake_objects',
                                    provide_context=True,
                                    python_callable=create_snowflake_obj)

        # Cargar stage
        upload_stage = SnowflakeOperator(
            task_id='upload_stage',
            sql="PUT file://{path_file} @{stage} AUTO_COMPRESS=TRUE;".format(
                path_file=params_info["path_file"],
                stage=params_info["stage"]
            ),
            snowflake_conn_id='snowflake_lf', # nombre de la conexión
            warehouse=params_info["DWH"],
            database=params_info["DB"],
            role=params_info["ROLE"],
            params=params_info  # toma los variables de "feature_info"
        )

         # Cargar el CSV a la tabla
        ingest_table = SnowflakeOperator(
            task_id='ingest_table',
            sql="COPY INTO {table} FROM @{stage}/teams_ligas.csv.gz FILE_FORMAT=(TYPE=csv FIELD_DELIMITER=',' SKIP_HEADER=1) ON_ERROR='CONTINUE';".format(
                table=params_info["table"],
                stage=params_info["stage"]
            ),  
            snowflake_conn_id='snowflake_lf', # nombre de la conexión
            warehouse=params_info["DWH"],
            database=params_info["DB"],
            role=params_info["ROLE"],
            params=params_info # toma los variables de "feature_info"
        )

        # Dependencias de las TAREAS
        get_url_country >> extract_data >> create_snowflake_objects >> upload_stage >> ingest_table