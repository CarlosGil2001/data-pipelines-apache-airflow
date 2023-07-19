
# Librerías necesarias
import pandas as pd
import time
import random
import os
from datetime import datetime

# Función que ontiene las URL y paises de las LIGAS
def get_url():
    # URLs de cada una de las ligas
    url = ['https://www.espn.com.co/futbol/posiciones/_/liga/esp.1',
        'https://www.espn.com.co/futbol/posiciones/_/liga/eng.1',
        'https://www.espn.com.co/futbol/posiciones/_/liga/ita.1',
        'https://www.espn.com.co/futbol/posiciones/_/liga/ger.1',
        'https://www.espn.com.co/futbol/posiciones/_/liga/fra.1',
        'https://www.espn.com.co/futbol/posiciones/_/liga/por.1',
        'https://www.espn.com.co/futbol/posiciones/_/liga/ned.1']
    # Nombre de las Ligas
    ligas = ['ESPAÑA', 'INGLATERRA', 'ITALIA', 'GERMANY', 'FRANCIA', 'PORTUGAL', 'HOLANDA']
    # Crear el DF mediante las listas
    df_urls = {'LIGA': ligas, 'URL':url}
    df_urls = pd.DataFrame(df_urls)
    return df_urls    


# Función que extrae la información de las ligas (se ingresa la URL y el nombre de la LIGA)
def extract_data_ligas(url,liga):
    tiempo = [1,3,2] # controlar el tiempo de espera de las solicitudes
    time.sleep(random.choice(tiempo)) # simular una carga más realista en el servidor web y a evitar el bloqueo o la detección de web scraping.
    df_ligas = pd.read_html(url) # leer las tablas HTML de la pag web
    df_ligas=pd.concat([df_ligas[0],df_ligas[1]],ignore_index=True,axis=1) # contanenar los valores
    df_ligas=df_ligas.rename(columns={0:'EQUIPO',1:'J', 2:'G', 3:'E', 4:'P', 5:'GF', 6:'GC', 7:'DIF', 8:'PTS'}) # renombrar los nombres de las columnas
    df_ligas['EQUIPO']=df_ligas['EQUIPO'].apply(lambda x: x[5:] if x[:2].isnumeric()==True else x[4:]) # aplica la limpieza en la columna EQUIPO
    df_ligas['LIGA'] = liga # agregar nueva columna LIGA
    df_ligas['ID_TEAM'] = [str(random.randint(1000, 9999)) for _ in range(len(df_ligas))] # generar ID únicos para cada equipo
    now_date = datetime.now() # obtener fecha y hora actual
    run_date = now_date.strftime("%Y-%m-%d") # formatear la fecha
    df_ligas['CREATED_AT'] = run_date # crea la nueva columna
    return df_ligas

def data_processing():
    # Leer el DF de las URL de las ligas
    df_ligas=pd.read_csv("/opt/airflow/dags/dag_lg/data/ligas_url.csv")
    # obtener la data de las 7 ligas
    df_spain=extract_data_ligas(df_ligas['URL'][0],df_ligas['LIGA'][0])
    df_premier=extract_data_ligas(df_ligas['URL'][1],df_ligas['LIGA'][1])
    df_italy=extract_data_ligas(df_ligas['URL'][2],df_ligas['LIGA'][2])
    df_germany=extract_data_ligas(df_ligas['URL'][3],df_ligas['LIGA'][3])
    df_francia=extract_data_ligas(df_ligas['URL'][4],df_ligas['LIGA'][4])
    df_portugal=extract_data_ligas(df_ligas['URL'][5],df_ligas['LIGA'][5])
    df_holanda=extract_data_ligas(df_ligas['URL'][6],df_ligas['LIGA'][6])
    # Concatenar todas las ligas
    df_all=pd.concat([df_spain,df_premier,df_italy,df_francia,df_portugal,df_holanda],ignore_index=False)
    return df_all