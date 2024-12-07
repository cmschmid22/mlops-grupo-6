from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import time 
import psycopg2
import boto3
from io import StringIO


default_args = {
    'owner': 'kevin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_processing_pipeline',
    default_args=default_args,
    description='Pipeline para procesamiento de datos y generación de recomendaciones',
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 23),
    catchup=False,
)

def leer_s3(nombre_archivo):
    ACCESS_KEY = 'AKIAVY2PG64L5G3OBJ4I'
    SECRET_KEY = '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'

    s3 = boto3.client(
    's3',
    region_name='us-east-1',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY)
    bucket_name = 'grupo-6-mlops'
    file_name = nombre_archivo
    file_object = s3.get_object(Bucket=bucket_name, Key=file_name)
    df = pd.read_csv(file_object['Body'], sep=',')
    return df

def guardar_s3(df, nombre_archivo):
    ACCESS_KEY = 'AKIAVY2PG64L5G3OBJ4I'
    SECRET_KEY = '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'

    # Crear cliente de S3
    s3 = boto3.client(
        's3',
        region_name='us-east-1',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    
    # Especificar el bucket de destino
    bucket_name = 'grupo-6-mlops'

    # Convertir el DataFrame a un CSV en memoria
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Subir el archivo al bucket
    s3.put_object(Bucket=bucket_name, Key=nombre_archivo, Body=csv_buffer.getvalue())



#def filtrar_datos(**kwargs):
def filtrar_datos():
    # Obtener la fecha de ejecución programada (ejemplo: 2024-11-23)
    #execution_date = kwargs['execution_date']
    execution_date = datetime.now()

    # Calcular la fecha del día anterior
    fecha_anterior = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Leer los archivos CSV
    df_ads_views = leer_s3('ads_views.csv')
    df_advertiser_ids = leer_s3('advertiser_ids.csv')
    df_product_views = leer_s3('product_views.csv')

    #Cambiamos formato de fechas a datetime
    df_ads_views['date'] = pd.to_datetime(df_ads_views['date'])
    df_product_views['date'] = pd.to_datetime(df_product_views['date'])
    
    # Luego, Filtrar los datos basados en la fecha del día anterior
    filtered_ads_views = df_ads_views[df_ads_views['date'] == fecha_anterior]
    filtered_product_views = df_product_views[df_product_views['date'] == fecha_anterior]
    
    # Filtrar los datos por los IDs de anunciantes activos
    active_advertisers = df_advertiser_ids['advertiser_id']
    filtered_ads_views = filtered_ads_views[filtered_ads_views['advertiser_id'].isin(active_advertisers)]
    filtered_product_views = filtered_product_views[filtered_product_views['advertiser_id'].isin(active_advertisers)]
    
    # Guardar los archivos filtrados
    guardar_s3(filtered_ads_views, 'filtered_ads_views' + '_' + fecha_anterior + '.csv')
    guardar_s3(filtered_product_views, 'filtered_product_views' + '_' + fecha_anterior + '.csv')

def calcular_top_ctr():
    # Obtener la fecha de ejecución programada (ejemplo: 2024-11-23)
    #execution_date = kwargs['execution_date']
    execution_date = datetime.now()

    # Calcular la fecha del día anterior
    fecha_anterior = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Leer los logs de vistas de anuncios
    df_filtered_ads_views = leer_s3('filtered_ads_views' + '_' + fecha_anterior + '.csv')
    
    # Calcular el número de impresiones y clics por producto y advertiser_id
    df_impressions = df_filtered_ads_views[df_filtered_ads_views['type'] == 'impression'].groupby(['advertiser_id', 'product_id']).size().reset_index(name='impressions')
    df_clicks = df_filtered_ads_views[df_filtered_ads_views['type'] == 'click'].groupby(['advertiser_id', 'product_id']).size().reset_index(name='clicks')
    
    # Unir las dos tablas
    df_ctr = pd.merge(df_impressions, df_clicks, on=['advertiser_id', 'product_id'], how='inner')
    df_ctr['ctr'] = df_ctr['clicks'] / df_ctr['impressions']


    
    # Filtrar los 20 productos con el mejor CTR por advertiser_id
    df_top_ctr = df_ctr.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'ctr')).reset_index(drop=True)
    
    df_top_ctr['fecha'] = fecha_anterior
    
    # Guardar los resultados
    guardar_s3(df_top_ctr, 'top_ctr'+ '_' + fecha_anterior + '.csv')


def calcular_top_product():
        # Obtener la fecha de ejecución programada (ejemplo: 2024-11-23)
    #execution_date = kwargs['execution_date']
    execution_date = datetime.now()

    # Calcular la fecha del día anterior
    fecha_anterior = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    
    df_filtered_product_views = leer_s3('filtered_product_views' + '_' + fecha_anterior + '.csv')
    
    # Contar las vistas por producto y advertiser_id
    df_top_products = df_filtered_product_views.groupby(['advertiser_id', 'product_id']).size().reset_index(name='views')
    df_top_products = df_top_products[df_top_products["views"]>0]

    # Filtrar los 20 productos más vistos por advertiser_id
    df_top_products = df_top_products.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'views')).reset_index(drop=True)
    
    df_top_products['fecha'] = fecha_anterior
    
    # Guardar los resultados
    guardar_s3(df_top_products, 'top_views'+ '_' + fecha_anterior + '.csv')

# Definir la tarea de espera
def espera_task():
    time.sleep(30)
    
def guardar_ctr_en_db():
    
    execution_date = datetime.now()
    fecha_anterior = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    
    ctr_input = leer_s3('top_ctr'+ '_' + fecha_anterior + '.csv')
    
    engine = psycopg2.connect(
    database="postgres",
    user="postgres",
    password="abcde12345",
    host="grupo-6-rds.cf4i6e6cwv74.us-east-1.rds.amazonaws.com",
    port='5432'
    )
    cursor = engine.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS top_ctr (advertiser_id VARCHAR(255), product_id VARCHAR(255), impressions INT, clicks INT, ctr FLOAT
, fecha DATE);""")

    # Crear la consulta de inserción
    insert_query = """
    INSERT INTO top_ctr (advertiser_id, product_id, impressions, clicks, ctr, fecha)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    # Convertir el DataFrame a una lista de tuplas
    data = list(ctr_input.itertuples(index=False, name=None))
    
    # Insertar los datos en la base de datos
    try:
        cursor.executemany(insert_query, data)
        engine.commit()
        print("Datos insertados exitosamente.")
    except Exception as e:
        print(f"Error: {e}")
        engine.rollback()
        
def guardar_views_en_db():
    
    execution_date = datetime.now()
    fecha_anterior = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    
    views_input = leer_s3('top_views'+ '_' + fecha_anterior + '.csv')
    
    engine = psycopg2.connect(
    database="postgres",
    user="postgres",
    password="abcde12345",
    host="grupo-6-rds.cf4i6e6cwv74.us-east-1.rds.amazonaws.com",
    port='5432'
    )	
    cursor = engine.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS top_product (advertiser_id VARCHAR(255), product_id VARCHAR(255), views INT, fecha DATE);""")

    # Crear la consulta de inserción
    insert_query = """
    INSERT INTO top_product (advertiser_id, product_id, views, fecha)
    VALUES (%s, %s, %s, %s)
    """
    
    # Convertir el DataFrame a una lista de tuplas
    data = list(views_input.itertuples(index=False, name=None))
    
    # Insertar los datos en la base de datos
    try:
        cursor.executemany(insert_query, data)
        engine.commit()
        print("Datos insertados exitosamente.")
    except Exception as e:
        print(f"Error: {e}")
        engine.rollback()

        

# Crear las tareas en el DAG
filtrar_task = PythonOperator(
    task_id='FiltrarDatos',
    python_callable=filtrar_datos,
    dag=dag,
)

espera_30_task = PythonOperator(
    task_id='wait_30_seconds',
    python_callable=espera_task,
    dag=dag,
)

calcular_top_ctr_task = PythonOperator(
    task_id='CalcularTopCTR',
    python_callable=calcular_top_ctr,
    op_args=[],
    dag=dag,
)

calcular_top_product_task = PythonOperator(
    task_id='CalcularTopProduct',
    python_callable=calcular_top_product,
    op_args=[],
    dag=dag,
)

guardar_ctr_en_db_task = PythonOperator(
    task_id='GuardarCtrEnDb',
    python_callable=guardar_ctr_en_db,
    op_args=[],
    dag=dag,
)

guardar_views_en_db_task = PythonOperator(
    task_id='GuardarViewsEnDb',
    python_callable=guardar_views_en_db,
    op_args=[],
    dag=dag,
)

# Establecer dependencias entre las tareas
filtrar_task >> espera_30_task
espera_30_task >> calcular_top_ctr_task
espera_30_task >> calcular_top_product_task
calcular_top_ctr_task >> guardar_ctr_en_db_task
calcular_top_product_task >> guardar_views_en_db_task
