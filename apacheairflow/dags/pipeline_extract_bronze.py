from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from datetime import datetime, timedelta
import requests
import pandas as pd
import boto3
import io
import os

S3_BUCKET = "abinbev_bucket_bronze"
S3_KEY = "bronze/breweries.parquet.gzip"
AWS_REGION = "us-east-1"  

def fetch_and_upload_data():
    API_URL = "https://api.openbrewerydb.org/breweries"
    response = requests.get(API_URL)

    if response.status_code == 200:
        data = response.json()

        df = pd.DataFrame(data)
        buffer = io.BytesIO()
        df.to_parquet(buffer, compression="gzip")
        buffer.seek(0)

        s3_client = boto3.client("s3", region_name=AWS_REGION)
        s3_client.upload_fileobj(buffer, S3_BUCKET, S3_KEY)
        print(f"Arquivo salvo no S3: s3://{S3_BUCKET}/{S3_KEY}")
    else:
        raise Exception(f"Erro na API: {response.status_code}")

# Configurações padrão da DAG
default_args = {
    'owner': 'gustavo',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['seuemail@example.com'],  
}

dag = DAG(
    'brewery_pipeline_s3',
    default_args=default_args,
    description='Pipeline para processar dados de cervejarias e salvar no S3',
    schedule_interval='@daily',
    catchup=False,
)

fetch_task = PythonOperator(
    task_id='fetch_and_upload_data',
    python_callable=fetch_and_upload_data,
    dag=dag,
)

file_sensor_task = S3KeySensor(
    task_id='check_s3_file',
    bucket_key=S3_KEY,
    bucket_name=S3_BUCKET,
    aws_conn_id='aws_default',  
    poke_interval=30,  
    timeout=300,  
    mode='poke',
    dag=dag,
)

email_alert_task = EmailOperator(
    task_id='send_email_on_failure',
    to='seuemail@example.com',
    subject=' Falha na Pipeline Brewery',
    html_content='A pipeline Brewery falhou. Verifique os logs no Airflow.',
    trigger_rule='one_failed', r
    dag=dag,
)

fetch_task >> file_sensor_task >> email_alert_task
