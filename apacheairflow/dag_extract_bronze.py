from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import boto3
import io

S3_BUCKET_BRONZE = "abinbev_bucket_bronze"
AWS_REGION = "us-east-1"

def fetch_and_upload_bronze():
    API_URL = "https://api.openbrewerydb.org/breweries"
    response = requests.get(API_URL)
    if response.status_code == 200:
        df = pd.DataFrame(response.json())
        buffer = io.BytesIO()
        df.to_parquet(buffer, compression="gzip")
        buffer.seek(0)
        boto3.client("s3", region_name=AWS_REGION).upload_fileobj(
            buffer, S3_BUCKET_BRONZE, "bronze/breweries.parquet.gzip"
        )
    else:
        raise Exception(f"Erro na API: {response.status_code}")

default_args = {
    'owner': 'gustavo',
    'start_date': datetime(2024, 3, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bronze_layer_brewery',
    default_args=default_args,
    description='Captura dados da API e salva na camada Bronze',
    schedule_interval='@daily',
    catchup=False,
)

fetch_bronze_task = PythonOperator(
    task_id='fetch_and_upload_bronze',
    python_callable=fetch_and_upload_bronze,
    dag=dag,
)

fetch_bronze_task
