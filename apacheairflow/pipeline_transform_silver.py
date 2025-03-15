from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from datetime import datetime, timedelta
import pandas as pd
import boto3
import io

S3_BUCKET_BRONZE = "abinbev_bucket_bronze"
S3_BUCKET_SILVER = "abinbev_bucket_silver"
AWS_REGION = "us-east-1"

def transform_silver():
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    response = s3_client.get_object(Bucket=S3_BUCKET_BRONZE, Key="bronze/breweries.parquet.gzip")
    df = pd.read_parquet(io.BytesIO(response['Body'].read()))

    df = df.drop(columns=["website_url", "phone"], errors="ignore")
    df = df.fillna({"state": "Unknown"})
    df["state"] = df["state"].str.replace(" ", "_")

    buffer = io.BytesIO()
    df.to_parquet(buffer, partition_cols=["state"], compression="snappy")
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, S3_BUCKET_SILVER, "silver/breweries.parquet")

default_args = {
    'owner': 'gustavo',
    'start_date': datetime(2024, 3, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'silver_layer_brewery',
    default_args=default_args,
    description='Transforma os dados da Bronze e armazena na Silver',
    schedule_interval='@daily',
    catchup=False,
)

sensor_bronze = S3KeySensor(
    task_id='check_bronze_file',
    bucket_key="bronze/breweries.parquet.gzip",
    bucket_name=S3_BUCKET_BRONZE,
    aws_conn_id='aws_default',
    poke_interval=30,
    timeout=300,
    mode='poke',
    dag=dag,
)

transform_silver_task = PythonOperator(
    task_id='transform_silver',
    python_callable=transform_silver,
    dag=dag,
)

sensor_bronze >> transform_silver_task
