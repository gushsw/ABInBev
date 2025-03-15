from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from datetime import datetime, timedelta
import pandas as pd
import boto3
import io

S3_BUCKET_SILVER = "abinbev_bucket_silver"
S3_BUCKET_GOLD = "abinbev_bucket_gold"
AWS_REGION = "us-east-1"

def aggregate_gold():
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    response = s3_client.get_object(Bucket=S3_BUCKET_SILVER, Key="silver/breweries.parquet")
    df = pd.read_parquet(io.BytesIO(response['Body'].read()))

    df_gold = df.groupby(["brewery_type", "state"]).size().reset_index(name="brewery_count")

    buffer = io.BytesIO()
    df_gold.to_parquet(buffer, compression="snappy")
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, S3_BUCKET_GOLD, "gold/brewery_aggregates.parquet")

default_args = {
    'owner': 'gustavo',
    'start_date': datetime(2024, 3, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'gold_layer_brewery',
    default_args=default_args,
    description='Agrega os dados da Silver e armazena na Gold',
    schedule_interval='@daily',
    catchup=False,
)

sensor_silver = S3KeySensor(
    task_id='check_silver_file',
    bucket_key="silver/breweries.parquet",
    bucket_name=S3_BUCKET_SILVER,
    aws_conn_id='aws_default',
    poke_interval=30,
    timeout=300,
    mode='poke',
    dag=dag,
)

aggregate_gold_task = PythonOperator(
    task_id='aggregate_gold',
    python_callable=aggregate_gold,
    dag=dag,
)

sensor_silver >> aggregate_gold_task
