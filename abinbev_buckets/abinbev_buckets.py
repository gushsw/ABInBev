import logging
import boto3 as b3
from botocore.exceptions import ClientError

def create_buckets(bucket_names, region_name='us-east-1'):
    s3_client = b3.client('s3', region_name=region_name)
    
    for bucket_name in bucket_names:
        try:
            if region_name == 'us-east-1':
                response = s3_client.create_bucket(Bucket=bucket_name)
            else:
                response = s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region_name}
                )
            logging.info(f'O Bucket {bucket_name} foi criado com sucesso na região {region_name}')
        except ClientError as e:
            logging.error(f'Erro ao criar o bucket {bucket_name}: {e}')
            continue

bucket_names = ['abinbev_bucket_gold', 'abinbev_bucket_silver', 'abinbev_bucket_bronze', 'abinbev_bucket_dados']
create_buckets(bucket_names)
