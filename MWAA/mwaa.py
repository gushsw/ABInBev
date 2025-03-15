import boto3

# Cria um cliente MWAA
client = boto3.client('mwaa')

# Parâmetros para o ambiente MWAA
environment_name = 'abinbev_mwaa'
region_name = 'us-east-1'
dag_s3_path = 's3://abinbev_bucket_dados/Engenharia/ApacheAirflow/Dags'
execution_role_arn = 'arn:aws:iam::123456789012:role/abinbevMWAARole' #Trocar pela arn da role que que executará o MWAA
source_bucket_arn = 'arn:aws:s3:::abinbev_bucket_dados/Engenharia/ApacheAirflow'
kms_key = 'arn:aws:kms:us-east-1:123456789012:key/abcd1234-abcd-1234-abcd-1234abcd1234'
subnet_ids = ['subnet-0123456789abcdef0']
security_group_ids = ['sg-0123456789abcdef0']

# Cria o ambiente MWAA
response = client.create_environment(
    Name=environment_name,
    DagS3Path=dag_s3_path,
    ExecutionRoleArn=execution_role_arn,
    SourceBucketArn=source_bucket_arn,
    KmsKey=kms_key,
    NetworkConfiguration={
        'SubnetIds': subnet_ids,
        'SecurityGroupIds': security_group_ids
    },
    AirflowVersion='2.5.1', 
    EnvironmentClass='mw1.medium',  # Escolha a classe do ambiente
    MaxWorkers=10,  # Número máximo de workers
    MinWorkers=1,  # Número mínimo de workers
    Schedulers=2,  # Número de schedulers
    WebserverAccessMode='PRIVATE_ONLY',
    WeeklyMaintenanceWindowStart='TUE:09:00'
)
