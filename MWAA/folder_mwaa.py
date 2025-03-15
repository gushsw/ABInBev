import boto3 as b3

s3 = b3.client('s3')

bucket_names = ["abinbev-bucket-dados"]
directory_names = ["Engenharia"]
subfolders_names = ["ApacheAirflow/Dags"]

for bucket_name in bucket_names:
    for directory_name in directory_names:
        for subfolder_name in subfolders_names:
            # Adiciona a barra '/' no final do nome do diretório
            key = f"{directory_name}/{subfolder_name}/"
            s3.put_object(Bucket=bucket_name, Key=key)
