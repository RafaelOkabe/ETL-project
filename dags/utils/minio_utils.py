import boto3
from botocore.client import Config

def get_minio_client(endpoint='minio:9000', access_key='minio', secret_key='minio123'):
    return boto3.client(
        's3',
        endpoint_url=f'http://{endpoint}',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def upload_file_to_minio(minio_client, bucket_name, object_name, file_path):
    with open(file_path, 'rb') as f:
        minio_client.upload_fileobj(f, bucket_name, object_name)
    print(f"Arquivo {file_path} enviado para o bucket {bucket_name} como {object_name}")
