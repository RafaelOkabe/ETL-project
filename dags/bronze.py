import boto3
import requests
from botocore.client import Config

s3 = boto3.client('s3',
                  endpoint_url='http://minio:9000',
                  aws_access_key_id='minio',
                  aws_secret_access_key='minio123',
                  config=Config(signature_version='s3v4'),
                  region_name='us-east-1')

response = requests.get('https://api.openbrewerydb.org/breweries')
data = response.content

with open('/tmp/raw_data.json', 'wb') as f:
    f.write(data)

s3.upload_file('/tmp/raw_data.json', 'bronze', 'raw_data.json')
print("Bronze layer data uploaded.")
