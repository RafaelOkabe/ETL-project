import os
import requests
import zipfile

from utils.minio_utils import get_minio_client, upload_file_to_minio

def main():
    # Definindo variaveis
    download_url = "https://www.kaggle.com/api/v1/datasets/download/atharvasoundankar/taco-sales-dataset-20242025"
    download_dir = "/tmp/taco_dataset"
    bucket = "bronze"

    # Fazendo o Download do arquivo
    os.makedirs(download_dir, exist_ok=True)
    zip_path = os.path.join(download_dir, "taco-sales-dataset-20242025.zip")
    
    print("Baixando o arquivo...")
    response = requests.get(download_url, allow_redirects=True)
    response.raise_for_status()

    with open(zip_path, 'wb') as f:
        f.write(response.content)
    
    print(f"Arquivo salvo em: {zip_path}")

    # Extraindo arquivo do zip
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(download_dir)

    print(f"Arquivos extraídos para: {download_dir}")
    
    minio_client = get_minio_client()

    for f in os.listdir(download_dir):
        if f.endswith('.csv'):
            local_file = os.path.join(download_dir, f)
            upload_file_to_minio(minio_client, bucket, f, local_file)

if __name__ == "__main__":
    main()
