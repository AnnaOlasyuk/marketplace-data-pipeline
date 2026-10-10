import boto3
import os
from datetime import datetime

# Подключение к локальному S3 (moto)
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

BUCKET_NAME = 'marketplace-raw-data'

def create_bucket():
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"ℹ️ Bucket '{BUCKET_NAME}' уже существует")
    except Exception:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print(f"✅ Bucket '{BUCKET_NAME}' создан")

def upload_file(file_path, key):
    s3_client.upload_file(file_path, BUCKET_NAME, key)
    print(f"✅ Файл загружен: s3://{BUCKET_NAME}/{key}")

if __name__ == '__main__':
    create_bucket()
    
    # Локальный путь к данным (не /app/data, это для Docker)
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    today = datetime.now().strftime('%Y%m%d')
    
    for file in os.listdir(data_dir):
        if file.endswith('.csv'):
            upload_file(
                os.path.join(data_dir, file),
                f'raw/{today}/{file}'
            )
    
    print("🎉 Извлечение данных завершено!")