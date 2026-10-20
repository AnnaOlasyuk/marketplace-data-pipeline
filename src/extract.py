import os
from dotenv import load_dotenv  # ← ДОБАВИТЬ
load_dotenv()                    # ← ДОБАВИТЬ

import boto3

s3_client = boto3.client(
    's3',
    endpoint_url=os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
    region_name='us-east-1'
)