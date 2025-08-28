import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

today = datetime.now().strftime('%Y-%m-%d')
S3_BUCKET = 'aws-myairflow-bucket'
S3_PREFIX = 'transc_data/'

def load_csv_s3(local_folder):
    s3_hook = S3Hook(aws_conn_id='airflow_s3_conn')
    for file_name in os.listdir(local_folder):
        local_path = os.path.join(local_folder, file_name)
        s3_key = f"{S3_PREFIX}{today}/{file_name}"
        s3_hook.load_file(
            filename=local_path,
            key=s3_key,
            bucket_name=S3_BUCKET,
            replace=True
        )
