from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from s3_upload_operator import S3UploadOperator

BASH_SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
DATASET_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'input'))
S3_BUCKET_NAME = "reddit-landing-didixuecoding"

default_args = dict(
    owner='didixuecoding',
    start_date=datetime(2021, 5, 1),
    end_date=datetime(2021, 5, 31),
    depends_on_past=False
)

dag = DAG(
    'reddit',
    default_args=default_args,
    description='Load reddit data into S3 and Redshift',
    schedule_interval='@monthly'
)

start_task = DummyOperator(dag=dag, task_id='start_execution')

download_dataset_task = BashOperator(
    dag=dag,
    task_id='daily_download_dataset',
    bash_command=BASH_SCRIPT_DIR + '/download_datasets.sh {{ds}}'
)


preporcess_authors_task = BashOperator(
    dag=dag,
    task_id='preprocess_authors',
    bash_command= f"zstd -cdq {DATASET_DIR}/Authors/RA_78M.csv.zst | {BASH_SCRIPT_DIR}/preprocess_authors.py | zstd -zqfo {DATASET_DIR}/Authors/RA_78M_processed.csv.zst",
)

upload_s3_task = S3UploadOperator(
    dag=dag,
    task_id='upload_s3_data',
    execution_timeout=timedelta(hours=1),
    aws_credentials_id='aws_credentials',
    dataset_dir=DATASET_DIR,
    file_glob="test/*",
    bucket_name=S3_BUCKET_NAME,
)

end_operator = DummyOperator(dag=dag, task_id='end_execution')

start_task >> download_dataset_task >> end_operator
