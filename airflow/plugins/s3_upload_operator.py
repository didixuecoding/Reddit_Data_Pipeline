from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pathlib import Path


class S3UploadOperator(BaseOperator):
    """
    Filter and upload content of a folder and upload matching files to AWS S3.
    :param aws_credentials_id: Airflow connection id for AWS connection secret
    :param dataset_dir: Path to directory that contains the files
    :param file_glob: Glob to match filenames
    :param bucket_name: AWS S3 destination bucket name
    """

    @apply_defaults
    def __init__(
        self,
        aws_credentials_id='',
        dataset_dir='',
        bucket_name='',
        file_glob="",
        *args, **kwargs
    ):
        super(S3UploadOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.dataset_dir = dataset_dir
        self.bucket_name = bucket_name
        self.file_glob = file_glob


    def execute(self, context):
        hook = S3Hook(self.aws_credentials_id)

        pathlist = list(Path(self.dataset_dir).glob(self.file_glob))
        for path in pathlist:
            bucket_key = str(path)[len(self.dataset_dir) + 1:]
            if hook.check_for_key(key=bucket_key, bucket_name=self.bucket_name):
                self.log.info(f"File '{bucket_key}' is already present as s3://{self.bucket_name}/{bucket_key}. Skip upload.")
            else:
                self.log.info(f"Upload file '{bucket_key}' to s3://{self.bucket_name}/{bucket_key}. This might take a while.")
                hook.load_file(filename=str(path), key=bucket_key, bucket_name=self.bucket_name)