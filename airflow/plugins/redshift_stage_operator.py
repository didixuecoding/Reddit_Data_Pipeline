from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Operator to transfer data from S3 to staging tables in a Redshift database.

    :param redshift_conn_id: Airflow connection id for Redshift connection secret
    :param aws_credentials_id: Airflow connection id for AWS connection secret
    :param table: Name of the table in Redshift that will be populated
    :param s3_src_bucket_name: Name of S3 source bucket, e.g. 'udacity-dend'
    :param s3_src_bucket_key: Key for files in the S3 source bucket. Templatable field.
        E.g. 'log_data/{execution_date.year}/{execution_date.month}/'
    :param data_format: Determines the format of the input data. Can be either 'csv' or 'json'.
    :param delimiter: CSV delimiter, will be ignored if `data_format` is not set to 'csv'
    :param jsonpaths: Defines how JSON objects are handled during import. More information see
        [here](https://docs.aws.amazon.com/en_us/redshift/latest/dg/copy-usage_notes-copy-from-json.html).
        Defaults to 'auto'.
    :param ignore_header: Specifies how many header lines should be ignored.
        [See documentation for
        details](https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-ignoreheader)
    """
    ui_color = '#c6bae8'
    template_fields = ("s3_src_bucket_key",)

    @apply_defaults
    def __init__(
            self,
            # Connections
            redshift_conn_id="",
            aws_credentials_id="",
            # Source / Target config
            table="",
            s3_src_bucket_name="",
            s3_src_bucket_key="",
            data_format="json",
            delimiter=",",
            jsonpath="auto",
            copy_opts="",
            ignore_header=0,
            *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_src_bucket_name = s3_src_bucket_name
        self.s3_src_bucket_key = s3_src_bucket_key
        self.data_format = data_format
        self.delimiter = delimiter
        self.jsonpath = jsonpath
        self.copy_opts = copy_opts
        self.ignore_header = ignore_header
        self._sql = """
            COPY {table:}
            FROM '{source:}'
            ACCESS_KEY_ID '{access_key:}'
            SECRET_ACCESS_KEY '{secret_access_key:}'
            {format:}
        """

    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift_conn = PostgresHook(
            postgres_conn_id=self.redshift_conn_id,
            connect_args={
                'keepalives': 1,
                'keepalives_idle': 60,
                'keepalives_interval': 60
            })

        self.log.debug(f"Truncate Table: {self.table}")
        redshift_conn.run(f"TRUNCATE TABLE {self.table}")

        format = ''
        if self.data_format == 'csv' and self.ignore_header > 0:
            format += f"IGNOREHEADER {self.ignore_header}\n"

        if self.data_format == 'csv':
            format += f"DELIMITER '{self.delimiter}'\n"
        elif self.data_format == 'json':
            format += f"FORMAT AS JSON '{self.jsonpath}'\n"
        format += f"{self.copy_opts}"
        self.log.debug(f"format : {format}")

        formatted_key = self.s3_src_bucket_key.format(**context)
        self.log.info(f"Rendered S3 source file key : {formatted_key}")
        s3_url = f"s3://{self.s3_src_bucket_name}/{formatted_key}"
        self.log.debug(f"S3 URL : {s3_url}")
        formatted_sql = self._sql.format(**dict(
            table=self.table,
            source=s3_url,
            access_key=aws_credentials.access_key,
            secret_access_key=aws_credentials.secret_key,
            format=format
        ))
        self.log.debug(f"Base SQL: {self._sql}")

        self.log.info(f"Copying data from S3 to Redshift table {self.table}...")
        redshift_conn.run(formatted_sql)
        self.log.info(f"Finished copying data from S3 to Redshift table {self.table}")
