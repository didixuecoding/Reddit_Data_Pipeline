from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadRedshiftTableOperator(BaseOperator):
    """
    Load data into DWH table with a customizable SQL query.
    :param redshift_conn_id: Airflow connection id for Redshift connection secret
    :param table: Name of the table in Redshift that will be populated
    :param sql_stmt: Select query to retrieve rows that will be used for populating the target table.
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id="",
            table="",
            sql_stmt="",
            update_mode="append",
            *args, **kwargs):
        super(LoadRedshiftTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.update_mode = update_mode.lower()

    def execute(self, context):
        redshift_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Connection to Redshift has been successfully created")
        sql_query = ""
        if self.update_mode == 'overwrite':
            sql_query += f"TRUNCATE TABLE '{self.table}';\n"
        sql_query += self.sql_stmt.format(**dict(
            table=self.table,
        ))

        self.log.info(f"Loading Redshift Table : {self.table}")
        self.log.debug(f"Formatted query: {sql_query}")
        redshift_conn.run(sql_query)
        self.log.info(f"Finished loading Redshift Table {self.table}")
