from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json 'auto'
    """

    @apply_defaults
    def __init__(self,
                 conn_id='redshift',
                 aws_creds='aws_credentials',
                 table_name='staging_events',
                 s3_bucket='',
                 s3_key='',
                 reload=False,
                 *args, **kwargs):
        """
        kwargs:
        - conn_id (string) - The postgres/redshift connection id
        - aws_creds (string) - The credential to use when instantiating the AWS Hook
        - table_name (string) - The name of the table
        - s3_bucket (string) - The s3 bucket where the staging data is stored
        - s3_key (string) - The path on the s3 bucket where the staging data is stored
        - reload (bool) - Whether to delete records before running the query
        """
        self.aws_hook = AwsHook(aws_creds)
        self.redshift_hook = PostgresHook(postgres_conn_id=conn_id)
        self.table = table_name
        self.bucket = s3_bucket
        self.key = s3_key
        self.reload = reload

    def has_data(self):
        """
        This checks whether the table has any records in it
        """
        first_record = self.redshift_hook.get_first(
            f'SELECT * FROM {self.table} LIMIT 1;')
        self.log.info(first_record)
        return len(first_record) > 0

    def execute(self, context):
        """
        This loads the data from S3 to Redshift, with some caveats
        If the 'reload' keyword argument is passed in, the data will be loaded
        regardless. However, if reload is false, and the tables already have data
        this process will be skipped.

        This speeds up the pipeline considerably while testing and debugging.
        """

        if not self.reload:
            self.log.info("Reload was false, checking if table has data")
            has_data = self.has_data()
            if has_data:
                self.log.info(
                    "Reload was false and the table already has data")
                return True

        self.log.info(f"Clearing data from Redshift table {self.table}")

        self.redshift_hook.run("DELETE FROM {}".format(self.table))

        credentials = self.aws_hook.get_credentials()

        formatted_sql = self.copy_sql.format(
            self.table,
            f"s3://{self.bucket}/{self.key}",
            credentials.access_key,
            credentials.secret_key,
        )
        self.log.info(f"The SQL Query to be run is: \n {formatted_sql}")
        self.redshift_hook.run(formatted_sql)
