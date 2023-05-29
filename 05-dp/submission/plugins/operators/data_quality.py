from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id='',
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:

            self.log.info(
                f"Checking for records in the '{table}' table...")
            first_record = redshift_hook.get_first(
                f"SELECT * FROM {table} LIMIT 1;")

            if len(first_record) == 0:
                self.log.error(
                    f"Validation failed for '{table}' table.")
                raise ValueError(
                    f"Validation failed for '{table}' table.")
            self.log.info(
                f"Validation passed for '{table}' table.")
