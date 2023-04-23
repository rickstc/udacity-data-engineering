from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
            self,
            append=False,
            table='',
            query='',
            connection_id='',
            *args,
            **kwargs
    ):
        """
        kwargs:
            - append (bool) - Whether to load the table using append or to
             delete and load the data. Default False.
            - table (string) - The name of the table to operate on
            - query (string) - The query to execute
            - connection_id (string) - The connection id
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.append = append
        self.table = table
        self.query = query
        self.conn_id = connection_id

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        if not self.append:
            # Remove the data - append is not true
            hook.run(f'DELETE FROM {self.table}')
        hook.run(self.sql_query)
