from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 query='',
                 conn_id='',
                 table_name='',
                 reload=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        """
        kwargs:
        - query (string) - The query to execute
        - conn_id (string) - The connection id
        - table_name (string) - The name of the table
        - reload (bool) - Whether to delete records before running the query
        """
        self.query = query
        self.redshift_hook = PostgresHook(postgres_conn_id=conn_id)
        self.table_name = table_name
        self.reload = reload

    def execute(self, context):
        if self.reload is True:
            self.log.info(f"DELETING FROM TABLE: {self.table_name}")
            self.redshift_hook.run(f"DELETE FROM {self.table_name};")

        self.log.info(f"Loading the fact table {self.table_name}")

        self.redshift_hook.run(self.query)
