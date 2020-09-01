from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, 
                    # Define your operators params (with defaults) here
                    # Examples: conn_id = you-connection-name 
                    *args, **kwargs):

            super(LoadFactOperator, self).__init__(*args, **kwargs)
            # map params here 
            # example: self.conn_id =conn_id

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')



