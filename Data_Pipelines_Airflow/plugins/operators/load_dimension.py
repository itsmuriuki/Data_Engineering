from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    ui_color = '#80BD9E'

    @apply_defaults
    def ___init__(self,
                #Define your operators params (with defaults) here
                # example: conn_id = your_connection_name
                *args, **kwargs):
        
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        #map params here 
        # example: self.conn_id= conn_id

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')