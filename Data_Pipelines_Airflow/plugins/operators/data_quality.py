from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    ui_color= '#89DA59'

    @apply_defaults
    def __init__(self, 
                # Define your operators params 
                # example: conn_id= your-connection-id
                *args, **kwargs):

            super(DataQualityOperator,self).__init__(*args, **kwargs)
            # map params here 
            # example: conn_id=conn_id

    def execute(self, contex):
        self.log.info("DataQualityOperator not implemented yet ")




        