from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'


    @apply_defaults
    def __init__(self, 
                #define your operator params(with defaults) here
                #example: redshift_conn_id = your-connection-name
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        #maps params here 
        #example: self.conn_id = conn_id

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')




        