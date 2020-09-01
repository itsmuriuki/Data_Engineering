from datetime import datetime
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimesionOperator, DataQualityOperator)
from helpers import Queries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner':'itsmuriuki',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
    
}

dag = DAG('Airflow-project',
            default_args=default_args,
            description='Load and transform data in Redshift with Airflow'
            schedule_interval='0 * * * *',
            max_active_runs=3
            )

start_operator = DummyOperator(task_id ='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id ='Stage_events',
    dag=dag,
    provide_context =True,
    aws_credentials_id ="aws_credentials",
    redshift_conn_id = 'redshift',
    s3_bucket="airflow-test",
    s3_key="log_data",
    table = "staging_events",
    create_stmt =Queries.create_table_staging_events
)

stage_songs_to_redshift= StageToRedshiftOperator(
    task_id = "Stage_songs",
    dag =dag,
    provide_context = True,
    aws_credentials_id= "aws_credentials",
    redshift_conn_id = 'redshift',
    s3_bucket="airflow-test",
    s3_key ="song_data",
    table = "staging_songs",
    create_stmt = Queries.create_table_staging_songs

)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context =True,
    aws_credentials_id= "aws_cedentials",
    redshift_conn_id ="redshift",
    create_stmt =Queries.create_table_songplays
    sql_query =Queries.songplay_table_insert
)


load_user_dimension_table = LoadDimesionOperator(
    task_id ='Load_users_dim_table',
    dag=dag,
    provide_context =True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id= "redshift",
    create_stmt =Queries.create_table_users,
    sql_query=Queries.user_table_insert
)

load_song_dimension_table =LoadDimesionOperator(
    task_id ='Load_song_dim_table',
    dag=dag,
    provide_context =True,
    aws_credentials_id='aws_credentials',
    redshift_conn_id="redshift",
    create_stmt=Queries.create_table_songs,
    sql_query=Queries.song_table_insert


)

load_artist_dimension_table= LoadDimesionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    provide_context =True,
    aws_credentials_id= 'aws_credentials',
    redshift_conn_id='redshift',
    create_stmt=Queries.create_table_artist,
    sql_query=Queries.artist_table_insert
)


load_time_dimension_table=LoadDimesionOperator(
    task_id ='Load_time_dim_table',
    dag=dag,
    provide_context= True,
    aws_credentials_id ='aws_crdentials',
    redshift_conn_id= 'redshift',
    create_stmt= Queries.create_table_time,
    sql_query=Queries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift'
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >>[stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_song_dimension_table,load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_song_dimension_table,load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]>>run_quality_checks
run_quality_checks >> end_operator