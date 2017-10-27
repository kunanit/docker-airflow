from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from custom_operators import PostgresToS3Transfer
from datetime import datetime, timedelta


default_args = {
    'owner': 'aang',
    'depends_on_past': False,
    'start_date': datetime(2017,10,26),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'adaptivity_db_to_s3',
    default_args=default_args,
    schedule_interval=timedelta(1),
)

"""
Copy database tables from adaptive-edx-green 
to 'latest' folder in vpal-data-adaptivity s3 bucket
"""

tables = [
    'engine_activity',
    'engine_collection',
    'engine_experimentalgroup',
    'engine_learner',
    'engine_score',
]

S3_BUCKET = 'vpal-data-adaptivity'

templated_command = "aws s3 cp {} {} --recursive".format(
    "s3://{{ params.bucket }}/latest",
    "s3://{{ params.bucket }}/{{ ds }}",
)

transfer_table_tasks = [PostgresToS3Transfer(
    task_id=table,
    postgres_conn_id='rds_adaptive_edx_green',
    table=table,
    s3_bucket=S3_BUCKET,
    s3_key='latest/{}.csv'.format(table),
    dag=dag,
) for table in tables]


"""
Copy 'latest' folder to date-labeled folder
"""
create_date_folder_task = BashOperator(
    task_id='date_versioned_copy',
    bash_command=templated_command,
    params=dict(bucket=S3_BUCKET),
    dag=dag,
)

"""
Only copy to date-versioned folder after rest of copy tasks have finished 
"""
create_date_folder_task.set_upstream(transfer_table_tasks)
