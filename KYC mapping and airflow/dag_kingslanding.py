"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/incubator-airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# schedule_interval = timedelta(seconds = 20)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(minutes = 10),#datetime(2018,12, 01),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds = 10), #timedelta(minutes=5),
    #'depends_on_past': True,
    #'wait_for_downstream' : True,
    #'max_threads': 1,
    # 'schedule_interval' : '@hourly',
    # 'schedule_interval': timedelta(seconds = 2), #'*/1 * * * *'#timedelta(seconds = 2)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('kingslanding_pipeline',
            default_args=default_args,
            schedule_interval= timedelta(seconds = 300),
            catchup=False
#'*/2 * * * *' #timedelta(minutes = 5) #'*/1 * * * *'#timedelta(seconds = 1)#'*/5 * * * *'
            )

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='referral_code',
    bash_command='python3 /repos/coinsquare_data_analytics/coinsquare-data-analytics/production/kingslanding_pipeline/referral_codes.py',
    dag=dag)

t2 = BashOperator(
    task_id='print_date',
    bash_command='python3 /home/peter/test/python_be_called.py ',
    dag=dag)



t3 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5 ',
    retries=3,
    dag=dag)

t4 = BashOperator(
    task_id='kingslanding_pipeline',
    bash_command='python3 /repos/coinsquare_data_analytics/coinsquare-data-analytics/production/kingslanding_pipeline/management_tool.py',
    dag=dag)

t1 >> t4
t4 >> t2
t4 >> t3
