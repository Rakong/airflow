from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

'''
    from airflow import Dataset
'''

dataset_dags_dataset_producer_1 = Dataset("dags_dataset_producer_1") # Dataset(큐 키값)

with DAG(
        dag_id='dags_dataset_producer_1',
        schedule='0 7 * * *',
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        catchup=False
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[dataset_dags_dataset_producer_1], # outlets[Dataset]
        bash_command='echo "producer_1 수행 완료"'
    )