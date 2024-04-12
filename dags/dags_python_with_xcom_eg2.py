from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_xcom_eg2",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success' # xcome에 알아서 올라감 key="return_value", task_id="python_xcom_push_by_return"


    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_ids='python_xcom_push_by_return') # 리턴값을 활용하는2안 value1 = "Success"
        print('xcom_pull 메서드로 직접 찾은 리턴 값:' + value1)

    @task(task_id='python_xcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print('함수 입력값으로 받은 값:' + status) # xcom_push_result >> xcom_pull_2 이므로 status = "Success"


    python_xcom_push_by_return = xcom_push_result()
    xcom_pull_2(python_xcom_push_by_return)  # Task 데커레이터 사용시 함수 입력/출력 관계만으로  task flow 정의가 됨
    python_xcom_push_by_return >> xcom_pull_1()

    # xcom_push_result() >> [xcom_pull_2(), xcom_pull_1()]