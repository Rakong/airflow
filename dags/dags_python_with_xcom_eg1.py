from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task
"""
    Xcome(Cross Communication)
    Airflow Dag 안 task간 데이터 공유를 위해 사용되는 기술 
    특징 : 주로 작은 규모의 데이터 공유를 위해 사용(Xcome 내용은 메타 DB의 xcome 테이블에 값이 저장 됨)

    사용방법
    1) **kwargs에 존재하는 it(task_instance) 객체 활용
    2) 파이썬 함수의 return 값 활용
"""
with DAG(
    dag_id="dags_python_with_xcom_eg1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id='python_xcom_push_task1')
    def xcom_push1(**kwargs):
        ti = kwargs['ti'] # kwargs ti 객체 가져오기
        ti.xcom_push(key="result1", value="value_1") # xcome에 데이타 넣기 k,v 형태
        ti.xcom_push(key="result2", value=[1,2,3])

    @task(task_id='python_xcom_push_task2')
    def xcom_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key="result1", value="value_2")
        ti.xcom_push(key="result2", value=[1,2,3,4])

    @task(task_id='python_xcom_pull_task')
    def xcom_pull(**kwargs):
        ti = kwargs['ti'] # kwargs ti 객체 가져오기
        value1 = ti.xcom_pull(key="result1") # xcome에서 데이터 가져오기 
        value2 = ti.xcom_pull(key="result2", task_ids='python_xcom_push_task1') # task 1의 result2 값을 가져오겠다.
        print(value1)
        print(value2)


    xcom_push1() >> xcom_push2() >> xcom_pull()
    #결과값 ? 
    #print(value1) = value_2 마지막값
    #print(value2) = [1,2,3] task_ids에 해당하는 값