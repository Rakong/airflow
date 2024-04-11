from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
import random

# def : 파이썬 함수 정의 ex) def 함수명(입력변수):
# python_collable : task 내에서 함수 호출 
# import random , randon.randint(0,3) 0~3 중에 랜던 값을 리턴함 
with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def select_fruit():
        fruit = ['APPLE','BANANA','ORANGE','AVOCADO']
        rand_int = random.randint(0,3)
        print(fruit[rand_int]) # fruit에서 rand_int에 해당하는 값을 출력함 

    py_t1 = PythonOperator( # task 정의 
        task_id='py_t1',
        python_callable=select_fruit # 함수 실행 
    )

    py_t1