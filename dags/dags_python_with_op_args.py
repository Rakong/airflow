from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import regist
# PythonOperator > op-arguments : op-args = [변수]
# 함수에 일반 변수만 있을 경우 / 일반변수 + *args / *args만 있는경우 

with DAG(
    dag_id="dags_python_with_op_args",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    regist_t1 = PythonOperator(
        task_id='regist_t1',
        python_callable=regist,
        op_args=['hjkim','man','kr','seoul'] # 일반 변수 + *args => 튜플로 받아짐
    )

    regist_t1