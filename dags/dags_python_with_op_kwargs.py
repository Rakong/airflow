from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import regist2
# PythonOperator > op_kwargs : op_kwargs = { 키 : 변수, ...} 딕셔너리 


with DAG(
    dag_id="dags_python_with_op_kwargs",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    regist2_t1 = PythonOperator(
        task_id='regist2_t1',
        python_callable=regist2,
        op_args=['hjkim','man','kr','seoul'],
        op_kwargs={'email':'hjkim_sun@naver.com','phone':'010'}
    )

    regist2_t1