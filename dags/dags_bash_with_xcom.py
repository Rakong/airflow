from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator
"""
    bash_command (str)
    env (dict[str, str] | None)
"""
with DAG(
    dag_id="dags_bash_with_xcom",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_push = BashOperator(
    task_id='bash_push',
    bash_command="echo START && "
                 "echo XCOM_PUSHED "
                 "{{ ti.xcom_push(key='bash_pushed',value='first_bash_message') }} && "
                 "echo COMPLETE" # <= 마지막 출력문은 자동으로 return_value에 저장 됨 (COMPLETE) 
    )

    bash_pull = BashOperator(
        task_id='bash_pull',
        env={'PUSHED_VALUE':"{{ ti.xcom_pull(key='bash_pushed') }}", # first_bash_message
            'RETURN_VALUE':"{{ ti.xcom_pull(task_ids='bash_push') }}"}, #task_ids만 지정하면 key return_vlaue를 의미함 (COMPLETE)
        bash_command="echo $PUSHED_VALUE && echo $RETURN_VALUE ",
        do_xcom_push=False # bash_command 마지막 값을 xcom에 넣지 말아라 
    )

    bash_push >> bash_pull