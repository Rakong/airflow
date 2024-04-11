from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator
"""
    bash_command (str)
    env (dict[str, str] | None)
"""
with DAG(
    dag_id="dags_bash_with_template",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_t1 = BashOperator( 
        task_id='bash_t1',
        bash_command='echo "data_interval_end: {{ data_interval_end }}  "'
    )
    # {{ data_interval_end }} 템플릿 변수 type pendulum.DateTime

    bash_t2 = BashOperator(
        task_id='bash_t2',
        env={
            'START_DATE':'{{data_interval_start | ds }}',
            'END_DATE':'{{data_interval_end | ds }}'
        },
        bash_command='echo $START_DATE && echo $END_DATE'
    )
    # {{data_interval_start | ds }} 대쉬가 붙은 형태로 출력하기 위해 | ds 추가 
    # 'echo $START_DATE && echo $END_DATE' && 쉘스크립트 작성법 

    bash_t1 >> bash_t2