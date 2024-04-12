from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models import Variable
"""
    1. 전역변수 Variable 이해
        Xcom: 특정 DAG, 특정 schedule 에 수행되는 Task 간에만 공유
        모든 DAG이 공유할 수 있는 전역 변수는 없을까? =>  airflow 사이트에서 등록 
        (실제 Variable의 Key, Value 값은 메타 DB에 저장됨(variable 테이블))
    2. 전역변수 사용법
        1) 라이브러리 이용  : from airflow.models import Variable 
        2) jinja templates 이용 : {{var.value.sample_key}} >> 권고사항 (부하가 작음)
"""
with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    var_value = Variable.get("sample_key") # DB값을 추출하기 위해 DB접속하므로 부하가 커짐

    bash_var_1 = BashOperator(
    task_id="bash_var_1",
    bash_command=f"echo variable:{var_value}"
    )

    bash_var_2 = BashOperator(
    task_id="bash_var_2",
    bash_command="echo variable:{{var.value.sample_key}}"
    )