from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)

    python_task_1 = print_context('task_decorator 실행')
# 함수를 감싼다!!! 
# 파이썬은 함수 안에 함수를 선언하는 것이 가능
# 함수의 인자로 함수를 전달하는 것이 가능
# 함수 자체를 리턴하는 것이 가능 

# [START howto_operator_python]
# @task(task_id="print_the_context")
# def print_context(ds=None, **kwargs):
#     """Print the Airflow context and ds variable from the context."""
#     pprint(kwargs)
#     print(ds)
#     return "Whatever you return gets printed in the logs"

# run_this = print_context()
# [END howto_operator_python]