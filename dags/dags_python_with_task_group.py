from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
"""
    Task들의 모음.
    UI Graph탭에서 Task들을 Group화하여 보여줌
    Task Group 안에 Task Group을 중첩하여 구성 가능

    Task Group 작성 방법은 2가지가 존재함.
    (데커레이터 & 클래스)
    Task Group 안에 Task Group 중첩하여 정의 가능
    Task Group 간에도 Flow 정의 가능
    Group이 다르면 task_id가 같아도 무방
    Tooltip 파라미터를 이용해 UI 화면에서 Task group에 대한 설명 제공 가능
    (데커레이터 활용시 docstring으로도 가능)
"""
with DAG(
    dag_id="dags_python_with_task_group",
    schedule=None,
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def inner_func(**kwargs):
        msg = kwargs.get('msg') or '' 
        print(msg)

    # 데커레이터를 이용한 방법 from airflow.decorators import task_group
    @task_group(group_id='first_group')
    def group_1():
        ''' task_group 데커레이터를 이용한 첫 번째 그룹입니다. ''' # ''' '''docstring 함수에 대한 설명 

        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('첫 번째 TaskGroup 내 첫 번째 task입니다.')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg':'첫 번째 TaskGroup내 두 번쨰 task입니다.'}
        )

        inner_func1() >> inner_function2

    # 클래스를 이용한 방법 from airflow.utils.task_group import TaskGroup
    # with문 docstring 대신 tooltip을 사용
    with TaskGroup(group_id='second_group', tooltip='두 번째 그룹입니다') as group_2:
        ''' 여기에 적은 docstring은 표시되지 않습니다'''
        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('두 번째 TaskGroup 내 첫 번째 task입니다.')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg': '두 번째 TaskGroup내 두 번째 task입니다.'}
        )
        inner_func1() >> inner_function2
    
    # group이 다르므로 task_id가 같아도 됨
    group_1() >> group_2 # inner_func1() >> inner_function2 >>  inner_func1() >> inner_function2