from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
"""
    ● Task 분기처리 방법
    BranchPythonOperator (주로사용)
    task.branch 데커레이터 이용 (주로사용)
    BaseBranchOperator 상속하여 직접 개발(choose_branch를 재정의)
"""
# BranchPythonOperator  사용 
"""
    skipmixin_key {'followed': ['task_a']}
"""
with DAG(
    dag_id='dags_branch_python_operator',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'), 
    schedule='0 1 * * *',
    catchup=False
) as dag:
    def select_random():
        import random

        item_lst = ['A','B','C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return 'task_a' # 하나일때
        elif selected_item in ['B','C']:
            return ['task_b','task_c'] # 두개 이상일때 배열
    # 주의사항 :  python_callable=select_random의 리턴값(후행 task_id 값을 줘야함)을 잘 작성해야함 
    python_branch_task = BranchPythonOperator(
        task_id='python_branch_task',
        python_callable=select_random
    )
    
    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    python_branch_task >> [task_a, task_b, task_c]  # [후보 task]
    # A => A
    # B OR C => B, C 