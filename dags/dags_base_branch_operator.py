from airflow import DAG
import pendulum
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import PythonOperator

# BaseBranchOperator
"""
    class CustomBranchOperator(BaseBranchOperator): 
    BaseBranchOperator 상속 시 choose_branch 함수를 구현해줘야함
    choose_branch는 반드시 task id를 리턴해야함
"""
with DAG(
    dag_id='dags_base_branch_operator',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    class CustomBranchOperator(BaseBranchOperator):
        def choose_branch(self, context): # 함수이름 변경하면 안됨, self, context 필요함(**kwargs)
            import random
            print(context)
            
            item_lst = ['A', 'B', 'C']
            selected_item = random.choice(item_lst)
            if selected_item == 'A':
                return 'task_a'
            elif selected_item in ['B','C']:
                return ['task_b','task_c']

    
    custom_branch_operator = CustomBranchOperator(task_id='python_branch_task')

    
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

    custom_branch_operator >> [task_a, task_b, task_c]