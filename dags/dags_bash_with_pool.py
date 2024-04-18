from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
'''
    task pool
    pool : 수행할 pool
    pool_slots : 차지할 slot 수 
    priority_weight : task 가중치(클수록 경합시 유리)
    weight_rule : 추가 가중치 합산방식(upstream / downstream / absolute)
'''
with DAG(
    dag_id="dags_bash_with_pool",
    schedule="10 0 * * 6",
    start_date=pendulum.datetime(2023, 5, 1, tz="Asia/Seoul"),
    catchup=False,
    default_args={
        'pool':'pool_small'
    }
) as dag:
    bash_task_1 = BashOperator(
        task_id='bash_task_1',
        bash_command='sleep 30',
        priority_weight=6
    )

    bash_task_2 = BashOperator(
        task_id='bash_task_2',
        bash_command='sleep 30',
        priority_weight=5
    )

    bash_task_3 = BashOperator(
        task_id='bash_task_3',
        bash_command='sleep 30',
        priority_weight=4
    )

    bash_task_4 = BashOperator(
        task_id='bash_task_4',
        bash_command='sleep 30'
    )

    bash_task_5 = BashOperator(
        task_id='bash_task_5',
        bash_command='sleep 30'
    )

    bash_task_6 = BashOperator(
        task_id='bash_task_6',
        bash_command='sleep 30'
    )

    bash_task_7 = BashOperator(
        task_id='bash_task_7',
        bash_command='sleep 30',
        priority_weight=7
    ) 

    bash_task_8 = BashOperator(
        task_id='bash_task_8',
        bash_command='sleep 30',
        priority_weight=8
    ) 

    bash_task_9 = BashOperator(
        task_id='bash_task_9',
        bash_command='sleep 30',
        priority_weight=9
    ) 