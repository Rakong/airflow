from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
from datetime import timedelta
from airflow.models import Variable
'''
    case 2
'''
email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_timeout_example_2',
    start_date=pendulum.datetime(2023, 5, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None,
    dagrun_timeout=timedelta(minutes=1),
    default_args={
        'execution_timeout': timedelta(seconds=40),
        'email_on_failure': True,
        'email': email_lst
    }
) as dag:
    bash_sleep_35 = BashOperator( # 정상
        task_id='bash_sleep_35',
        bash_command='sleep 35',
    )

    bash_sleep_36 = BashOperator( # 정상
        trigger_rule='all_done',
        task_id='bash_sleep_36',
        bash_command='sleep 36',
    )

    bash_go = BashOperator(  # 정상
        task_id='bash_go',
        bash_command='exit 0',
    )

    bash_sleep_35 >> bash_sleep_36 >> bash_go
    # 35+36 > 60 dagrun_timeout에 걸리므로 Dag자체는 fail 
    # bash_sleep_36 스킵 bash_go no status