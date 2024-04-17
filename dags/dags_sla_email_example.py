from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum
from airflow.models import Variable

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]
'''
    sla_miss_callback: SLA Miss시 수행할 함수 지정 가능(DAG의 파라미터임)
'''
with DAG(
    dag_id='dags_sla_email_example',
    start_date=pendulum.datetime(2023, 5, 1, tz='Asia/Seoul'),
    schedule='*/10 * * * *',
    catchup=False,
    default_args={
        'sla': timedelta(seconds=70), # BaseOperator의 파라미터
        'email': email_lst
    }
) as dag:
    
    task_slp_30s_sla_70s = BashOperator(
        task_id='task_slp_30s_sla_70s',
        bash_command='sleep 30'
    )
    
    task_slp_60_sla_70s = BashOperator(
        task_id='task_slp_60_sla_70s',
        bash_command='sleep 60'
    )

    task_slp_10s_sla_70s = BashOperator(
        task_id='task_slp_10s_sla_70s',
        bash_command='sleep 10'
    )

    task_slp_10s_sla_30s = BashOperator(
        task_id='task_slp_10s_sla_30s',
        bash_command='sleep 10',
        sla=timedelta(seconds=30) # 우선순위가 더 높음
    )

    task_slp_30s_sla_70s >> task_slp_60_sla_70s >> task_slp_10s_sla_70s >> task_slp_10s_sla_30s