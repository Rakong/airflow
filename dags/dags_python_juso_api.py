from airflow import DAG
from operators.juso_api_operator import JusoApiOperator
from airflow.models import Variable
import pendulum

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_python_juso_api',
    schedule='0 8 * * *',
    start_date=pendulum.datetime(2024,4,1, tz='Asia/Seoul'),
    catchup=False,
    default_args={
        'email_on_failure': True,
        'email': email_lst
    }
) as dag: 
    task_juso_api = JusoApiOperator(
        task_id='task_juso_api',
        cntc_cd='100005',
        path='/opt/airflow/files/jusoapi',
        base_dt='{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}'
    )

    task_juso_api




