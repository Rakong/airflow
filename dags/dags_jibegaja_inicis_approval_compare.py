from airflow import DAG
from operators.inicis_api_approval_operator import InicisApprovalOperator
import pendulum


with DAG(
    dag_id='dags_jibegaja_inicis_apporval_compare',
    schedule='0 9 * * *',
    start_date=pendulum.datetime(2024,4,22, tz='Asia/Seoul'),
    catchup=False,
    tags=['이니시스', '승인대사']
) as dag:
    
    approval_task = InicisApprovalOperator(
        task_id='inicis_approval_task',
        url='https://iniweb.inicis.com/service/urlsvc/UrlSendAll.jsp',
        postgres_conn_id='my_postgres_connection'
    )