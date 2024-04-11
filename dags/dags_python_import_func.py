from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp 
# airflow는 plugins까지 패스를 잡아주므로 plugins가 필요 없음
# .env 파일에서 WORKSPACE_FOLDER=C:/Projects/airflow 설정해주면 됨
# from plugins.common.common_func import get_sftp

with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    task_get_sftp = PythonOperator(
        task_id='task_get_sftp',
        python_callable=get_sftp # common 
    )