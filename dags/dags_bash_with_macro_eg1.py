from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

"""
from datetime import datetime
from dateutil import relativedelta

now = datetime(year=2023, month=3, day=30)
print('현재시간 :' + str(now))
print('---------------------- 월 연산 ------------------')
print(now + relativedelta.relativedelta(month=1))
print(now.replace(month=1))
print(now + relativedelta.relativedelta(months=-1))
print('---------------------- 일 연산 ------------------')
print(now + relativedelta.relativedelta(day=1))
print(now.replace(day=1))
print(now + relativedelta.relativedelta(days=-1))
print('---------------------- 연산 여러개 ------------------')
print(now + relativedelta.relativedelta(months=-1) + relativedelta.relativedelta(days=-1) ) # 1개월, 1일 빼기
"""
# macros.dateutil.relativedelta.relativedelta
with DAG(
    dag_id="dags_bash_with_macro_eg1",
    schedule="10 0 L * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # START_DATE: 전월 말일, END_DATE: 1일 전
    # 타임존 맞춰줘야함 
    bash_task_1 = BashOperator(
        task_id='bash_task_1',
        env={'START_DATE':'{{ data_interval_start.in_timezone("Asia/Seoul") | ds }}',
             'END_DATE':'{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}'
        },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )