from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd 
"""

"""
class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path','file_name','base_dt')
    # init 재정의
    '''
        task_id => **kwargs로 감
    '''
    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        self.base_dt = base_dt
    
    def execute(self, context):
        import os
        
        connection = BaseHook.get_connection(self.http_conn_id) #  BaseHook.get_connection => connection 객체가 얻어짐
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame() # 비어있는 데이터 프레임
        start_row = 1
        end_row = 1000
        while True:
            self.log.info(f'시작:{start_row}')
            self.log.info(f'끝:{end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row)
            total_row_df = pd.concat([total_row_df, row_df]) # pd.concat([]) 데이터 프레임으로 합침
            if len(row_df) < 1000:
                break
            else:
                start_row = end_row + 1
                end_row += 1000

        if not os.path.exists(self.path): # path가 없다면 생성
            os.system(f'mkdir -p {self.path}')
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)

    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json 

        headers = {'Content-Type': 'application/json',
                   'charset': 'utf-8',
                   'Accept': '*/*'
                   }

        request_url = f'{base_url}/{start_row}/{end_row}/'
        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
        response = requests.get(request_url, headers) # requests : hppt get 요청을 하는 라이브러리
        contents = json.loads(response.text) # response : 반환값(딕셔너리형태의 스트링)  json.loads로 딕셔너리로 변환

        key_nm = list(contents.keys())[0] # stationInfo의 value값
        row_data = contents.get(key_nm).get('row') # 딕션너리의 list 형태 
        row_df = pd.DataFrame(row_data) # DataFrame 판다스 구조로 변환

        return row_df