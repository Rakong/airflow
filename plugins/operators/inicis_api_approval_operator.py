from airflow.models.baseoperator import BaseOperator

'''
승인대사 
'''

class InicisApprovalOperator(BaseOperator):
    
    template_fields = ('mid', 'passwd', 'base_dt')

    def __init__(self, url, postgres_conn_id, **kwargs):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.url = url
        self.mid = '{{ var.value.jibegaja_inicis_mid}}'
        self.passwd = '{{ var.value.jibegaja_inicis_passwd}}'
        self.base_dt = '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds_nodash}}'

    def execute(self, context):
        import requests

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Charset": "EUC-KR"
        }

        request_data = {
            "urlid" : self.mid,                  # 상점아이디
            "passwd": self.passwd,              # 승인대사 공인아이디 등록 시 발급된 비번
            "date"  : self.base_dt,              # 조회 기준
            "stime" : "000000",                  # 시작시간 고정
            "etime" : "235959",                  # 종료시간 고정
        }

        print("Request : ", request_data)

        response = requests.post(self.url, headers=headers, data=request_data)

        if response.status_code == 200:
            print("이니시스 승인대사 응답 성공")
            
            lines = response.text.split("<br>")
            data =[line.split("|") for line in lines if line.strip()]
            print("Response : ", data)

        else:
            print(f"=== Failed to connect 이니시스 승인대사, status code: {response.status_code} ===")
            print(response.text)
            raise Exception(f"Inicis approval API failed with status code : {response.status_code}")
