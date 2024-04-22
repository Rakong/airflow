from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
 
'''
    요청 url을 변수로 지정
    요청 파라미터
    app_key : 승인키 (필수)
    date_dt : 날짜구분코드(D:일변동, M:월변동) (필수)
    cntc_cd : 자료요청구분 (필수)
    retry_in : 재반영데이터포함여부(Y/N) (필수)
    req_dt : 요청일자 from (req_dt~ req_dt2 10일을 초과할 수 없음 ('err_code', 'E1004'))
    req_dt2 : 요청일자 to
'''
class JusoApiOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'base_dt', 'cntc_cd')

    def __init__(self, cntc_cd, path, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'api.juso.go.kr'
        self.db_conn_id = 'conn-db-postgres-custom'
        self.table_nm = ''
        self.path = path
        self.cntc_cd = cntc_cd
        self.base_dt = base_dt
        self.endpoint = 'updateInfo.do?app_key={{var.value.apikey_juso_go_kr}}&date_gb=D&retry_in=Y&cntc_cd=' + cntc_cd+'&req_dt='+base_dt

    def execute(self, context):
        import os 
        import pandas as pd 
        import zipfile 
        import urllib.request

        api_connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{api_connection.host}/{self.endpoint}'
        u = urllib.request.urlopen(self.base_url)
        while True:
            print(u.getheaders()) # header error code 있음
            # 파일 순번 존재확인
            file_seq = u.read(2)

            # 파일 순번 미존재시 종료(break)
            if not file_seq:
                break

            # 수신정보 저장 // strip() = 공백삭제, decode() = 바이트를 스트링으로 변환
            file_base_dt = u.read(8)
            file_name = u.read(50).strip().decode()
            file_size = int(u.read(10))
            res_code = u.read(5)
            req_code = u.read(6)
            replay = u.read(1)
            create_dt = u.read(8).decode()

            # 파일 다운로드 경로 지정
            outpath = self.path +"/"+ create_dt[2:8]+"/" 

            # 파일 다운로드 폴더 생성
            if not os.path.isdir(outpath):
                os.makedirs(outpath)
            
            # zip파일 데이터 읽기 & 쓰기
            zip_file = u.read(file_size+10)
            f = open(outpath+file_name, 'wb')
            f.write(zip_file)
            f.close()

            file_path = outpath + file_name
            print(file_path)

            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                update_pd = []
                delete_pd = []
                for file_info in zip_ref.infolist():
                    filenm = file_info.filename
                    # 수정건
                    update_pd.append(pd.read_csv(zip_ref.open(file_info), encoding='cp949', sep='|', header=None ))
                    
                    # 삭제건 (파일명으로 분기처리해야함)
            
            upt_data = pd.concat(update_pd)
        
            columnss_to_insert = ['area_cod', 'road_code', 'road_name', 'road_name_en', ]

            if upt_data.loc[0,0] != 'No Data':
                import psycopg2
                from airflow.providers.postgres.hooks.postgres import PostgresHook

                #     postgres_hook = PostgresHook(self.db_conn_id)
                #     with closing(postgres_hook.get_conn()) as conn:
                #         with closing(conn.cursor()) as cursor:
                #             conn.commit()
                            



                # db_connection = BaseHook.get_connection(self.db_conn_id)
                # self.host = db_connection.host
                # self.user = db_connection.login
                # self.passwork = db_connection.password
                # self.dbname = db_connection.schema
                # self.port = db_connection.port

                # 

            else:
                print("No Data")









        


