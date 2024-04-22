import urllib.request
import os
import zipfile 

# 요청 url을 변수로 지정
'''
    요청 파라미터
    app_key : 승인키 (필수)
    date_dt : 날짜구분코드(D:일변동, M:월변동) (필수)
    cntc_cd : 자료요청구분 (필수)
    retry_in : 재반영데이터포함여부(Y/N) (필수)
    req_dt : 요청일자 from (req_dt~ req_dt2 10일을 초과할 수 없음 ('err_code', 'E1004'))
    req_dt2 : 요청일자 to
'''
# U01TX0FVVEgyMDI0MDQxOTE3MzAxOTExNDcwNjU=
#url ="http://update.juso.go.kr/updateInfo.do?app_key={}&date_gb=D&retry_in=Y&cntc_cd=100005"
url ="http://update.juso.go.kr/updateInfo.do?app_key=U01TX0FVVEgyMDI0MDQxOTE3MzAxOTExNDcwNjU=&date_gb=D&retry_in=Y&cntc_cd=100001&req_dt=20240422"
#url ="http://update.juso.go.kr/updateInfo.do?app_key={}&date_gb=D&retry_in=Y&cntc_cd=200001&req_dt=20240418&req_dt2=20240419"
u = urllib.request.urlopen(url)

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
    outpath = "C:/address_api/"+create_dt[2:8]+"/" 

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

    '''
        파일명 
        AlterD.JUSUZR.20240422.TI_SPRD_RDNM.TXT
        AlterD : 공간 및 속성정보데이터 일/월변동분 
        JUSUZR : 문서코드
        20240422 : 데이터가 추출되어 전송되는 일자
        TI_SPRD_RDNM : 데이터 추출의 기준이 되는 테이블명 
        Deletion : 삭제건 데이터
    '''
    import pandas as pd

    with zipfile.ZipFile(file_path, 'r') as zip_ref:
         # List all the files and directories in the zip file
        dfs = []
        print("Contents of the zip file:")
        for file_info in zip_ref.infolist():
            print(file_info.filename)
            dfs.append(pd.read_csv(zip_ref.open(file_info), encoding='cp949', sep='|', header=None ))

        if not dfs:
            print("No Data")
        else: 
            print("IS DATA")
            all_df=pd.concat(dfs)

            data_str = all_df.loc[0,0]
            if data_str =='No Data': 
                print("Daat 없어")
            else: 
               print("Daat 있어")