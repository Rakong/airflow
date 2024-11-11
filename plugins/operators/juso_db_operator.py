import zipfile
import pandas as pd
import os
import pyproj

path = 'C:/address_api/240426/'

files = os.listdir(path) # 파일리스트 

print(files)

df009007_colnm_kr = ['주소관할읍면동코드', '시도명', '시군구명','읍면동명', '도로명코드', '도로명','지하여부', '건물본번', '건물부번'
                    ,'우편번호', '건물관리번호', '시군구용건물명', '건축물용도분류', '행정동코드','행정동명', '지상층수', '지하층수', '공동주택구분'
                    ,'건물수', '상세건물명', '건물명변경이력', '거주여부', '건물중심점x좌표', '건물중심점y좌표', '출입구x좌표', '출입구y좌표'
                    ,'시도명(영문)', '시군구명(영문)', '읍면동명(영문)', '도로명(영문)', '읍면동구분', '이동사유코드']

df200001_colnm_kr = ['도로명관리번호', '법정동코드', '시도명', '시군구명','읍면동명', '리명', '도로명코드', '도로명', '지하여부', '건물본번', '건물부번', '기초구역번호(우편번호)', '효력발생일', '이동사유코드', '출입구일련번호', '출입구구분','출입구유형','출입구좌표x','출입구좌표y']

df009007 = pd.DataFrame()
df100001 = pd.DataFrame(columns = ['도로명관리번호', '법정동코드', '시도명', '시군구명','읍면동명', '리명', '산여부', '번지', '호', '도로명코드', '도로명', '지하여부', '건물본번', '건물부번', '행정동코드', '행정동명','기초구역번호(우편번호)','이전도로명주소','효력발생일','공동주택구분','이동사유코드','건축물대장건물명','시군구용건물명','비고'])
df200001 = pd.DataFrame()

for file in files:
    file_path = os.path.join(path, file)
    print(file_path)

    if(file.endswith('MATCH_ENTRC_BULD.TXT')): # 009007
        f = open(file_path, 'r', encoding='euc-kr')
        df009007 = pd.read_csv(f, sep='|', names=df009007_colnm_kr)
        f.close()
    elif(file.endswith('TH_SGCO_RNADR_POSITION.TXT')): # 200001
        f = open(file_path, 'r', encoding='euc-kr')
        df200001 = pd.read_csv(f, sep='|', names=df200001_colnm_kr)
        print(df100001)
        f.close()
    elif(file.endswith('TH_SGCO_RNADR_MST.TXT')):
        f = open(file_path, 'r', encoding='cp949')
        lines = f.readlines()
        for line in lines:
            line = line.strip()  # 줄 끝의 줄 바꿈 문자를 제거한다.
            lineLst = line.split('|')

            # df에 추가
            df100001.loc[len(df100001)] = lineLst
        #df100001 = pd.read_csv(f, sep='|')
        print(df100001)
        f.close()


# df009007.drop(columns=['건물관리번호',  '행정동코드', '지상층수', '지하층수','공동주택구분','상세건물명', '건물명변경이력', '거주여부', '건물중심점x좌표', '건물중심점y좌표'
                    #    ,'시도명(영문)', '시군구명(영문)', '읍면동명(영문)', '도로명(영문)', '읍면동구분'], inplace=True)
df009007_column_name_mapping = {    
    '주소관할읍면동코드' : 'law_code',
    '시도명' : 'large_name',
    '시군구명' : 'middle_name',
    '읍면동명' : 'small_name',
    '도로명코드' : 'road_code',
    '도로명' : 'road_name',
    '지하여부' : 'underground_yn',
    '건물본번' : 'main_bld_no',
    '건물부번' : 'sub_bld_no',
    '우편번호' : 'zip_code',
    '시군구용건물명' : 'bld_name',
    '건축물용도분류' : 'bld_usage',
    '행정동명' : 'admin_name',
    '건물수' : 'bld_cnt',
    '출입구x좌표' : 'utmk_x',
    '출입구y좌표' : 'utmk_y',
    '이동사유코드' : 'move_reason_code',
}
# df009007.rename(columns=df009007_column_name_mapping, inplace=True)



# df200001.drop(columns=['도로명관리번호', '시도명', '시군구명','읍면동명', '리명', '도로명', '효력발생일', '출입구구분','출입구유형','출입구좌표x','출입구좌표y'],inplace=True)

# print(df009007)
#print(df200001)
# 법정동코드, 도로명코드, 지하여부, 건물본번, 건물부번

# 009007 : 주소관할읍코드 = 법정동코드로 보기

# 200001 출입구일련번호 => 신규 업데이트만 