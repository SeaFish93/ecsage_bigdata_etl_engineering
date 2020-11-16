from __future__ import absolute_import, unicode_literals
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tylerscope import app
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
import os
import requests
import time

#etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
@app.task
def add(x,y):
    return x+y

@app.task
def multiply(x,y):
    return x*y

@app.task
def signup():
    print('i am here now.')

@app.task
def run_task_exception(AsyncNotemptyFile="",AsyncEmptyFile="",AsyncNotSuccFile="",AsyncStatusExceptionFile="",ExecData=""):
    get_data = ExecData
    set_true = True
    n = 1
    while set_true:
      try:
         set_async_status_content_content(MediaType=get_data[1],ServiceCode=get_data[2],AccountId=get_data[0],TaskId=get_data[4],Token=get_data[3],AsyncNotemptyFile=AsyncNotemptyFile,AsyncEmptyFile=AsyncEmptyFile,AsyncNotSuccFile=AsyncNotSuccFile)
         set_true = False
      except Exception as e:
         print("!!!!!!!!!!!!!%s %s"%(AsyncNotemptyFile,get_data[0]))
         if get_data[4] == 0:
            n = 4
         if n > 3:
            os.system("""echo "%s %s %s %s">>%s """ % (get_data[0], get_data[1], get_data[2], get_data[3], "/tmp/AsyncNotemptyFile.log.log"))
            os.system("""echo "%s %s %s %s %s %s">>%s """ % (get_data[0], get_data[1], get_data[2], get_data[3], get_data[4], get_data[5], AsyncNotemptyFile))
            os.system("""echo "%s %s %s %s %s %s">>%s """ % (get_data[0], get_data[1], get_data[2], get_data[3], get_data[4], get_data[5],  AsyncStatusExceptionFile+"_last"))
            insert_sql = """
               insert into metadb.oe_valid_account_interface_bak
               (account_id,media_type,service_code,token_data)
               select '%s',%s,'%s','%s'
            """ % (get_data[0], get_data[1], get_data[2], get_data[3])
            #ok = etl_md.execute_sql(sql=insert_sql)

            set_true = False
         else:
          time.sleep(2)
      n = n + 1

def set_async_status_content_content(MediaType="",ServiceCode="",AccountId="",TaskId="",Token="",AsyncNotemptyFile="",AsyncEmptyFile="",AsyncNotSuccFile=""):
    os.system("""echo "%s %s">>/tmp/account_status.log """%(ServiceCode,AccountId))
    resp_data = get_tasks_status(AccountId=AccountId, TaskId=TaskId, Token=Token)
    data = resp_data["code"]
    if data == 40105:
        token = get_account_token(ServiceCode=ServiceCode)
        resp_data = get_tasks_status(AccountId=AccountId, TaskId=TaskId, Token=token)
    file_size = resp_data["data"]["list"][0]["file_size"]
    task_status = resp_data["data"]["list"][0]["task_status"]
    print("账户：%s，serviceCode：%s，文件大小：%s，任务状态：%s"%(AccountId,ServiceCode,file_size,task_status))
    if task_status == "ASYNC_TASK_STATUS_COMPLETED":
       if int(file_size) == 12:
           os.system("""echo "%s %s %s">>%s """%(AccountId,TaskId,Token,AsyncEmptyFile))
       else:
           print("有数据：%s"%(AccountId))
           insert_sql = """
               insert into metadb.oe_valid_account_interface_bak
               (account_id,media_type,service_code,token_data)
               select '%s',%s,'%s','%s'
            """ % (AccountId, MediaType,ServiceCode, Token)
           #ok = etl_md.execute_sql(sql=insert_sql)
           os.system("""echo "%s %s %s %s %s %s">>%s """ % (AccountId, MediaType,ServiceCode, Token, TaskId,"", AsyncNotemptyFile))
           os.system("""echo "%s %s %s %s">>%s """ % (AccountId, MediaType,ServiceCode, Token, "/tmp/AsyncNotemptyFile.log.log"))
    else:
       insert_sql = """
               insert into metadb.oe_valid_account_interface_bak
               (account_id,media_type,service_code,token_data)
               select '%s',%s,'%s','%s'
            """ % (AccountId, MediaType,ServiceCode, Token)
       #ok = etl_md.execute_sql(sql=insert_sql)
       os.system("""echo "%s %s %s %s %s %s">>%s """ % (AccountId, MediaType,ServiceCode, Token, TaskId,"", AsyncNotemptyFile))
       os.system("""echo "%s %s %s %s">>%s """ % (AccountId, MediaType,ServiceCode, Token, "/tmp/AsyncNotemptyFile.log.log"))

def get_account_token(ServiceCode=""):
    headers = {'Content-Type': "application/json", "Connection": "close"}
    token_url = """http://token.ecsage.net/service-media-token/rest/getToken?code=%s""" % (ServiceCode)
    set_true = True
    n = 1
    token_data = None
    while set_true:
      try:
        token_data_list = requests.post(token_url,headers=headers).json()
        token_data = token_data_list["t"]["token"]
        set_true = False
      except Exception as e:
        if n > 3:
            os.system("""echo "错误子账户token：%s">>/tmp/account_token_token.log """%(ServiceCode))
            set_true = False
        else:
            time.sleep(2)
      n = n + 1
    return token_data

def get_tasks_status(AccountId="",TaskId="",Token=""):
    open_api_domain = "https://ad.toutiao.com"
    path = "/open_api/2/async_task/get/"
    url = open_api_domain + path
    params = {
        "advertiser_id": AccountId,
        "filtering": {
            "task_ids": [TaskId]
        }
    }
    headers = {
        'Content-Type': "application/json",
        'Access-Token': Token,
        'Connection': "close"
    }
    resp = requests.get(url, json=params, headers=headers,timeout = 20)
    resp_data = resp.json()
    return resp_data
