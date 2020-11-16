from __future__ import absolute_import, unicode_literals
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tylerscope import app
import os
import requests
import time
import socket
import datetime

hostname = socket.gethostname()
@app.task
def run_task_exception(AsyncNotemptyFile="",AsyncEmptyFile="",AsyncStatusExceptionFile="",ExecData=""):
    get_data = ExecData
    media_type = get_data[1]
    service_code = get_data[2]
    account_id = get_data[0]
    task_id = get_data[4]
    token = get_data[3]
    task_name = get_data[5]
    set_true = True
    n = 1
    print("执行子账户：%s"%(account_id))
    while set_true:
      try:
         set_async_status_content_content(MediaType=media_type,ServiceCode=service_code,AccountId=account_id,TaskId=task_id,Token=token,AsyncNotemptyFile=AsyncNotemptyFile,AsyncEmptyFile=AsyncEmptyFile)
         set_true = False
      except Exception as e:
         if task_id == 0:
            n = 4
         if n > 3:
            print("异常子账户：%s" % (account_id))
            os.system("""echo "%s %s %s %s %s %s">>%s """ % (account_id, media_type, service_code, token, task_id, "999999", "/tmp/%s"%(AsyncNotemptyFile.split("/")[-1])))
            os.system("""echo "%s %s %s %s %s %s">>%s """ % (account_id, media_type, service_code, token, task_id, "999999", AsyncNotemptyFile+".%s"%(hostname)))
            os.system("""echo "%s %s %s %s %s %s">>%s """ % (account_id, media_type, service_code, token, task_id, "999999", AsyncStatusExceptionFile+".%s"%(hostname)))
            set_true = False
         else:
          time.sleep(2)
      n = n + 1

def set_async_status_content_content(MediaType="",ServiceCode="",AccountId="",TaskId="",Token="",AsyncNotemptyFile="",AsyncEmptyFile=""):
    os.system("""echo "%s %s %s">>/tmp/account_status.log """%(ServiceCode,AccountId,datetime.datetime.now()))
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
           os.system("""echo "%s %s %s">>%s """%(AccountId,TaskId,Token,AsyncEmptyFile+".%s"%(hostname)))
       else:
           print("有数据：%s"%(AccountId))
           os.system("""echo "%s %s %s %s %s %s">>%s """ % (AccountId, MediaType,ServiceCode, Token, TaskId,"有数", AsyncNotemptyFile+".%s"%(hostname)))
           os.system("""echo "%s %s %s %s %s %s">>%s """ % (AccountId, MediaType,ServiceCode, Token, TaskId,"有数", "/tmp/%s"%(AsyncNotemptyFile.split("/")[-1])))
    else:
       os.system("""echo "%s %s %s %s %s %s">>%s """ % (AccountId, MediaType,ServiceCode, Token, TaskId,"未执行完成", AsyncNotemptyFile+".%s"%(hostname)))
       os.system("""echo "%s %s %s %s %s %s">>%s """ % (AccountId, MediaType,ServiceCode, Token, TaskId,"未执行完成", "/tmp/%s"%(AsyncNotemptyFile.split("/")[-1])))

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
            os.system("""echo "错误子账户token：%s %s">>/tmp/account_token_token.log """%(ServiceCode,datetime.datetime.now()))
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
