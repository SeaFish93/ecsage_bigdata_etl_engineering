from __future__ import absolute_import, unicode_literals
from tylerscope import app
import os
import requests
import time

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
            os.system("""echo "%s %s %s %s">>%s """ % (get_data[0], get_data[1], get_data[2], get_data[3], AsyncNotemptyFile))
            os.system("""echo "%s %s %s %s %s %s">>%s """ % (get_data[0], get_data[1], get_data[2], get_data[3], get_data[4], get_data[5],  AsyncStatusExceptionFile+"_last"))
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
           os.system("""echo "%s %s %s %s">>%s """ % (AccountId, MediaType,ServiceCode, Token, AsyncNotemptyFile))
    else:
       os.system("""echo "%s %s %s %s">>%s """ % (AccountId, MediaType,ServiceCode, Token, AsyncNotemptyFile))

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
            os.system("""echo "错误子账户token：%s">>/tmp/account_token_token.log """%(account_id))
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
