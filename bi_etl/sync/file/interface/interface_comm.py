# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: interface_comm.py
# @Software: PyCharm
# function info：接口处理方法

import requests
import os
import datetime
import socket
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.get_account_tokens import get_oe_account_token
hostname = socket.gethostname()

def set_oe_async_status_content_content(ExecData="",AsyncNotemptyFile="",AsyncEmptyFile=""):
    get_data = ExecData
    media_type = get_data[1]
    service_code = get_data[2]
    account_id = get_data[0]
    task_id = get_data[4]
    token = get_data[3]
    task_name = get_data[5]

    os.system("""echo "%s %s %s">>/tmp/account_status.log """%(service_code,account_id,datetime.datetime.now()))
    resp_data = get_oe_tasks_status(AccountId=account_id, TaskId=task_id, Token=token)
    data = resp_data["code"]
    if data == 40105:
        token = get_oe_account_token(ServiceCode=service_code)
        resp_data = get_oe_tasks_status(AccountId=account_id, TaskId=task_id, Token=token)
    file_size = resp_data["data"]["list"][0]["file_size"]
    task_status = resp_data["data"]["list"][0]["task_status"]
    print("账户：%s，serviceCode：%s，文件大小：%s，任务状态：%s"%(account_id,service_code,file_size,task_status))
    if task_status == "ASYNC_TASK_STATUS_COMPLETED":
       if int(file_size) == 12:
           os.system("""echo "%s %s %s %s %s %s">>%s """%(account_id, media_type,service_code, token, task_id,"有数",AsyncEmptyFile+".%s"%(hostname)))
       else:
           print("有数据：%s"%(account_id))
           os.system("""echo "%s %s %s %s %s %s">>%s """ % (account_id, media_type,service_code, token, task_id,"有数", AsyncNotemptyFile+".%s"%(hostname)))
           os.system("""echo "%s %s %s %s %s %s">>%s """ % (account_id, media_type,service_code, token, task_id,"有数", "/tmp/%s"%(AsyncNotemptyFile.split("/")[-1])))
    else:
       os.system("""echo "%s %s %s %s %s %s">>%s """ % (account_id, media_type,service_code, token, task_id,"未执行完成", AsyncNotemptyFile+".%s"%(hostname)))
       os.system("""echo "%s %s %s %s %s %s">>%s """ % (account_id, media_type,service_code, token, task_id,"未执行完成", "/tmp/%s"%(AsyncNotemptyFile.split("/")[-1])))

#获取oe异步任务执行状态
def get_oe_tasks_status(AccountId="",TaskId="",Token=""):
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

#写入异常文件
def get_oe_save_exception_file(ExecData="",AsyncNotemptyFile="",AsyncStatusExceptionFile=""):
    get_data = ExecData
    media_type = get_data[1]
    service_code = get_data[2]
    account_id = get_data[0]
    task_id = get_data[4]
    token = get_data[3]
    task_name = get_data[5]
    os.system("""echo "%s %s %s %s %s %s">>%s """ % (account_id, media_type, service_code, token, task_id, "999999", "/tmp/%s" % (AsyncNotemptyFile.split("/")[-1])))
    os.system("""echo "%s %s %s %s %s %s">>%s """ % (account_id, media_type, service_code, token, task_id, "999999", AsyncNotemptyFile + ".%s" % (hostname)))
    os.system("""echo "%s %s %s %s %s %s">>%s """ % (account_id, media_type, service_code, token, task_id, "999999", AsyncStatusExceptionFile + ".%s" % (hostname)))
