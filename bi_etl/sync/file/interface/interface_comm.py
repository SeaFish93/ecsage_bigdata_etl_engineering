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
import time
import json
import ast
from six import string_types
from six.moves.urllib.parse import urlencode, urlunparse
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_alert_info_d
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.get_account_tokens import get_oe_account_token
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread
hostname = socket.gethostname()

def build_url(path, query=""):
    scheme, netloc = "https", "ad.oceanengine.com"
    return urlunparse((scheme, netloc, path, "", query, ""))

#头条同步API
def set_sync_data(ParamJson="",UrlPath="",Token=""):
    """
    {"end_date": "",
     "page_size": "",
     "start_date": "",
     "advertiser_id": "",
     "group_by": "",
     "time_granularity": "",
     "page": ""
     "service_code":""
     }
    """
    query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in ParamJson.items()})
    url = build_url(UrlPath, query_string)
    headers = {
        "Access-Token": Token,
    }
    rsp = requests.get(url, headers=headers)
    return rsp.json()

def get_sync_data_return(ParamJson="",UrlPath=""):
    """
    {"end_date": "",
     "page_size": "",
     "start_date": "",
     "advertiser_id": "",
     "group_by": "",
     "time_granularity": "",
     "page": ""
     "service_code":""
     }
    """
    param_json = json.dumps(ParamJson)
    param_json = ast.literal_eval(json.loads(param_json))
    service_code = param_json["service_code"]
    token = get_oe_account_token(ServiceCode=service_code)
    del param_json["service_code"]
    data_list = set_sync_data(ParamJson=param_json,UrlPath=UrlPath,Token=token)
    return data_list["data"]["page_info"]["total_page"]

def get_sync_data(ParamJson="",UrlPath=""):
    """
    {"end_date": "",
     "page_size": "",
     "start_date": "",
     "advertiser_id": "",
     "group_by": "",
     "time_granularity": "",
     "page": ""
     "service_code":""
     }
    """
    param_json = json.dumps(ParamJson)
    param_json = ast.literal_eval(json.loads(param_json))
    service_code = param_json["service_code"]
    token = get_oe_account_token(ServiceCode=service_code)
    del param_json["service_code"]
    data_list = set_sync_data(ParamJson=param_json,UrlPath=UrlPath,Token=token)
    print(data_list)
    shell_cmd = """
      cat >> %s << endwritefilewwwww
%s
endwritefilewwwww""" % ("/home/ecsage_data/oceanengine/async/2/testtest.log" + ".%s" % (hostname),str(data_list).replace("""'""",""" " """))
    os.system(shell_cmd)

#多线程上传hdfs
def get_local_hdfs_thread(TargetDb="",TargetTable="",ExecDate="",DataFileList="",HDFSDir=""):
    th = []
    i = 0
    file_num = 0
    for data_files in DataFileList:
        etl_thread = EtlThread(thread_id=i, thread_name="%d" % (i),
                               my_run=local_hdfs_thread,DataFile=data_files, HDFSDir=HDFSDir
                               )
        etl_thread.start()
        th.append(etl_thread)
        i = i + 1
    for etl_th in th:
        etl_th.join()
    for data_files in DataFileList:
        status = os.system("""hadoop fs -ls %s/%s"""%(HDFSDir,data_files.split("/")[-1]))
        if int(status) == 0:
            file_num = file_num + 1
    if len(DataFileList) != file_num:
       msg = get_alert_info_d(DagId="airflow.dag", TaskId="airflow.task",
                              SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                              TargetTable="%s.%s" % (TargetDb, TargetTable),
                              BeginExecDate=ExecDate,
                              EndExecDate=ExecDate,
                              Status="Error",
                              Log="上传本地数据文件至HDFS出现异常！！！",
                              Developer="developer")
       set_exit(LevelStatu="red", MSG=msg)


#多线程上传hdfs
def local_hdfs_thread(DataFile="",HDFSDir="",arg=None):
    if arg is not None or len(arg) > 0:
       DataFile = arg["DataFile"]
       HDFSDir = arg["HDFSDir"]
       os.system("hadoop fs -put %s %s/" % (DataFile, HDFSDir))

#创建创意
#def
def set_oe_async_status_content_content(ExecData="",AsyncNotemptyFile="",AsyncEmptyFile="",ExecDate=""):
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
           os.system("""echo "%s %s %s %s %s %s %s">>%s """%(ExecDate,account_id, media_type,service_code, token, task_id,"有数",AsyncEmptyFile+".%s"%(hostname)))
       else:
           print("有数据：%s"%(account_id))
           os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (ExecDate,account_id, media_type,service_code, token, task_id,"有数", AsyncNotemptyFile+".%s"%(hostname)))
           os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (ExecDate,account_id, media_type,service_code, token, task_id,"有数", "/tmp/%s"%(AsyncNotemptyFile.split("/")[-1])))
    else:
       os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (ExecDate,account_id, media_type,service_code, token, task_id,"未执行完成", AsyncNotemptyFile+".%s"%(hostname)))
       os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (ExecDate,account_id, media_type,service_code, token, task_id,"未执行完成", "/tmp/%s"%(AsyncNotemptyFile.split("/")[-1])))

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
def get_oe_save_exception_file(ExceptionType="",ExecData="",AsyncNotemptyFile="",AsyncStatusExceptionFile="",ExecDate="",AirflowInstance=""):
    if ExceptionType !="create":
       get_data = ExecData
       media_type = get_data[1]
       service_code = get_data[2]
       account_id = get_data[0]
       task_id = get_data[4]
       token = get_data[3]
       task_name = get_data[5]
       if len(AsyncNotemptyFile) >0:
          os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (ExecDate,account_id, media_type, service_code, token, task_id, "999999", AsyncNotemptyFile + ".%s" % (hostname)))
       os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (account_id, media_type, service_code, token, task_id, "999999",AirflowInstance, AsyncStatusExceptionFile + ".%s" % (hostname)))
    else:
        account_id = ExecData[0]
        interface_flag = ExecData[1]
        media_type = ExecData[2]
        service_code = ExecData[3]
        token = ExecData[6]
        group_by = str(ExecData[4])
        fields = ExecData[5]
        #os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (media_type, token, service_code, account_id, 0, 999999, interface_flag, AsyncNotemptyFile))
        os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (account_id,interface_flag,media_type,service_code,group_by,
                                                   fields,token, AsyncStatusExceptionFile+".%s"%(hostname)))
def set_oe_async_tasks_data(DataFile="",ExecData="",AirflowInstance=""):
    get_data = ExecData
    media_type = get_data[1]
    service_code = get_data[2]
    account_id = get_data[0]
    task_id = get_data[4]
    token = get_data[3]
    task_name = get_data[5]
    resp_datas = ""
    n = 1
    set_run = True
    code = 0
    while set_run:
       code,resp_datas = get_oe_async_tasks_data(Token=token, AccountId=account_id, TaskId=task_id)
       if int(code) == 40105:
           token = get_oe_account_token(ServiceCode=service_code)
           if n >2:
             set_run = False
             os.system("""echo '%s'>>%s""" % (account_id, "/home/ecsage_data/oceanengine/async/%s/" % (media_type) + "token_exception_%s_%s" % (AirflowInstance,hostname)))
           else:
             time.sleep(2)
       else:
           os.system("""echo '%s %s'>>%s""" % (account_id,code, "/home/ecsage_data/oceanengine/async/%s/" % (media_type) + "account_sum_%s_%s" % (AirflowInstance,hostname)))
           if int(code) == 0:
             for data in resp_datas:
                 try:
                   os.system("""echo '%s'>>%s""" % (account_id, "/home/ecsage_data/oceanengine/async/%s/" % (media_type) + "test_%s_%s" % (AirflowInstance, hostname)))
                   shell_cmd = """
                   cat >> %s << endwritefilewwwww
%s
endwritefilewwwww"""%(DataFile+".%s"%(hostname),data.decode("utf8","ignore").replace("""`""","%%@@%%").replace("'","%%&&%%"))
                   #os.system("""echo '%s'>>%s""" % (account_id, "/home/ecsage_data/oceanengine/async/%s/" % (media_type) + "test_%s_%s" % (AirflowInstance, hostname)))
                   os.system(shell_cmd)
                 except Exception as e:
                   os.system("""echo '%s'>>%s""" % (account_id, "/home/ecsage_data/oceanengine/async/%s/"%(media_type) + "write_exception_%s_%s" % (AirflowInstance,hostname)))
           set_run = False
       n = n + 1
    return code

#获取oe异步任务数据
def get_oe_async_tasks_data(Token="",AccountId="",TaskId=""):
    resp_datas = ""
    open_api_domain = "https://ad.toutiao.com"
    path = "/open_api/2/async_task/download/"
    url = open_api_domain + path
    params = {
        "advertiser_id": AccountId,
        "task_id": TaskId
    }
    headers = {
        'Content-Type': "application/json",
        'Access-Token': Token,
        'Connection': "close"
    }
    return_resp_data = ""
    try:
      resp = requests.get(url, json=params, headers=headers)
      resp_data = resp.content
      return_resp_data = resp.iter_lines()
      code = eval(resp_data.decode())["code"]
    except Exception as e:
      code = 0
    return code,return_resp_data

#定义设置头条异步任务创建
def set_oe_async_tasks_create(AccountId="",AsyncTaskName="",Fields="",ExecDate="",Token="",GroupBy=""):
    open_api_domain = "https://ad.toutiao.com"
    path = "/open_api/2/async_task/create/"
    url = open_api_domain + path
    if Fields is None or len(Fields) == 0:
        params = {
            "advertiser_id": AccountId,
            "task_name": "%s" % (AsyncTaskName),
            "task_type": "REPORT",
            "force": "true",
            "task_params": {"start_date": ExecDate,
                            "end_date": ExecDate,
                            "group_by": GroupBy
                            }
        }
    else:
        params = {
            "advertiser_id": AccountId,
            "task_name": "%s" % (AsyncTaskName),
            "task_type": "REPORT",
            "force": "true",
            "task_params": {"start_date": ExecDate,
                            "end_date": ExecDate,
                            "group_by": GroupBy,
                            "fields": Fields
                            }
        }
    headers = {
        'Content-Type': "application/json",
        'Access-Token': Token,
        'Connection': "close"
    }
    resp = requests.post(url, json=params, headers=headers)
    resp_data = resp.json()
    return resp_data

#执行头条异步任务创建
def get_set_oe_async_tasks_create(InterfaceFlag="",MediaType="",ServiceCode="",AccountId="",AsyncTaskName="",AsyncTaskFile="",ExecDate="",GroupBy="",Fields="",Token=""):
    n = 1
    set_run = True
    token = Token
    resp_data = ""
    while set_run:
        resp_data = set_oe_async_tasks_create(AccountId=AccountId, AsyncTaskName=AsyncTaskName, Fields=Fields,
                                              ExecDate=ExecDate, Token=token, GroupBy=GroupBy)
        mess = str(resp_data).replace(" ","")
        code = resp_data["code"]
        if code == 40105 or code == 40104:
            token = get_oe_account_token(ServiceCode=ServiceCode)
            if n > 3:
              resp_data["data"]["task_name"] = mess
              resp_data["data"]["task_id"] = 40105
              set_run = False
        #没权限创建
        elif code == 40002:
            resp_data["data"]["task_name"] = mess
            resp_data["data"]["task_id"] = 40002
            set_run = False
        else:
            set_run = False
        n = n + 1
    task_id = resp_data["data"]["task_id"]
    task_name = resp_data["data"]["task_name"]
    async_task_file = """%s.%s"""%(AsyncTaskFile,hostname)
    """
     select a.account_id,'%s' as interface_flag,a.media_type,a.service_code,'%s' as group_by
                   ,'%s' as fields,a.token_data
    """
    os.system("""echo "%s %s %s %s %s %s %s %s %s">>%s """ % (AccountId,InterfaceFlag,MediaType,ServiceCode, "##","##",token, task_id, task_name, async_task_file))
    #os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (MediaType, token, ServiceCode, AccountId, task_id, task_name, InterfaceFlag, "/home/ecsage_data/oceanengine/async/ttttttt.%s"%(hostname)))
