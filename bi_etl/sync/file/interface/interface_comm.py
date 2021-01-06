# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: interface_comm.py
# @Software: PyCharm
# function info：接口处理方法

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import os
import datetime
import socket
import time
import json
import ast
from six import string_types
import urllib3
from six.moves.urllib.parse import urlencode, urlunparse
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_alert_info_d
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.get_account_tokens import get_oe_account_token
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.set_Logger import LogManager
from ecsage_bigdata_etl_engineering.common.base.def_table_struct import def_ods_structure
from ecsage_bigdata_etl_engineering.common.base.def_table_struct import adj_snap_structure

hostname = socket.gethostname()

def build_url(path="", netloc="",query=""):
    scheme= "https"
    return urlunparse((scheme, netloc, path, "", query, ""))
#头条同步API
def set_sync_data(ParamJson="",UrlPath="",netloc="ad.oceanengine.com",Token="",IsPost="N"):
    query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in ParamJson.items()})
    url = build_url(UrlPath,netloc, query_string)
    headers = {
        "Access-Token": Token,
        'Connection': "close",
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
    }
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    s = requests.session()
    retries = Retry(total=5,backoff_factor=0.1,status_forcelist=[500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    s.keep_alive = False
    if IsPost == "Y":
        rsp = s.post(url=url, headers=headers, verify=False, stream=False, timeout=300)
    else:
       rsp = s.get(url=url, headers=headers, verify=False, stream=False, timeout=300)
    return rsp.json()

def get_sync_data_return(ParamJson="",UrlPath="",PageTaskFile="",DataFileDir="",DataFile="",TaskFlag=""):
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
    advertiser_id = param_json["advertiser_id"]
    token = get_oe_account_token(ServiceCode=service_code)
    page = 0
    remark = ""
    page_task_file = "%s.%s"%(PageTaskFile,hostname)
    del param_json["service_code"]
    data_list = ""
    try:
      data_list = set_sync_data(ParamJson=param_json,UrlPath=UrlPath,Token=token)
      if "page_info" in data_list["data"]:
         data_list["returns_account_id"] = advertiser_id
         test_log = LogManager("""%s-%s""" % (DataFile.split(".")[0],hostname)).get_logger_and_add_handlers(2,log_path=DataFileDir,log_filename="""%s-%s.%s""" % (DataFile.split(".")[0],hostname,DataFile.split(".")[1]))
         test_log.info(json.dumps(data_list))
         page = data_list["data"]["page_info"]["total_page"]
         remark = "正常"
         if page == 0:
            data = str(data_list).replace(" ","")
         else:
            data = ""
      else:
         #没权限及token失败
         if int(data_list["code"]) in [40002,40105,40104]:
             remark = "正常"
             data = str(data_list).replace(" ","")
         else:
             print("没有页数：%s,%s,%s,%s"%(service_code,advertiser_id,data_list,param_json["filtering"]["campaign_ids"]))
             remark = "异常"
             data = str(data_list).replace(" ","")
    except:
      print("请求失败：%s,%s,%s" % (service_code, advertiser_id, param_json["filtering"]["campaign_ids"]))
      remark = "异常"
      data = "请求失败：%s,%s,%s" % (service_code, advertiser_id, param_json["filtering"]["campaign_ids"])
    os.system("""echo "%s %s %s %s %s %s %s">>%s.%s""" % (page,advertiser_id, service_code,remark,data,param_json["filtering"]["campaign_ids"],TaskFlag, page_task_file,hostname))
    return remark

def get_sync_data(ParamJson="",UrlPath="",DataFileDir="",DataFile=""):
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
    advertiser_id = param_json["advertiser_id"]
    service_code = param_json["service_code"]
    token = get_oe_account_token(ServiceCode=service_code)
    page = 0
    del param_json["service_code"]
    try:
      data_list = set_sync_data(ParamJson=param_json,UrlPath=UrlPath,Token=token)
      #for get_data in data_list["data"]["list"]:
      #    get_data["returns_account_id"] = advertiser_id
      #    shell = """
#cat >> %s << endwritefilewwwww
#%s
#endwritefilewwwww"""%("""/home/ecsage_data/oceanengine/async/2/data"""+".%s"%(hostname),str(get_data).replace("""`""","%%@@%%"))
#          os.system(shell)
      ######log = Logger(filename="/home/ecsage_data/oceanengine/async/2/sync_data_file.log.%s" % (hostname))
      ######log.logger.info(data_list)
      ######log.logger.removeHandler(log.rotateHandler)
      if "page_info" in data_list["data"]:
         data_list["returns_account_id"] = advertiser_id
         test_log = LogManager("""%s-%s""" % (DataFile.split(".")[0], hostname)).get_logger_and_add_handlers(2,log_path=DataFileDir,log_filename="""%s-%s.%s""" % (DataFile.split(".")[0],hostname,DataFile.split(".")[1]))

         #test_log = LogManager("""sync_data_file.%s""" % (hostname)).get_logger_and_add_handlers(2,log_path='/home/ecsage_data/oceanengine/async/2',
         #                                                                                         log_filename="""sync_data_file.%s.log""" % (hostname))
         test_log.info(json.dumps(data_list))
         page = data_list["data"]["page_info"]["total_page"]
         remark = "正常"
         data = data_list
      else:
         #没权限及token失败
         if int(data_list["code"])in [40002,40105,40104]:
             remark = "正常"
             data = data_list
         else:
             print("获取数据异常：%s,%s,%s,%s"%(service_code,advertiser_id,data_list,param_json["filtering"]["campaign_ids"]))
             remark = "数据异常"
             data = data_list
    except:
      print("请求数据失败：%s,%s,%s" % (service_code, advertiser_id, param_json["filtering"]["campaign_ids"]))
      remark = "数据失败"
      data = "数据失败"
    return remark,page

#多线程上传hdfs
def get_local_hdfs_thread(TargetDb="",TargetTable="",ExecDate="",DataFileList="",HDFSDir="",EtlMdSession=""):
    th = []
    i = 0
    th_n = 0
    file_num = 0
    for data_files in DataFileList:
        etl_thread = EtlThread(thread_id=i, thread_name="%d" % (i),
                               my_run=local_hdfs_thread,DataFile=data_files, HDFSDir=HDFSDir
                               )
        etl_thread.start()
        th.append(etl_thread)
        if th_n >=1 or len(DataFileList)-1 == i:
           for etl_th in th:
              etl_th.join()
           th = []
           th_n = -1
        th_n = th_n + 1
        i = i + 1

    size_error_file = DataFileList[0].rsplit("/", 1)[0] + '/' + 'file_size_error.log'
    os.system(""" > %s"""%(size_error_file))
    for data_files in DataFileList:
        file_size = os.path.getsize(data_files)
        if int(file_size) == 0:
            print("【%s】文件大小异常，请注意" % (data_files))
            data_files_list=data_files.rsplit("/",1)
            os.system("""echo "%s %s %s" >> %s""" % (data_files_list[0],data_files_list[1],int(file_size),size_error_file))

        status = os.system("""hadoop fs -ls %s/%s"""%(HDFSDir,data_files.split("/")[-1]))
        if int(status) == 0:
            file_num = file_num + 1

    error_file_size = os.path.getsize(size_error_file)
    if int(error_file_size) > 0 and EtlMdSession is not None :
        insert_sql = """
            load data local infile '%s' into table metadb.monitor_collect_file_log fields terminated by ' ' lines terminated by '\\n' (target_file_dir,target_file,target_file_size)
        """ % (size_error_file)
        print(insert_sql)
        if len(EtlMdSession) > 0:#故意错误代码
            print("利用报错使其重跑！！！并记录到Mysql！")
       #etl_md.local_file_to_mysql(sql=insert_sql)

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
           os.system("""echo "%s %s %s %s %s %s %s">>%s """%(ExecDate,account_id, media_type,service_code, token, task_id,"无数",AsyncEmptyFile+".%s"%(hostname)))
       else:
           print("有数据：%s"%(account_id))
           status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (ExecDate,account_id, media_type,service_code, token, task_id,"有数", AsyncNotemptyFile+".%s"%(hostname)))
           if int(status) != 0:
               a = 1/0
    else:
       status_1 = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (ExecDate,account_id, media_type,service_code, token, task_id,"未执行完成", AsyncNotemptyFile+".%s"%(hostname)))
       if int(status_1) != 0:
           a = 1 / 0

#新版异步任务状态
def set_oe_status_async_tasks(ExecDate="",DataFileDir="",DataFile="",UrlPath="",ParamJson="",Token="",ReturnAccountId="",ServiceCode="",MediaType="",TaskFlag=""):
    code = 1
    data = ""
    try:
        resp_data = set_sync_data(ParamJson=ParamJson, UrlPath=UrlPath, netloc="ad.toutiao.com", Token=Token,IsPost="N")
        code = resp_data["code"]
        # token无效重试
        if int(code) == 40105:
            token = get_oe_account_token(ServiceCode=ServiceCode)
            resp_data = set_sync_data(ParamJson=ParamJson, UrlPath=UrlPath, netloc="ad.toutiao.com", Token=token,IsPost="N")
            code = resp_data["code"]
        request_id = resp_data["request_id"]
        if int(code) == 0:
            file_size = resp_data["data"]["list"][0]["file_size"]
            task_status = resp_data["data"]["list"][0]["task_status"]
            if task_status == "ASYNC_TASK_STATUS_COMPLETED":
                remark = "正常"
                if int(file_size) > 12:
                    print("有数据：%s %s" % (ReturnAccountId,ServiceCode))
                    task_id = resp_data["data"]["list"][0]["task_id"]
                    resp_data = """%s %s %s %s %s %s %s %s %s""" % (ExecDate, ReturnAccountId, MediaType, ServiceCode, Token, task_id, "有数",TaskFlag,request_id)
                    remark, data = get_write_local_file(RequestsData=resp_data, RequestID=request_id,DataFileDir=DataFileDir,DataFile=DataFile)
            else:
                print("媒体异步任务未执行完成：%s %s" % (ReturnAccountId,ServiceCode))
                task_id = resp_data["data"]["list"][0]["task_id"]
                resp_data = """%s %s %s %s %s %s %s %s %s""" % (ExecDate, ReturnAccountId, MediaType, ServiceCode, Token, task_id, "未执行完成", TaskFlag, request_id)
                remark, data = get_write_local_file(RequestsData=resp_data, RequestID=request_id,DataFileDir=DataFileDir, DataFile=DataFile)
            if remark != "正常":
               code = 1
        elif int(code) in [40002, 40105, 40104]:
            code = 0
            os.system(""" echo "%s %s">>%s/%s """%(ReturnAccountId,str(resp_data).replace(" ", ""),DataFileDir,"account_perssion.log"))
        else:
            code = 1
            data = str(resp_data).replace(" ", "")
    except Exception as e:
        code = 1
        data = "请求失败：%s" % (str(e).replace("\n", "").replace(" ", "").replace("""\"""", ""))
    if int(code) != 0:
        status = os.system(""" echo "%s %s %s %s %s %s">>%s/%s.%s """ % (time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()), ReturnAccountId, ServiceCode,
        str(ParamJson).replace(" ", ""), data, Token, DataFileDir, "account_status.log", hostname))
        if int(status) != 0:
            for i in range(10):
                status = os.system(""" echo "%s %s %s %s %s %s">>%s/%s.%s """ % (time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()), ReturnAccountId, ServiceCode,
                str(ParamJson).replace(" ", ""), data, Token, DataFileDir, "account_status.log", hostname))
                if int(status) == 0:
                    break;
    return code

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
    elif ExceptionType =="create":
        account_id = ExecData[0]
        interface_flag = ExecData[1]
        media_type = ExecData[2]
        service_code = ExecData[3]
        token = ExecData[6]
        group_by = str(ExecData[4])
        fields = ExecData[5]
        os.system("""echo "%s %s %s %s %s %s %s %s %s">>%s """ % (account_id,interface_flag,media_type,service_code, "##","##",token, 0, 999999, AsyncNotemptyFile+".%s"%(hostname)))
        os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (account_id,interface_flag,media_type,service_code,group_by,fields,token, AsyncStatusExceptionFile+".%s"%(hostname)))
    else:
        account_id = ExecData[0]
        interface_flag = ExecData[1]
        media_type = ExecData[2]
        service_code = ExecData[3]
        token = ExecData[6]
        group_by = str(ExecData[4])
        fields = ExecData[5]
        os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (account_id, interface_flag, media_type, service_code, group_by, fields, token,AsyncStatusExceptionFile + ".%s" % (hostname)))

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
def get_set_oe_async_tasks_create(InterfaceFlag="",MediaType="",ServiceCode="",AccountId="",AsyncTaskName="",AsyncTaskFile="",ExecDate="",GroupBy="",Fields="",LocalDir=""):
    mess = ""
    try:
        token = get_oe_account_token(ServiceCode=ServiceCode)
        resp_data = set_oe_async_tasks_create(AccountId=AccountId, AsyncTaskName=AsyncTaskName, Fields=Fields,
                                              ExecDate=ExecDate, Token=token, GroupBy=GroupBy)
        mess = str(resp_data).replace(" ","")
        code = resp_data["code"]
        if int(code) == 0:
            task_id = resp_data["data"]["task_id"]
            task_name = resp_data["data"]["task_name"]
            async_task_file = """%s.%s""" % (AsyncTaskFile, hostname)
            status = os.system("""echo "%s %s %s %s %s %s %s %s %s">>%s """ % (AccountId, InterfaceFlag, MediaType, ServiceCode, "##", "##", token, task_id, task_name, async_task_file))
            if int(status) != 0:
               print("写入失败：%s"%(AccountId))
               code = 1
               mess = "写入失败"
        #没权限创建
        elif int(code) in [40002, 40105, 40104]:
            code = 0
        else:
            code = 1
    except Exception as e:
        code = 1
        mess = "请求失败"
    if int(code) != 0:
       os.system("""echo "%s %s">>%s/%s.%s """ % (AccountId,mess,LocalDir,InterfaceFlag,hostname))
    return code

#定义设置头条异步任务创建
def set_oe_create_async_tasks(DataFileDir="",DataFile="",UrlPath="",ParamJson="",Token="",ReturnAccountId="",ServiceCode="",InterfaceFlag="",MediaType="",TaskFlag=""):
    code = 1
    data = ""
    try:
        resp_data = set_sync_data(ParamJson=ParamJson, UrlPath=UrlPath, netloc="ad.toutiao.com", Token=Token,IsPost="Y")
        code = resp_data["code"]
        # token无效重试
        if int(code) == 40105:
            token = get_oe_account_token(ServiceCode=ServiceCode)
            resp_data = set_sync_data(ParamJson=ParamJson, UrlPath=UrlPath, netloc="ad.toutiao.com", Token=token,IsPost="Y")
            code = resp_data["code"]
        request_id = resp_data["request_id"]
        if int(code) == 0:
            task_id = resp_data["data"]["task_id"]
            task_name = resp_data["data"]["task_name"]
            resp_data = """%s %s %s %s %s %s %s %s %s"""%(ReturnAccountId, InterfaceFlag, MediaType, ServiceCode, Token, task_id,task_name,str(resp_data).replace(" ",""),TaskFlag)
            remark, data = get_write_local_file(RequestsData=resp_data, RequestID=request_id, DataFileDir=DataFileDir,DataFile=DataFile)
            if remark != "正常":
                code = 1
        elif int(code) in [40002, 40105, 40104]:
            code = 0
            task_id = "111111"
            task_name = "无权限"
            resp_data = """%s %s %s %s %s %s %s %s %s""" % (ReturnAccountId, InterfaceFlag, MediaType, ServiceCode, Token, task_id, task_name,str(resp_data).replace(" ", ""),TaskFlag)
            remark, data = get_write_local_file(RequestsData=resp_data, RequestID=request_id, DataFileDir=DataFileDir,DataFile=DataFile)
            if remark != "正常":
                code = 1
        else:
            code = 1
            data = str(resp_data).replace(" ", "")
    except Exception as e:
        code = 1
        data = "请求失败：%s" % (str(e).replace("\n", "").replace(" ", "").replace("""\"""", ""))
    if int(code) != 0:
        status = os.system(""" echo "%s %s %s %s %s %s">>%s/%s.%s """ % (time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()), ReturnAccountId, ServiceCode,str(ParamJson).replace(" ", ""), data, Token, DataFileDir, "account_status.log", hostname))
        if int(status) != 0:
            for i in range(10):
                status = os.system(""" echo "%s %s %s %s %s %s">>%s/%s.%s """ % (time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()), ReturnAccountId, ServiceCode,str(ParamJson).replace(" ", ""), data, Token, DataFileDir, "account_status.log", hostname))
                if int(status) == 0:
                    break;
    return code

def set_oe_async_tasks_data_return(DataFileDir="",DataFile="",UrlPath="",ParamJson="",Token="",ReturnAccountId="",ServiceCode=""):
    code = 1
    data = ""
    try:
        resp_datas = get_oe_async_tasks_data_return(UrlPath=UrlPath, ParamJson=ParamJson, Token=Token)
        try:
          #异步返回数据若是json格式，则是异常
          code = eval(resp_datas.decode())["code"]
          data = str(resp_datas.decode()).replace(" ","").replace("'","")
        except:
          code = 0
        # token无效重试
        if int(code) == 40105:
            token = get_oe_account_token(ServiceCode=ServiceCode)
            resp_datas = get_oe_async_tasks_data_return(UrlPath=UrlPath,ParamJson=ParamJson,Token=token)
            try:
                code = eval(resp_datas.decode())["code"]
                data = str(resp_datas.decode()).replace(" ","").replace("'","")
            except:
                code = 0
        if int(code) == 0:
            request_id = "request_id#&#ds"+str(ParamJson).replace("'", "")+"request_id#&#ds"
            resp_data = request_id + resp_datas.decode()
            remark,data = get_write_local_file(RequestsData=resp_data, RequestID=request_id, DataFileDir=DataFileDir, DataFile=DataFile)
            if remark != "正常":
               code = 1
        elif int(code) in [40002, 40105, 40104]:
            code = 0
            data = str(data).replace(" ", "")
        else:
            code = 1
            data = str(data).replace(" ", "")
    except Exception as e:
        code = 1
        data = "请求失败：%s" % (str(e).replace("\n", "").replace(" ", "").replace("""\"""", ""))
    if int(code) != 0 or (int(code) == 0 and len(data) > 0):
        status = os.system(""" echo "%s %s %s %s %s %s">>%s/%s.%s """ % (time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()), ReturnAccountId, ServiceCode,str(ParamJson).replace(" ", ""), data, Token, DataFileDir, "account_status.log", hostname))
        if int(status) != 0:
            for i in range(10):
                status = os.system(""" echo "%s %s %s %s %s %s">>%s/%s.%s """ % (time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()), ReturnAccountId, ServiceCode,str(ParamJson).replace(" ", ""), data, Token, DataFileDir, "account_status.log", hostname))
                if int(status) == 0:
                    break;
    return code

def get_oe_async_tasks_data_return(UrlPath="",ParamJson="",Token=""):
    open_api_domain = "https://ad.toutiao.com"
    url = open_api_domain + UrlPath
    #params = {
    #    "advertiser_id": AccountId,
    #    "task_id": TaskId
    #}
    headers = {
        'Content-Type': "application/json",
        'Access-Token': Token,
        'Connection': "close"
    }
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    s = requests.session()
    retries = Retry(total=5,backoff_factor=0.1,status_forcelist=[500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    s.keep_alive = False
    resp = s.get(url=url, json=ParamJson,headers=headers, verify=False, stream=False, timeout=300)
    return resp.content

def get_write_local_file(RequestsData="",RequestID="",DataFileDir="",DataFile=""):
    file_name = """%s-%s.%s""" % (DataFile.split(".")[0], hostname, DataFile.split(".")[1])
    n = 0
    data = "写入日志正常"
    not_exist = "N"
    set_run = True
    while set_run:
        test_log = LogManager("""%s-%s""" % (DataFile.split(".")[0], hostname)).get_logger_and_add_handlers(2,log_path=DataFileDir,log_filename=file_name)
        test_log.info(RequestsData)
        get_dir = os.popen("ls -t %s|grep %s" % (DataFileDir, file_name))
        for files in get_dir.read().split():
            is_exist = os.popen("grep -o '%s' %s/%s" % (RequestID, DataFileDir, files))
            is_exist_value = is_exist.read().split()
            if is_exist_value is not None and len(is_exist_value) > 0:
                not_exist = "Y"
                break;
        if not_exist == "Y":
            remark = "正常"
            set_run = False
        else:
            if n > 20:
                remark = "异常"
                data = "写入日志失败"
                set_run = False
            else:
                time.sleep(2)
        n = n + 1
    return remark,data

#广告主
def get_advertiser_info(AccountIdList="",ServiceCode="",DataFileDir="",DataFile=""):
    token = get_oe_account_token(ServiceCode=ServiceCode)
    open_api_url_prefix = "https://ad.oceanengine.com/open_api/"
    uri = "2/advertiser/info/"
    url = open_api_url_prefix + uri
    params = {
        "advertiser_ids": AccountIdList
    }
    headers = {"Access-Token": token,
               'Connection': "close"
               }
    code = 1
    try:
        rsp = requests.get(url, json=params, headers=headers)
        rsp_data = rsp.json()
        code = rsp_data["code"]
        if int(code) == 0 :
           rsp_data["returns_account_id"] = str(AccountIdList).replace("[","").replace("]","")
           test_log = LogManager("""%s-%s""" % (DataFile.split(".")[0], hostname)).get_logger_and_add_handlers(2,log_path=DataFileDir,log_filename="""%s-%s.%s""" % (DataFile.split(".")[0],hostname,DataFile.split(".")[1]))
           test_log.info(json.dumps(rsp_data))
        elif int(code) in [40002, 40105, 40104]:
            code = 0
            os.system(""" echo "%s %s %s">>%s/%s.%s """%(str(rsp_data).replace(" ",""),AccountIdList,ServiceCode,DataFileDir,"account_status.log",hostname))
        else:
           code = 1
    except Exception as e:
        code = 1
    return code

#创意详情
def get_creative_detail_datas(ParamJson="", UrlPath="", DataFileDir="", DataFile=""):
        param_json = json.dumps(ParamJson)
        param_json = ast.literal_eval(json.loads(param_json))
        service_code = param_json["service_code"]
        token = get_oe_account_token(ServiceCode=service_code)
        del param_json["service_code"]
        try:
            data_list = set_sync_data(ParamJson=param_json, UrlPath=UrlPath, Token=token)
            code = data_list["code"]
            if int(code) == 0:
                test_log = LogManager("""%s-%s""" % (DataFile.split(".")[0], hostname)).get_logger_and_add_handlers(2,log_path=DataFileDir,log_filename="""%s-%s.%s""" % (DataFile.split(".")[0],hostname,DataFile.split(".")[1]))
                test_log.info(json.dumps(data_list))
            else:
                # 没权限及token失败
                if int(code) in [40002, 40105, 40104]:
                    code = 0
                    os.system(""" echo "%s">>%s/%s.%s """ % (str(data_list).replace(" ",""), DataFileDir, "account_status.log", hostname))
                else:
                    code = 1
        except Exception as e:
            code = 1
        return code

#通过代理ID获取accountID
def set_services(Media="",ServiceId="",Token="",Page="",PageSize=""):
    if int(Media) == 2:
       open_api_url_prefix = "https://ad.oceanengine.com/open_api/"
       uri = "2/agent/advertiser/select/"
       url = open_api_url_prefix + uri
       params = {
           "advertiser_id":int(ServiceId),
           "page":Page,
           "page_size":PageSize
       }
       headers = {"Access-Token": Token}
       rsp = requests.get(url, json=params, headers=headers)
       rsp_data = rsp.json()
    elif int(Media) == 203:
        open_api_url_prefix = "https://ad.oceanengine.com/open_api/"
        uri = "2/majordomo/advertiser/select/"
        url = open_api_url_prefix + uri
        params = {
            "advertiser_id": int(ServiceId)
        }
        headers = {"Access-Token": Token}
        rsp = requests.get(url, json=params, headers=headers)
        rsp_data = rsp.json()
    return rsp_data

def get_services(ServiceId="",ServiceCode="",Media="",Page="",PageSize="",DataFile="",PageFileData="",TaskFlag=""):
    total_page = 0
    data = ""
    try:
      token = get_oe_account_token(ServiceCode=ServiceCode)
      get_data = set_services(Media=Media,ServiceId=ServiceId, Token=token, Page=Page, PageSize=PageSize)
      code = get_data["code"]
      if int(code) == 0:
          remark = "正常"
          if int(Media) == 2:
             total_page = int(get_data["data"]["page_info"]["total_page"])
             for advertiser_id in get_data["data"]["advertiser_ids"]:
                 os.system("""echo "%s %s %s %s">>%s.%s """ % (ServiceId,ServiceCode,advertiser_id,Media,DataFile,hostname))
          elif int(Media) == 203:
             for list_data in get_data["data"]["list"]:
                 os.system("""echo "%s %s %s %s">>%s.%s """ % (ServiceId, ServiceCode, list_data["advertiser_id"], Media, DataFile, hostname))
      else:
          # 没权限及token失败
          if int(code) in [40002, 40105, 40104]:
              remark = "正常"
              data = str(get_data).replace(" ", "")
          else:
              remark = "异常"
              data = str(get_data).replace(" ", "")
    except Exception as e:
      remark = "异常"
      data = "请求失败"
    if PageFileData is not None and len(PageFileData) > 0 and PageFileData != "":
      os.system("""echo "%s %s %s %s %s %s %s %s">>%s.%s""" % (total_page, ServiceId, ServiceCode, remark, data, ServiceId, TaskFlag,Media, PageFileData,hostname))
    return remark

#不翻页处理
def set_not_page(UrlPath="",ParamJson="",ServiceCode="",Token="",DataFileDir="",DataFile="",ReturnAccountId=""):
    code = 1
    data = ""
    set_run = True
    n = 0
    not_exist = "N"
    try:
      rsp_data = set_sync_data(ParamJson=ParamJson, UrlPath=UrlPath, Token=Token)
      code = rsp_data["code"]
      # token无效重试
      if int(code) == 40105:
          token = get_oe_account_token(ServiceCode=ServiceCode)
          rsp_data = set_sync_data(ParamJson=ParamJson, UrlPath=UrlPath, Token=token)
          code = rsp_data["code"]
      rsp_data["returns_account_id"] = str(ReturnAccountId)
      rsp_data["returns_columns"] = str(ParamJson)
      request_id = rsp_data["request_id"]
      if int(code) == 0:
        file_name = """%s-%s.%s""" % (DataFile.split(".")[0],hostname,DataFile.split(".")[1])
        while set_run:
          test_log = LogManager("""%s-%s""" % (DataFile.split(".")[0], hostname)).get_logger_and_add_handlers(2,log_path=DataFileDir,log_filename=file_name)
          test_log.info(json.dumps(rsp_data))
          get_dir = os.popen("ls -t %s|grep %s" % (DataFileDir, file_name))
          for files in get_dir.read().split():
              is_exist = os.popen("grep -o '%s' %s/%s" % (request_id, DataFileDir, files))
              is_exist_value = is_exist.read().split()
              if is_exist_value is not None and len(is_exist_value) > 0:
                  not_exist = "Y"
                  break;
          if not_exist == "Y":
              set_run = False
          else:
              if n > 20:
                  code = 1
                  data = "写入日志失败"
                  set_run = False
              else:
                  time.sleep(2)
          n = n + 1
      elif int(code) in [40002, 40105, 40104]:
          code = 0
          data = str(rsp_data).replace(" ", "")
      else:
          code = 1
          data = str(rsp_data).replace(" ","")
    except Exception as e:
        code = 1
        data = "请求失败：%s"%(str(e).replace("\n","").replace(" ","").replace("""\"""",""))
    if int(code) != 0:
      status = os.system(""" echo "%s %s %s %s %s %s">>%s/%s.%s """ % (time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()),ReturnAccountId, ServiceCode, str(ParamJson).replace(" ",""), data,Token,DataFileDir, "account_status.log", hostname))
      if int(status) != 0:
       for i in range(10):
        status = os.system(""" echo "%s %s %s %s %s %s">>%s/%s.%s """ % (time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()), ReturnAccountId, ServiceCode,str(ParamJson).replace(" ", ""), data, Token, DataFileDir, "account_status.log", hostname))
        if int(status) == 0:
            break;
    return code

#翻页处理
def set_pages(UrlPath="",ParamJson="",ServiceCode="",Token="",DataFileDir="",DataFile="",ReturnAccountId="",TaskFlag="",PageTaskFile=""):
    page = 0
    data = ""
    set_run = True
    n = 0
    token = None
    not_exist = "N"
    try:
      rsp_data = set_sync_data(ParamJson=ParamJson, UrlPath=UrlPath, Token=Token)
      code = rsp_data["code"]
      #token无效重试
      if int(code) == 40105:
          token = get_oe_account_token(ServiceCode=ServiceCode)
          rsp_data = set_sync_data(ParamJson=ParamJson, UrlPath=UrlPath, Token=token)
          code = rsp_data["code"]
      rsp_data["returns_account_id"] = str(ReturnAccountId)
      rsp_data["returns_columns"] = str(ParamJson)
      request_id = rsp_data["request_id"]
      if int(code) == 0:
         file_name = """%s-%s.%s""" % (DataFile.split(".")[0],hostname,DataFile.split(".")[1])
         while set_run:
           test_log = LogManager("""%s-%s""" % (DataFile.split(".")[0], hostname)).get_logger_and_add_handlers(2,log_path=DataFileDir,log_filename=file_name)
           test_log.info(json.dumps(rsp_data))
           get_dir = os.popen("ls -t %s|grep %s" % (DataFileDir, file_name))
           for files in get_dir.read().split():
               is_exist = os.popen("grep -o '%s' %s/%s" % (request_id,DataFileDir,files))
               is_exist_value = is_exist.read().split()
               if is_exist_value is not None and len(is_exist_value) > 0:
                  not_exist = "Y"
                  break;
           if not_exist == "Y":
              remark = "正常"
              set_run = False
           else:
              if n > 20:
                  remark = "异常"
                  data = "写入日志失败"
                  set_run = False
              else:
                  time.sleep(2)
           n = n + 1
         page = rsp_data["data"]["page_info"]["total_page"]
         if page == 0:
            data = str(rsp_data).replace(" ", "")
      elif int(code) in [40002, 40105, 40104]:
          remark = "正常"
          data = str(rsp_data).replace(" ", "")
      else:
          remark = "异常"
          data = str(rsp_data).replace(" ", "")
    except Exception as e:
        remark = "异常"
        data = "请求失败：%s"%(str(e).replace("\n","").replace(" ","").replace("""\"""",""))
    set_run = True
    n = 0
    while set_run:
      status = os.system("""echo "%s %s %s %s %s %s %s %s">>%s.%s""" % (page, ReturnAccountId, ServiceCode, remark, data, str(ParamJson).replace(" ",""), TaskFlag,Token, PageTaskFile,hostname))
      if int(status) == 0:
         set_run = False
      else:
         if n > 10:
           remark = "异常"
           set_run = False
         else:
           time.sleep(2)
      n = n + 1
    return remark

#etl_mid->Ods层
def get_data_2_ods(HiveSession="",BeelineSession="",SourceDB="",SourceTable="",TargetDB="", TargetTable=""
                   ,IsReport="",SelectExcludeColumns="",KeyColumns="",ExecDate="",ArrayFlag="",CustomSetParameter="",IsReplace="Y",DagId="",TaskId=""):
    etl_ods_field_diff = def_ods_structure(HiveSession=HiveSession, BeelineSession=BeelineSession
                                           ,SourceTable=SourceTable, TargetDB=TargetDB, TargetTable=TargetTable
                                           ,IsTargetPartition="Y", ExecDate=ExecDate, ArrayFlag=ArrayFlag
                                           ,IsReplace=IsReplace)
    print("返回的表差异 %s || %s || %s" % (etl_ods_field_diff[0], etl_ods_field_diff[1], etl_ods_field_diff[2]))
    ok, get_ods_column = HiveSession.get_column_info(TargetDB, TargetTable)
    system_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    system_table_columns = "returns_account_id,returns_colums,request_type,extract_system_time,etl_date"
    is_key_columns(SourceDB=SourceDB, SourceTable=SourceTable, TargetDB=TargetDB, TargetTable=TargetTable,
                   ExecDate=ExecDate, KeyColumns=KeyColumns,DagId=DagId,TaskId=TaskId)
    row_number_columns = ""
    key_column_list = KeyColumns.split(",")
    for key in key_column_list:
        row_number_columns = row_number_columns + "," + "`" + key + "`"
    row_number_columns = row_number_columns.replace(",", "", 1)
    select_exclude_columns = SelectExcludeColumns
    if select_exclude_columns is None or len(select_exclude_columns) == 0:
        select_exclude_columns = "000000"
    columns = ""
    for column in get_ods_column:
        columns = columns + "," + column[0]
        if column[0] == "etl_date":
            break;
    columns = columns.replace(",", "", 1)
    json_tuple_columns = ""
    for get_json_tuple_column in columns.split(","):
        if get_json_tuple_column not in select_exclude_columns.split(
                ",") and get_json_tuple_column not in system_table_columns.split(","):
            json_tuple_columns = json_tuple_columns + "," + "'%s'" % (get_json_tuple_column)
    json_tuple_columns = json_tuple_columns.replace(",", "", 1)
    json_tuple_column = json_tuple_columns.replace("'", "")
    select_json_tuple_column = json_tuple_columns.replace("'", "`")
    columns = ','.join("`%s`" % (x) for x in columns.split(",") if x != 'etl_date')
    array_flag = ArrayFlag
    if array_flag in ["list", "custom_audience_list"]:
        if IsReplace == "Y":
            regexp_extract = """get_json_object(regexp_replace(regexp_extract(a.request_data,'(\\\\"\\\\}## \\\\{\\\\".*)',1),'\\\\"\\\\}## ',''),'$.data.%s') as data_colums""" % (array_flag)
        else:
            regexp_extract = """get_json_object(a.request_data,'$.data.%s') as data_colums""" % (array_flag)
    else:
        if IsReplace == "Y":
            regexp_extract = """concat(concat('[',get_json_object(regexp_replace(regexp_extract(a.request_data,'(\\\\"\\\\}## \\\\{\\\\".*)',1),'\\\\"\\\\}## ',''),'$.data')),']') as data_colums"""
        else:
            regexp_extract = """concat(concat('[',regexp_replace(get_json_object(a.request_data,'$.data'),'^\\\\[|\\\\]$','')),']') as data_colums"""
    if IsReplace == "Y":
        return_regexp_extract = """regexp_replace(regexp_extract(a.request_data,'(##\\\\{\\\\"accountId\\\\":.*\\\\}##)',1),'##','') as returns_colums"""
        returns_account_id = """trim(get_json_object(regexp_replace(regexp_replace(regexp_extract(a.request_data,'(##\\\\{\\\\"accountId\\\\":.*\\\\}## )',1),'##',''),' ',''),'$.accountId')) as returns_account_id"""
        filter_line = """ and length(regexp_extract(a.request_data,'(\\\\"\\\\}## \\\\{\\\\".*)',1)) > 0"""
    else:
        return_regexp_extract = """trim(get_json_object(a.request_data,'$.returns_columns')) as returns_colums"""
        returns_account_id = """trim(get_json_object(a.request_data,'$.returns_account_id')) as returns_account_id"""
        filter_line = ""

    specified_pars_str = etl_ods_field_diff[3]
    specified_pars_list = etl_ods_field_diff[2]
    null_field_set = list(set(json_tuple_column.split(",")).difference(set(specified_pars_list)))
    null_field_list = []
    for null_field in null_field_set:
        null_field_list.append(",cast( null as String) as `%s`" % (null_field))
    null_field_str = ''.join(null_field_list)
    null_field_str = null_field_str + ",'%s' as `extract_system_time`" % (system_time)

    print("Json待解析字段：" + specified_pars_str)
    if specified_pars_str is not None and len(specified_pars_str) > 0:
        pars_str_list = []
        for pars_field in specified_pars_str.split(","):
            as_str = pars_field.split(".")[-1]
            pars_str_list.append("get_json_object(data_num_colums,'$.%s') as `%s`" % (pars_field, as_str))
        pars_str = ','.join(pars_str_list)
        sql = """
                add file hdfs:///tmp/airflow/get_arrary.py;
                drop table if exists %s.%s_tmp;
                create table %s.%s_tmp stored as parquet as 
                select %s
                from (select returns_colums,%s %s,returns_account_id,request_type
                      from(select split(split(data_colums,'@@####@@')[0],'##&&##')[0] as returns_colums
                                  ,split(data_colums,'@@####@@')[1] as data_colums
                                  ,split(split(data_colums,'@@####@@')[0],'##&&##')[1] as returns_account_id
                                  ,split(split(data_colums,'@@####@@')[0],'##&&##')[2] as request_type
                           from(select transform(concat_ws('##@@',concat_ws('##&&##',returns_colums,returns_account_id,request_type),data_colums)) USING 'python get_arrary.py' as (data_colums)
                                from(select %s
                                            ,%s
                                            ,%s
                                            ,request_type
                                     from %s.%s a
                                     where a.etl_date = '%s'
                                        %s
                                    ) a
                                where data_colums is not null
                                    and data_colums  <> '[]'
                                ) b
                           ) c
                           lateral view explode(split(data_colums, '##@@')) num_line as data_num_colums
                      ) a
                      ;
                """ % (
        "etl_mid", TargetTable, "etl_mid", TargetTable, columns, pars_str, null_field_str, return_regexp_extract,
        regexp_extract, returns_account_id, SourceDB, SourceTable, ExecDate, filter_line)

    ok = BeelineSession.execute_sql(sql, CustomSetParameter)
    if ok is False:
        msg = get_alert_info_d(DagId=DagId, TaskId=TaskId,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (TargetDB, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="ods入库-tmp失败！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)
    sql = """
                insert overwrite table %s.%s
                partition(etl_date = '%s')
                select %s from(
                select %s,row_number()over(partition by %s order by 1) as rn_row_number
                from %s.%s_tmp
                ) tmp where rn_row_number = 1
                       ;
                drop table if exists %s.%s_tmp;
                """ % (
    TargetDB, TargetTable, ExecDate, columns, columns, row_number_columns, "etl_mid", TargetTable, "etl_mid", TargetTable)
    ok = BeelineSession.execute_sql(sql, CustomSetParameter)
    if ok is False:
        msg = get_alert_info_d(DagId=DagId, TaskId=TaskId,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (TargetDB, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="ods入库失败！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)

def is_key_columns(SourceDB="",SourceTable="",TargetDB="",TargetTable="",ExecDate="",KeyColumns="",DagId="",TaskId=""):
    if KeyColumns is None or len(KeyColumns) == 0:
        msg = get_alert_info_d(DagId=DagId, TaskId=TaskId,
                               SourceTable="%s.%s" % (SourceDB, SourceTable),
                               TargetTable="%s.%s" % (TargetDB, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="请确认配置表指定主键字段是否正确！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)

#Ods->Snap层
def get_data_2_snap(HiveSession="",BeelineSession="",SourceDB="",SourceTable="",
                        TargetDB="", TargetTable="",IsReport="",KeyColumns="",ExecDate="",CustomSetParameter="",DagId="",TaskId=""):
   adj_snap_structure(HiveSession=HiveSession,BeelineSession=BeelineSession,SourceDB=SourceDB,SourceTable=SourceTable,
                        TargetDB=TargetDB, TargetTable=TargetTable,CustomSetParameter=CustomSetParameter,IsReport=IsReport)
   # 获取snap表字段
   ok, snap_table_columns = HiveSession.get_column_info(TargetDB, TargetTable)

   IsTargetPartition = "Y" if IsReport == 1 else "N"
   snap_columns_tmp_0 = []  # 日报一定得分区，同时排除分区字段etl_date
   snap_columns_tmp_1 = []  # 日报一定得分区，同时排除分区字段etl_date
   for column in snap_table_columns:
       snap_columns_tmp_0.append("a.`%s`" % (column[0]))
       if column[0] != 'etl_date':
           snap_columns_tmp_1.append("a.`%s`" % (column[0]))
       elif IsTargetPartition == "N":#兼容新增字段加载etl_date后面
           continue
       else:
           break
   snap_columns = ",".join(snap_columns_tmp_0)
   snap_columns_1 = ",".join(snap_columns_tmp_1)


   if IsTargetPartition == "N":
       is_key_columns(SourceDB=SourceDB, SourceTable=SourceTable, TargetDB=TargetDB,
                      TargetTable=TargetTable, ExecDate=ExecDate, KeyColumns=KeyColumns)
       key_column_list = KeyColumns.split(",")
       is_null_col = "`"+key_column_list[0]+"`"
       key_columns_joins = ""
       num = 0
       for key in key_column_list:
           if num == 0:
              key_columns_join = "on a.`%s` = b.`%s`"%(key,key)
           else:
               key_columns_join = "and a.`%s` = b.`%s`" % (key, key)
           num += 1
           key_columns_joins = key_columns_joins + " " + key_columns_join
           num = num + 1

       sql = """
           insert overwrite table %s.%s
           select %s
           from %s.%s a
           left join %s.%s b
           %s
           and b.etl_date = '%s'
           where b.%s is null
              union all
           select %s from %s.%s a where etl_date = '%s'
       """%(TargetDB,TargetTable,snap_columns,TargetDB,TargetTable,SourceDB,SourceTable,
            key_columns_joins,ExecDate,is_null_col,snap_columns,SourceDB, SourceTable,ExecDate
            )
   else:

       sql = """
        insert overwrite table %s.%s
        partition(etl_date = '%s')
        select %s
        from %s.%s a where etl_date = '%s' 
       """%(TargetDB,TargetTable,ExecDate,snap_columns_1,SourceDB,SourceTable,ExecDate)
   ok = BeelineSession.execute_sql(sql,CustomSetParameter)
   if ok is False:
       msg = get_alert_info_d(DagId=DagId, TaskId=TaskId,
                              SourceTable="%s.%s" % (SourceDB, SourceTable),
                              TargetTable="%s.%s" % (TargetDB, TargetTable),
                              BeginExecDate=ExecDate,
                              EndExecDate=ExecDate,
                              Status="Error",
                              Log="snap入库失败！！！",
                              Developer="developer")
       set_exit(LevelStatu="red", MSG=msg)
   sql_check = """
               select a.source_cnt,b.target_cnt
               from(select count(1) as source_cnt
                    from %s.%s where etl_date = '%s'
                   ) a
               inner join (
                    select count(1) as target_cnt
                    from %s.%s where etl_date = '%s'
               ) b
               on 1 = 1
               where a.source_cnt <> b.target_cnt
               """%(SourceDB,SourceTable,ExecDate,TargetDB,TargetTable,ExecDate)
   #ok, data = HiveSession.get_all_rows(sql_check)
   data = []
   ok = True
   print("snap入库数据：" + str(data))
   if ok is False or len(data) > 0:
       msg = get_alert_info_d(DagId=DagId, TaskId=TaskId,
                              SourceTable="%s.%s" % (SourceDB, SourceTable),
                              TargetTable="%s.%s" % (TargetDB, TargetTable),
                              BeginExecDate=ExecDate,
                              EndExecDate=ExecDate,
                              Status="Error",
                              Log="snap入库数据对比不上！！！",
                              Developer="developer")
       set_exit(LevelStatu="red", MSG=msg)