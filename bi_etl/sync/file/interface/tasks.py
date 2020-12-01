# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: tasks.py
# @Software: PyCharm
# function info：定义celery任务

from __future__ import absolute_import, unicode_literals
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tylerscope import app
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_oe_async_status_content_content
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_oe_save_exception_file
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_oe_async_tasks_data
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_set_oe_async_tasks_create
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_sync_data_return
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_oe_async_tasks_data_return
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_sync_data

import time
import socket

hostname = socket.gethostname()

#定义oe任务创建
@app.task(rate_limit='5/m')
def get_test(string=""):
    now = time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime())
    print(now,"=================================")

#定义oe任务创建
@app.task(rate_limit='750/m')
def get_oe_async_tasks_create_all(AsyncTaskName="", AsyncTaskFile="", AsyncTaskExceptionFile="",ExecData="",ExecDate=""):
    account_id = ExecData[0]
    interface_flag = ExecData[1]
    media_type = ExecData[2]
    service_code = ExecData[3]
    group_by = str(ExecData[4]).split(",")
    fields = ExecData[5]
    token = ExecData[6]
    if fields == "" or fields is None or len(fields) == 0 or fields == "NULL" or fields == "null":
        fields = []
    else:
        fields = fields.split(",")
    set_true = True
    n = 1
    print("执行创建子账户：%s"%(account_id))
    while set_true:
      try:
        #if int(AsyncTaskName)%2 == 0:
        # time.sleep(2)
        get_set_oe_async_tasks_create(InterfaceFlag=interface_flag, MediaType=media_type, ServiceCode=service_code,
                                       AccountId=account_id, AsyncTaskName=AsyncTaskName, AsyncTaskFile=AsyncTaskFile,
                                       ExecDate=ExecDate,GroupBy=group_by, Fields=fields,Token=token)
        set_true = False
      except Exception as e:
         #if n > 3:
         print("异常创建子账户：%s" % (account_id))
         get_oe_save_exception_file(ExceptionType="create",ExecData=ExecData,AsyncNotemptyFile=AsyncTaskFile,AsyncStatusExceptionFile=AsyncTaskExceptionFile,ExecDate=ExecDate)
         set_true = False
         #else:
         # time.sleep(360)
      n = n + 1

#定义oe任务创建
@app.task(rate_limit='10/s')
def get_oe_async_tasks_create_all_exception(AsyncTaskName="", AsyncTaskFile="", AsyncTaskExceptionFile="",ExecData="",ExecDate=""):
    account_id = ExecData[0]
    interface_flag = ExecData[1]
    media_type = ExecData[2]
    service_code = ExecData[3]
    group_by = str(ExecData[4]).split(",")
    fields = ExecData[5]
    token = ExecData[6]
    if fields == "" or fields is None or len(fields) == 0 or fields == "NULL" or fields == "null":
        fields = []
    else:
        fields = fields.split(",")
    set_true = True
    n = 1
    print("执行创建子账户：%s"%(account_id))
    while set_true:
      try:
        #if int(AsyncTaskName)%2 == 0:
        # time.sleep(2)
        get_set_oe_async_tasks_create(InterfaceFlag=interface_flag, MediaType=media_type, ServiceCode=service_code,
                                       AccountId=account_id, AsyncTaskName=AsyncTaskName, AsyncTaskFile=AsyncTaskFile,
                                       ExecDate=ExecDate,GroupBy=group_by, Fields=fields,Token=token)
        set_true = False
      except Exception as e:
         #if n > 3:
         print("异常创建子账户：%s" % (account_id))
         get_oe_save_exception_file(ExceptionType="create",ExecData=ExecData,AsyncNotemptyFile=AsyncTaskFile,AsyncStatusExceptionFile=AsyncTaskExceptionFile,ExecDate=ExecDate)
         set_true = False
         #else:
         # time.sleep(360)
      n = n + 1

#定义oe任务创建
@app.task(rate_limit='10/s')
def get_oe_async_tasks_create(AsyncTaskName="", AsyncTaskFile="", AsyncTaskExceptionFile="",ExecData="",ExecDate=""):
    account_id = ExecData[0]
    interface_flag = ExecData[1]
    media_type = ExecData[2]
    service_code = ExecData[3]
    group_by = str(ExecData[4]).split(",")
    fields = ExecData[5]
    token = ExecData[6]
    if fields == "" or fields is None or len(fields) == 0 or fields == "NULL" or fields == "null":
        fields = []
    else:
        fields = fields.split(",")
    set_true = True
    n = 1
    print("执行创建子账户：%s"%(account_id))
    while set_true:
      try:
        #if int(AsyncTaskName)%2 == 0:
        # time.sleep(2)
        get_set_oe_async_tasks_create(InterfaceFlag=interface_flag, MediaType=media_type, ServiceCode=service_code,
                                       AccountId=account_id, AsyncTaskName=AsyncTaskName, AsyncTaskFile=AsyncTaskFile,
                                       ExecDate=ExecDate,GroupBy=group_by, Fields=fields,Token=token)
        set_true = False
      except Exception as e:
         #if n > 3:
         print("异常创建子账户：%s" % (account_id))
         get_oe_save_exception_file(ExceptionType="create",ExecData=ExecData,AsyncNotemptyFile=AsyncTaskFile,AsyncStatusExceptionFile=AsyncTaskExceptionFile,ExecDate=ExecDate)
         set_true = False
         #else:
         # time.sleep(360)
      n = n + 1

#定义oe任务状态
@app.task
def get_oe_async_tasks_status(AsyncNotemptyFile="",AsyncEmptyFile="",AsyncStatusExceptionFile="",ExecData="",ExecDate=""):
    account_id = ExecData[0]
    set_true = True
    n = 1
    print("执行状态子账户：%s"%(account_id))
    while set_true:
      try:
         set_oe_async_status_content_content(ExecData=ExecData,AsyncNotemptyFile=AsyncNotemptyFile,AsyncEmptyFile=AsyncEmptyFile,ExecDate=ExecDate)
         set_true = False
      except Exception as e:
         if n > 3:
            print("异常状态子账户：%s" % (account_id))
            get_oe_save_exception_file(ExceptionType="status",ExecData=ExecData,AsyncNotemptyFile=AsyncNotemptyFile,AsyncStatusExceptionFile=AsyncStatusExceptionFile,ExecDate=ExecDate)
            set_true = False
         else:
          time.sleep(2)
      n = n + 1

#定义oe任务数据
@app.task(time_limit=600)
def get_oe_async_tasks_data(DataFile="",ExceptionFile="",ExecData="",ExecDate="",AirflowInstance=""):
    account_id = ExecData[0]
    set_true = True
    n = 1
    print("执行数据子账户：%s"%(account_id))
    while set_true:
       code = set_oe_async_tasks_data(DataFile=DataFile,ExecData=ExecData,AirflowInstance=AirflowInstance)
       if code != 0:
         if n > 3:
            print("异常数据子账户：%s" % (account_id))
            get_oe_save_exception_file(ExceptionType="data",ExecData=ExecData, AsyncNotemptyFile="",AsyncStatusExceptionFile=ExceptionFile,ExecDate=ExecDate,AirflowInstance=AirflowInstance)
            set_true = False
         else:
            time.sleep(2)
       else:
         set_true = False
       n = n + 1

#定义oe任务数据
@app.task(time_limit=600)
def get_oe_async_tasks_data_return(DataFile="",ExceptionFile="",ExecData="",ExecDate="",AirflowInstance=""):
    account_id = ExecData[0]
    set_true = True
    n = 1
    data = "####"
    print("执行数据子账户：%s"%(account_id))
    while set_true:
       code,data = set_oe_async_tasks_data_return(DataFile=DataFile,ExecData=ExecData,AirflowInstance=AirflowInstance)
       if code != 0:
         if n > 3:
            print("异常数据子账户：%s" % (account_id))
            get_oe_save_exception_file(ExceptionType="data",ExecData=ExecData, AsyncNotemptyFile="",AsyncStatusExceptionFile=ExceptionFile,ExecDate=ExecDate,AirflowInstance=AirflowInstance)
            set_true = False
         else:
            time.sleep(2)
       else:
         set_true = False
       n = n + 1
    return data

#定义oe同步数据
@app.task(rate_limit='20/s')
def get_oe_sync_tasks_data_return(ParamJson="",UrlPath=""):
    return get_sync_data_return(ParamJson=ParamJson,UrlPath=UrlPath)

@app.task(rate_limit='1000/m',worker_concurrency=200)
def get_oe_sync_tasks_data(ParamJson="",UrlPath=""):
   try:
     get_sync_data(ParamJson=ParamJson,UrlPath=UrlPath)
   except Exception as e:
     print("异常！！！！！！")