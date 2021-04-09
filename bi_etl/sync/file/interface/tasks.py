# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: tasks.py
# @Software: PyCharm
# function info：定义celery任务

#from __future__ import absolute_import, unicode_literals
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tylerscope import app
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_oe_async_status_content_content
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_oe_save_exception_file
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_oe_async_tasks_data
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_set_oe_async_tasks_create
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_sync_data_return
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_oe_async_tasks_data_return
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_tc_async_tasks_data_return
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_advertiser_info
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_creative_detail_datas
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_services
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_not_page
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_pages
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_oe_create_async_tasks
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_tc_add_async_tasks
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_sync_data
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_oe_status_async_tasks
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_tc_status_async_tasks
from ecsage_bigdata_etl_engineering.common.base.get_config import Conf
from ecsage_bigdata_etl_engineering.bi_etl.web_interface.exec_interface_script import execute
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_write_local_file
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
import json
from hashlib import md5
import ast
import os
import time
import socket
import random

conf = Conf().conf
hostname = socket.gethostname()

#定义oe任务创建
@app.task(rate_limit='5/m')
def get_test(**kwargs):
    now = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    return kwargs

@app.task()
def PMI(words_fre="", pmi_threshold="",i=""):
    import re
    """
    凝固度：min{P(abc)/P(ab)P(c),P(abc)/P(a)P(bc)}
    """
    #过滤单个字
    if len(i) == 1:
        pass
    else:
        if len(i) == 3:
            pmi_threshold = 0.05
        #计算px*py
        p_x_p_y = min([words_fre.get(i[:j]) * words_fre.get(i[j:]) for j in range(1, len(i))])
        #大于阈值的添加为新词
        if words_fre.get(i) / p_x_p_y > pmi_threshold:
          #过滤掉含有字母
          if len(re.findall(re.compile(r'[A-Za-z]'), i)) == 0:
            #new_words.append(i)
            os.system(""" echo "%s,%s,%s,%s,%s">>/root/wangsong/data.pmi.log """ % (i,words_fre.get(i),p_x_p_y,words_fre.get(i) / p_x_p_y,pmi_threshold))


#处理报表接口
@app.task()
def get_web_interface_data(**kwargs):
    """
    元数据表：web_interface_info
    param page:
      {"kwargs": {"interface_id":"xxx",
                  "page": 1,
                  "page_size":100
               }
      }

    param not page:
      {"kwargs": {"interface_id":"xxx"
               }
      }

    return not page:
     {"result": {"code": 0,  #请求接口成功，并返回数据，若code非0，则请求失败，接口使用方最好通过这个来判断接口访问是否成功
                 "msg": "OK",
                 "data": {"list": [{"returns_account_id": "1688019616093198","cost": "0.0300"}]},
                 "request_begin_time": "2021-02-24 15:22:45",
                 "request_end_time": "2021-02-24 15:22:45"
                },
      "state": "SUCCESS", #已接到请求，并处理成功，但不代表处理接口业务逻辑成功
      "task-id": "ced6fd57-419e-4b8e-8d99-0770be717cb4"
     }

     return page:
     {"result": {"code": 0,  #请求接口成功，并返回数据，若code非0，则请求失败，接口使用方最好通过这个来判断接口访问是否成功
                 "msg": "OK",
                 "data": {"list": [{"returns_account_id": "1688019616093198","cost": "0.0300"}]},
                 "page": 1,
                 "page_size":100,
                 "total_page":5,
                 "request_begin_time": "2021-02-24 15:22:45",
                 "request_end_time": "2021-02-24 15:22:45"
                },
      "state": "SUCCESS", #已接到请求，并处理成功，但不代表处理接口业务逻辑成功
      "task-id": "ced6fd57-419e-4b8e-8d99-0770be717cb4"
     }
     mysql、impala：实现分页案例：select * from snap.etl_metadb_dags_info  order by id limit 20 offset 0
                  （1）order by必要指定字段，字段类型任意；
                  （2）limit必要指定，返回条数
                  （3）offset必要指定，返回从第几条开始，偏移量为0开始
    """
    data = execute(InterfaceParamsInfo=kwargs)
    return data

#定义oe任务创建
@app.task(rate_limit='1000/m')
def get_oe_async_tasks_create_all(AsyncTaskName="", AsyncTaskFile="", AsyncTaskExceptionFile="",ExecData="",ExecDate="",LocalDir=""):
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
      code = get_set_oe_async_tasks_create(InterfaceFlag=interface_flag, MediaType=media_type, ServiceCode=service_code,
                                       AccountId=account_id, AsyncTaskName=AsyncTaskName, AsyncTaskFile=AsyncTaskFile,
                                       ExecDate=ExecDate,GroupBy=group_by, Fields=fields,LocalDir=LocalDir)
      if int(code) == 0:
        set_true = False
      else:
        if n > 1:
          print("异常创建子账户：%s" % (account_id))
          get_oe_save_exception_file(ExceptionType="create",ExecData=ExecData,AsyncNotemptyFile=AsyncTaskFile,AsyncStatusExceptionFile=AsyncTaskExceptionFile,ExecDate=ExecDate)
          set_true = False
        else:
          time.sleep(10)
      n = n + 1

#定义oe任务创建
@app.task(rate_limit='1000/m')
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
@app.task(rate_limit='1000/m')
def get_oe_async_tasks_create(AsyncTaskName="", LocalDir="",AsyncTaskFile="", AsyncTaskExceptionFile="",ExecData="",ExecDate=""):
    account_id = ExecData[0]
    interface_flag = ExecData[1]
    media_type = ExecData[2]
    service_code = ExecData[3]
    group_by = str(ExecData[4]).split(",")
    fields = ExecData[5]
    if fields == "" or fields is None or len(fields) == 0 or fields == "NULL" or fields == "null":
        fields = []
    else:
        fields = fields.split(",")
    set_true = True
    n = 1
    print("执行创建子账户：%s"%(account_id))
    while set_true:
      code = get_set_oe_async_tasks_create(InterfaceFlag=interface_flag, MediaType=media_type, ServiceCode=service_code,
                                       AccountId=account_id, AsyncTaskName=AsyncTaskName, AsyncTaskFile=AsyncTaskFile,
                                       ExecDate=ExecDate,GroupBy=group_by, Fields=fields,LocalDir=LocalDir)
      if int(code) == 0:
        set_true = False
      else:
        if n > 3:
         print("异常创建子账户：%s" % (account_id))
         get_oe_save_exception_file(ExceptionType="create",ExecData=ExecData,AsyncNotemptyFile=AsyncTaskFile,AsyncStatusExceptionFile=AsyncTaskExceptionFile,ExecDate=ExecDate)
         set_true = False
        else:
          time.sleep(10)
      n = n + 1

#定义oe任务状态
@app.task(rate_limit='1000/m')
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
@app.task(time_limit=3600)
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
@app.task(rate_limit='1000/m')
def get_oe_async_tasks_data_return(DataFileDir="",DataFile="",UrlPath="",ParamJson="",Token="",
                                   ReturnAccountId="",ServiceCode="",TaskFlag="",
                                   TaskExceptionFile=""):
    print("执行数据子账户：%s"%(ReturnAccountId))
    set_true = True
    n = 0
    code = 9999
    try:
      while set_true:
          code = set_oe_async_tasks_data_return(DataFileDir=DataFileDir,DataFile=DataFile,UrlPath=UrlPath,ParamJson=ParamJson,Token=Token,ReturnAccountId=ReturnAccountId,ServiceCode=ServiceCode)
          if int(code) == 0:
              set_true = False
          else:
              if n > 2:
                  print("异常处理异步数据子账户：%s,%s" % (ReturnAccountId, ServiceCode))
                  status = os.system("""echo "%s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""), TaskFlag,Token, TaskExceptionFile + ".%s" % hostname))
                  if int(status) != 0:
                      for i in range(100):
                          status = os.system("""echo "%s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""),TaskFlag, Token, TaskExceptionFile + ".%s" % hostname))
                          if int(status) == 0:
                              break;
                  set_true = False
              else:
                  time.sleep(5)
          n = n + 1
      # 记录状态
      status_id = md5(str(str(ParamJson) + ServiceCode + Token).encode('utf8')).hexdigest()
      etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
      sql = """
               insert into metadb.celery_sync_status
               select '%s','%s'
            """ % (TaskFlag,status_id)
      ok = etl_md.execute_sql(sql=sql)
      if ok is False:
          code = 999999999
          print(code)
    except Exception as e:
        status = os.system("""echo "%s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""),TaskFlag, Token, TaskExceptionFile + ".%s" % hostname))
        if int(status) != 0:
            for i in range(100):
                status = os.system("""echo "%s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""),TaskFlag, Token, TaskExceptionFile + ".%s" % hostname))
                if int(status) == 0:
                    break;
    return """code：%s""" % (code)

#定义oe同步数据
@app.task(rate_limit='1000/m')
def get_oe_sync_tasks_data_return(ParamJson="",UrlPath="",PageTaskFile="",DataFileDir="",DataFile="",TaskFlag=""):
    set_true = True
    n = 0
    page = 0
    data_list = ""
    while set_true:
      remark = get_sync_data_return(ParamJson=ParamJson, UrlPath=UrlPath,PageTaskFile=PageTaskFile,DataFileDir=DataFileDir,DataFile=DataFile,TaskFlag=TaskFlag)
      if remark == "正常":
          set_true = False
      else:
        if n > 2:
          set_true = False
        else:
          time.sleep(2)
      n = n + 1
    #return data_list

@app.task(rate_limit='1000/m')
def get_oe_sync_tasks_data(ParamJson="",UrlPath="",TaskExceptionFile="",DataFileDir="",DataFile=""):
   set_true = True
   n = 0
   page = 0
   data = ""
   while set_true:
       remark,page = get_sync_data(ParamJson=ParamJson,UrlPath=UrlPath,DataFileDir=DataFileDir,DataFile=DataFile)
       if remark == "正常" and int(page) > 0:
           set_true = False
       else:
           if n > 5:
               os.system("""echo "异常：%s">>%s """%(advertiser_id,TaskExceptionFile+"1"))
               param_json = json.dumps(ParamJson)
               param_json = ast.literal_eval(json.loads(param_json))
               advertiser_id = param_json["advertiser_id"]
               service_code = param_json["service_code"]
               os.system("""echo "%s %s %s %s %s">>%s""" % (page, advertiser_id, service_code, remark, param_json["filtering"]["campaign_ids"], TaskExceptionFile))
               set_true = False
           else:
               time.sleep(5)
       n = n + 1
   #return data

@app.task(rate_limit='1000/m')
def get_advertisers_data(AccountIdList="",ServiceCode="",DataFileDir="",DataFile="",TaskExceptionFile="",InterfaceFlag=""):
   set_true = True
   n = 0
   while set_true:
       code = get_advertiser_info(AccountIdList=AccountIdList,ServiceCode=ServiceCode,DataFileDir=DataFileDir,DataFile=DataFile)
       if int(code) == 0:
           set_true = False
       else:
           if n > 1:
               print("异常：%s %s %s"% (AccountIdList,ServiceCode,InterfaceFlag))
               os.system("""echo "%s %s %s">>%s """ % (str(AccountIdList).replace("[","").replace("]",""),ServiceCode,InterfaceFlag, TaskExceptionFile+".%s"%hostname))
               set_true = False
           else:
               time.sleep(2)
       n = n + 1

#创意详情
@app.task(rate_limit='1000/m')
def get_creative_detail_data(ParamJson="", UrlPath="", DataFileDir="", DataFile="",InterfaceFlag="",TaskExceptionFile=""):
    set_true = True
    n = 0
    while set_true:
        code = get_creative_detail_datas(ParamJson=ParamJson, UrlPath=UrlPath, DataFileDir=DataFileDir, DataFile=DataFile)
        if int(code) == 0:
            set_true = False
        else:
            if n > 2:
                param_json = json.dumps(ParamJson)
                param_json = ast.literal_eval(json.loads(param_json))
                advertiser_id = param_json["advertiser_id"]
                service_code = param_json["service_code"]
                ad_id = param_json["ad_id"]
                os.system("""echo "%s %s %s %s">>%s """ % (advertiser_id, service_code, InterfaceFlag, ad_id, TaskExceptionFile + ".%s" % hostname))
                set_true = False
            else:
                time.sleep(2)
        n = n + 1

#获取代理下子账户页数
@app.task(rate_limit='1000/m')
def get_service_page_data(ServiceId="",ServiceCode="",Media="",Page="",PageSize="",
                          DataFile="",PageFileData="",TaskFlag=""):
    set_true = True
    n = 0
    while set_true:
        remark = get_services(ServiceId=ServiceId, ServiceCode=ServiceCode, Media=Media,
                              Page=Page, PageSize=PageSize,DataFile=DataFile,PageFileData=PageFileData,
                              TaskFlag=TaskFlag
                       )
        if remark == "正常":
            set_true = False
        else:
            if n > 2:
                print("异常获取子账户：%s,%s,%s"%(TaskFlag,ServiceId,ServiceCode))
                set_true = False
            else:
                time.sleep(2)
        n = n + 1

#获取代理下子账户
@app.task(rate_limit='1000/m')
def get_service_data(ServiceId="",ServiceCode="",Media="",Page="",PageSize="",
                     DataFile="",PageFileData="",TaskFlag="",TaskExceptionFile=""):
    set_true = True
    n = 0
    while set_true:
        remark = get_services(ServiceId=ServiceId, ServiceCode=ServiceCode, Media=Media,
                              Page=Page, PageSize=PageSize,DataFile=DataFile,PageFileData=PageFileData,
                              TaskFlag=TaskFlag
                       )
        if remark == "正常":
            set_true = False
        else:
            if n > 2:
                print("异常获取子账户：%s,%s,%s"%(TaskFlag,ServiceId,ServiceCode))
                os.system("""echo "%s %s %s %s %s %s">>%s """ % (ServiceId, ServiceCode, TaskFlag,Media, Page,PageSize, TaskExceptionFile + ".%s" % hostname))
                set_true = False
            else:
                time.sleep(5)
        n = n + 1

#处理不分页
@app.task(rate_limit='2000/m')
def get_not_page(UrlPath="",ParamJson="",ServiceCode="",Token="",ReturnAccountId="",
                 TaskFlag="",DataFileDir="",DataFile="",TaskExceptionFile="",
                 ArrayFlag="",TargetFlag="oe"):
    set_true = True
    n = 0
    code = 9999
    try:
      while set_true:
        code = set_not_page(UrlPath=UrlPath,ParamJson=ParamJson,ServiceCode=ServiceCode,Token=Token
                            ,DataFileDir=DataFileDir,DataFile=DataFile,ReturnAccountId=ReturnAccountId,ArrayFlag=ArrayFlag,TargetFlag=TargetFlag)
        if TargetFlag == "tc":
            sucess_code=[ int(x) for x in conf.get("Tc_Code", "sucess_code").split(",")]
        else:
            sucess_code=[ int(x) for x in conf.get("Oe_Code", "sucess_code").split(",")]
        if int(code) in sucess_code:
            set_true = False
        else:
            if n > 2:
              print("处理不分页异常：%s,%s"%(ReturnAccountId,ServiceCode))
              for i in range(100):
                status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ",""),ServiceCode,str(ReturnAccountId).replace(" ",""), TaskFlag,Token,int(code), TaskExceptionFile + ".%s" % hostname))
                if int(status) == 0:
                    break;
              set_true = False
            else:
              time.sleep(5)
        n = n + 1
      # 记录状态
      status_id = md5(str(str(ParamJson) + ServiceCode + Token).encode('utf8')).hexdigest()
      etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
      sql = """
           insert into metadb.celery_sync_status
           select '%s','%s'
        """ % (TaskFlag, status_id)
      ok = etl_md.execute_sql(sql=sql)
      if ok is False:
          code = 999999999
          print(code)
    except Exception as e:
        status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ",""),ServiceCode,str(ReturnAccountId).replace(" ",""), TaskFlag,Token,int(code), TaskExceptionFile + ".%s" % hostname))
        if int(status) != 0:
            for i in range(100):
                status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ",""),ServiceCode,str(ReturnAccountId).replace(" ",""), TaskFlag,Token,int(code), TaskExceptionFile + ".%s" % hostname))
                if int(status) == 0:
                    break;
    return """code：%s""" % (code)

#处理分页
@app.task(rate_limit='3000/m')
def get_pages(UrlPath="",ParamJson="",ServiceCode="",Token="",DataFileDir="",DataFile="",
              ReturnAccountId="",TaskFlag="",PageTaskFile="",TaskExceptionFile="",
              Pagestyle="",ArrayFlag="",TargetFlag="oe"):
    set_true = True
    n = 0
    code = 9999
    try:
       while set_true:
         code = set_pages(UrlPath=UrlPath,ParamJson=ParamJson,Token=Token,
                               ServiceCode=ServiceCode,DataFileDir=DataFileDir,
                               DataFile=DataFile,ReturnAccountId=ReturnAccountId,
                               TaskFlag=TaskFlag,PageTaskFile=PageTaskFile,Pagestyle=Pagestyle,ArrayFlag=ArrayFlag,TargetFlag=TargetFlag
                              )
         if TargetFlag == "tc":
             sucess_code=[ int(x) for x in conf.get("Tc_Code", "sucess_code").split(",")]
         else:
             sucess_code=[ int(x) for x in conf.get("Oe_Code", "sucess_code").split(",")]
         print(sucess_code)
         if int(code) in sucess_code:
             set_true = False
         else:
             if n > 2:
               print("异常分页：%s,%s"%(ReturnAccountId,ServiceCode))
               for i in range(100):
                 status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""), TaskFlag,Token,int(code),TaskExceptionFile + ".%s" % hostname))
                 if int(status) == 0:
                    break;
               set_true = False
             else:
               time.sleep(5)
         n = n + 1
       # 记录状态
       status_id = md5(str(str(ParamJson)+ServiceCode+Token).encode('utf8')).hexdigest()
       etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
       sql = """
           insert into metadb.celery_sync_status
           select '%s','%s'
        """ % (TaskFlag, status_id)
       ok = etl_md.execute_sql(sql=sql)
       if ok is False:
           code = 999999999
           print(code)
    except Exception as e:
        status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""), TaskFlag,Token,int(code),TaskExceptionFile + ".%s" % hostname))
        if int(status) != 0:
            for i in range(100):
                status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""), TaskFlag,Token,int(code),TaskExceptionFile + ".%s" % hostname))
                if int(status) == 0:
                    break;
    return """code：%s""" % (code)

#创建异步任务
@app.task(rate_limit='1000/m')
def get_oe_create_async_tasks(DataFileDir="",DataFile="",UrlPath="",ParamJson="",
                              Token="",ReturnAccountId="",ServiceCode="",InterfaceFlag="",
                              MediaType="",TaskExceptionFile="",TaskFlag=""):
    set_true = True
    n = 0
    code = 9999
    try:
      while set_true:
          code = set_oe_create_async_tasks(DataFileDir=DataFileDir, DataFile=DataFile, UrlPath=UrlPath, ParamJson=ParamJson, Token=Token,TaskFlag=TaskFlag,
                                           ReturnAccountId=ReturnAccountId, ServiceCode=ServiceCode, InterfaceFlag=InterfaceFlag, MediaType=MediaType)
          if int(code) == 0:
              set_true = False
          else:
              if n > 2:
                  print("处理创建异步任务异常：%s,%s" % (ReturnAccountId, ServiceCode))
                  status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % ( UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""), MediaType,Token,TaskFlag+"##"+InterfaceFlag, TaskExceptionFile + ".%s" % hostname))
                  if int(status) != 0:
                      for i in range(100):
                          status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""),MediaType, Token,TaskFlag+"##"+InterfaceFlag, TaskExceptionFile + ".%s" % hostname))
                          if int(status) == 0:
                              break;
                  set_true = False
              else:
                  time.sleep(5)
          n = n + 1
      # 记录状态
      status_id = md5(str(str(ParamJson) + ServiceCode + Token).encode('utf8')).hexdigest()
      etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
      sql = """
        insert into metadb.celery_sync_status
        select '%s','%s'
      """ % (TaskFlag, status_id)
      ok = etl_md.execute_sql(sql=sql)
      if ok is False:
          code = 999999999
          print(code)
    except Exception as e:
        status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""), MediaType, Token,TaskFlag + "##" + InterfaceFlag, TaskExceptionFile + ".%s" % hostname))
        if int(status) != 0:
            for i in range(100):
                status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""), MediaType,Token, TaskFlag + "##" + InterfaceFlag, TaskExceptionFile + ".%s" % hostname))
                if int(status) == 0:
                    break;
    return """code：%s""" % (code)

#定义oe任务状态
@app.task(rate_limit='1000/m')
def get_oe_status_async_tasks(ExecDate="",DataFileDir="",DataFile="",UrlPath="",ParamJson="",
                              Token="",ReturnAccountId="",ServiceCode="",MediaType="",TaskFlag="",
                              TaskExceptionFile=""):
    set_true = True
    n = 0
    code = 9999
    try:
      while set_true:
          code = set_oe_status_async_tasks(ExecDate=ExecDate,DataFileDir=DataFileDir,DataFile=DataFile,
                                           UrlPath=UrlPath,ParamJson=ParamJson,Token=Token,ReturnAccountId=ReturnAccountId,
                                           ServiceCode=ServiceCode,MediaType=MediaType,TaskFlag=TaskFlag)
          if int(code) == 0:
              set_true = False
          else:
              if n > 2:
                  print("处理异步任务状态异常：%s,%s,%s" % (TaskFlag,ReturnAccountId, ServiceCode))
                  status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""), MediaType,Token, TaskFlag, TaskExceptionFile + ".%s" % hostname))
                  if int(status) != 0:
                      for i in range(100):
                          status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""),MediaType, Token, TaskFlag, TaskExceptionFile + ".%s" % hostname))
                          if int(status) == 0:
                              break;
                  set_true = False
              else:
                  time.sleep(5)
          n = n + 1
      # 记录状态
      status_id = md5(str(str(ParamJson) + ServiceCode + Token).encode('utf8')).hexdigest()
      etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
      sql = """
        insert into metadb.celery_sync_status
        select '%s','%s'
      """ % (TaskFlag, status_id)
      ok = etl_md.execute_sql(sql=sql)
      if ok is False:
          code = 999999999
          print(code)
    except Exception as e:
        status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""), MediaType, Token,TaskFlag, TaskExceptionFile + ".%s" % hostname))
        if int(status) != 0:
            for i in range(100):
                status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""), MediaType,Token, TaskFlag, TaskExceptionFile + ".%s" % hostname))
                if int(status) == 0:
                    break;
    return """code：%s""" % (code)



#####################################################腾讯###################################################################
#处理不分页-腾讯，便于速度控制
@app.task(rate_limit='1000/m')
def get_not_page_tc(UrlPath="",ParamJson="",ServiceCode="",Token="",ReturnAccountId="",TaskFlag="",DataFileDir="",DataFile="",TaskExceptionFile="",ArrayFlag="",TargetFlag="oe"):
    set_true = True
    n = 0
    code = 9999
    while set_true:
      code = set_not_page(UrlPath=UrlPath,ParamJson=ParamJson,ServiceCode=ServiceCode,Token=Token
                          ,DataFileDir=DataFileDir,DataFile=DataFile,ReturnAccountId=ReturnAccountId,ArrayFlag=ArrayFlag,TargetFlag=TargetFlag)
      if TargetFlag == "tc":
          sucess_code=[ int(x) for x in conf.get("Tc_Code", "sucess_code").split(",")]
      else:
          sucess_code=[ int(x) for x in conf.get("Oe_Code", "sucess_code").split(",")]
      if int(code) in sucess_code:
          set_true = False
      else:
          if n > 2:
            print("处理不分页异常：%s,%s"%(ReturnAccountId,ServiceCode))
            for i in range(100):
              status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ",""),ServiceCode,str(ReturnAccountId).replace(" ",""), TaskFlag,Token,int(code), TaskExceptionFile + ".%s" % hostname))
              if int(status) == 0:
                  break;
            set_true = False
          else:
            time.sleep(5)
      n = n + 1
    return """code：%s""" % (code)

#处理分页-腾讯，便于速度控制
@app.task(rate_limit='1000/m')
def get_pages_tc(UrlPath="",ParamJson="",ServiceCode="",Token="",DataFileDir=""
              ,DataFile="",ReturnAccountId="",TaskFlag="",PageTaskFile="",TaskExceptionFile="",Pagestyle="",ArrayFlag="",TargetFlag="oe"):
    set_true = True
    n = 0
    code = 9999
    while set_true:
      time.sleep(30)
      code = set_pages(UrlPath=UrlPath,ParamJson=ParamJson,Token=Token,
                            ServiceCode=ServiceCode,DataFileDir=DataFileDir,
                            DataFile=DataFile,ReturnAccountId=ReturnAccountId,
                            TaskFlag=TaskFlag,PageTaskFile=PageTaskFile,Pagestyle=Pagestyle,ArrayFlag=ArrayFlag,TargetFlag=TargetFlag
                           )
      if TargetFlag == "tc":
          sucess_code=[ int(x) for x in conf.get("Tc_Code", "sucess_code").split(",")]
      else:
          sucess_code=[ int(x) for x in conf.get("Oe_Code", "sucess_code").split(",")]
      print(sucess_code)
      if int(code) in sucess_code:
          set_true = False
      else:
          if n > 2:
            print("异常分页：%s,%s"%(ReturnAccountId,ServiceCode))
            for i in range(100):
              status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""), TaskFlag,Token,int(code),TaskExceptionFile + ".%s" % hostname))
              if int(status) == 0:
                 break;
            set_true = False
          else:
            time.sleep(5)
      n = n + 1
    return """code：%s"""%(code)
#创建tc异步任务
@app.task(rate_limit='1000/m')
def get_tc_add_async_tasks(DataFileDir="",DataFile="",UrlPath="",ParamJson="",Token="",ReturnAccountId="",ServiceCode="",Level="",MediaType="",TaskExceptionFile="",TaskFlag=""):
    set_true = True
    n = 0
    code = 9999
    while set_true:
        code = set_tc_add_async_tasks(DataFileDir=DataFileDir, DataFile=DataFile, UrlPath=UrlPath, ParamJson=ParamJson, Token=Token, TaskFlag=TaskFlag,
                                         ReturnAccountId=ReturnAccountId, ServiceCode=ServiceCode, Level=Level, MediaType=MediaType)
        if int(code) == 0:
            set_true = False
        else:
            if n > 2:
                print("处理创建异步任务异常：%s,%s" % (ReturnAccountId, ServiceCode))
                status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % ( UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""), MediaType,Token,TaskFlag+"##"+Level, TaskExceptionFile + ".%s" % hostname))
                if int(status) != 0:
                    for i in range(100):
                        status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""),MediaType, Token,TaskFlag+"##"+Level, TaskExceptionFile + ".%s" % hostname))
                        if int(status) == 0:
                            break;
                set_true = False
            else:
                time.sleep(5)
        n = n + 1
    return """code：%s""" % (code)

#定义tc任务状态
@app.task(rate_limit='1000/m')
def get_tc_status_async_tasks(ExecDate="",DataFileDir="",DataFile="",UrlPath="",TaskId="",Token="",ReturnAccountId="",ServiceCode="",MediaType="",TaskFlag="",TaskExceptionFile=""):
    set_true = True
    n = 0
    code = 9999
    while set_true:
        code = set_tc_status_async_tasks(ExecDate=ExecDate,DataFileDir=DataFileDir,DataFile=DataFile,
                                         UrlPath=UrlPath,TaskId=TaskId,Token=Token,
                                         ReturnAccountId=ReturnAccountId,
                                         ServiceCode=ServiceCode,MediaType=MediaType,TaskFlag=TaskFlag)
        if int(code) == 0:
            set_true = False
        else:
            if n > 2:
                print("处理异步任务状态异常：%s,%s,%s" % (TaskFlag,ReturnAccountId, ServiceCode))
                status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(TaskId).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""), MediaType,Token, TaskFlag, TaskExceptionFile + ".%s" % hostname))
                if int(status) != 0:
                    for i in range(100):
                        status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(TaskId).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""),MediaType, Token, TaskFlag, TaskExceptionFile + ".%s" % hostname))
                        if int(status) == 0:
                            break;
                ####resp_data = """%s %s %s %s %s %s %s %s %s""" % (ExecDate, ReturnAccountId, MediaType, ServiceCode, Token, "9999", "执行异常", TaskFlag, "9999")
                ####status = os.system("""echo "%s">>%s/%s """ % (resp_data, DataFileDir, DataFile+"_exception.%s"%(hostname)))
                ####if int(status) != 0:
                ####    for i in range(100):
                ####        status = os.system("""echo "%s">>%s/%s """ % (resp_data, DataFileDir, DataFile+"_exception.%s"%(hostname)))
                ####        if int(status) == 0:
                ####            break;
                set_true = False
            else:
                time.sleep(5)
        n = n + 1
    return """code：%s""" % (code)

#定义tc异步任务数据
@app.task(rate_limit='1000/m')
def get_tc_async_tasks_data_return(DataFileDir="",DataFile="",UrlPath="",ParamJson="",Token="",ReturnAccountId="",ServiceCode="",TaskFlag="",TaskExceptionFile=""):
    print("执行数据子账户：%s"%(ReturnAccountId))
    set_true = True
    n = 0
    code = 9999
    while set_true:
        code = set_tc_async_tasks_data_return(DataFileDir=DataFileDir,DataFile=DataFile,UrlPath=UrlPath,ParamJson=ParamJson,Token=Token,ReturnAccountId=ReturnAccountId,ServiceCode=ServiceCode)
        if int(code) == 0:
            set_true = False
        else:
            if n > 2:
                print("异常处理异步数据子账户：%s,%s" % (ReturnAccountId, ServiceCode))
                status = os.system("""echo "%s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""), TaskFlag,Token, TaskExceptionFile + ".%s" % hostname))
                if int(status) != 0:
                    for i in range(100):
                        status = os.system("""echo "%s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""),TaskFlag, Token, TaskExceptionFile + ".%s" % hostname))
                        if int(status) == 0:
                            break;
                set_true = False
            else:
                time.sleep(5)
        n = n + 1
    return """code：%s""" % (code)
