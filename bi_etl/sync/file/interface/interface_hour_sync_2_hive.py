# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: interface_oe_async_2_hive.py
# @Software: PyCharm
# function info：定义oe异步接口

from celery.result import AsyncResult
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_alert_info_d
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.common.base.airflow_instance import Airflow
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_service_page_data_hour as get_service_page_data_celery_hour
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_not_page_hour as get_not_page_celery_hour
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_pages_hour as get_pages_celery_hour
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_service_data_hour as get_service_data_celery_hour
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_local_hdfs_thread
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_data_2_ods
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_data_2_snap
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.get_account_tokens import get_oe_account_token
from ecsage_bigdata_etl_engineering.common.base.get_config import Conf

import os
import time
import json
import ast
import socket
import pandas as pd
import pymysql
conf = Conf().conf
etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
interface_data_dir = conf.get("Interface", "oe_interface_data_home")
oe_celery_works_hostnames = eval(conf.get("Interface", "oe_celery_works_hostname"))

#入口方法
def main(TaskInfo,Level="",**kwargs):
    global airflow
    global developer
    global regexp_extract_column
    airflow = Airflow(kwargs)
    print(TaskInfo,"####################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
#    exec_date = airflow.execution_date_utc8_str[0:10]
    exec_date = time.strftime("%Y-%m-%d", time.localtime())
    target_db = TaskInfo[14]
    target_table = TaskInfo[15]
    source_db = TaskInfo[11]
    source_table = TaskInfo[12]
    is_report = TaskInfo[18]
    key_columns = TaskInfo[19]
    array_flag = TaskInfo[28]
    custom_set_parameter = TaskInfo[37]
    orderby_columns = TaskInfo[39]
    beeline_session = set_db_session(SessionType="beeline", SessionHandler="beeline")
    if Level == "file":
       if TaskInfo[0] == "metadb_oe_service_account_hour":
          #删除celery日志
          etl_md.execute_sql("truncate table sync.celery_taskmeta")
          get_service_info(AirflowDag=airflow.dag,AirflowTask=airflow.task,TaskInfo=TaskInfo,ExecDate=exec_date)
       else:
          get_data_2_etl_mid(BeelineSession=beeline_session, TargetDB=target_db, TargetTable=target_table,
                             AirflowDag=airflow.dag, AirflowTask=airflow.task,
                             TaskInfo=TaskInfo, ExecDate=exec_date,ArrayFlag=array_flag
                            )
    elif Level == "ods":
        hive_session = set_db_session(SessionType="hive", SessionHandler="hive")
        get_data_2_ods(HiveSession=hive_session,BeelineSession=beeline_session,SourceDB=source_db,
                       SourceTable=source_table,TargetDB=target_db,TargetTable=target_table,
                       ExecDate=exec_date,ArrayFlag=array_flag,KeyColumns=key_columns,IsReplace="N",DagId=airflow.dag,TaskId=airflow.task,CustomSetParameter=custom_set_parameter,OrderbyColumns=orderby_columns)
    elif Level == "snap":
        hive_session = set_db_session(SessionType="hive", SessionHandler="hive")
        get_data_2_snap(HiveSession=hive_session, BeelineSession=beeline_session, SourceDB=source_db, SourceTable=source_table,
                             TargetDB=target_db, TargetTable=target_table, IsReport=is_report
                            ,KeyColumns=key_columns, ExecDate=exec_date
                            ,DagId=airflow.dag,TaskId=airflow.task)

def get_data_2_etl_mid(BeelineSession="",TargetDB="",TargetTable="",AirflowDag="",AirflowTask="",TaskInfo="",ExecDate="",ArrayFlag=""):
  task_flag = "%s.%s"%(AirflowDag,AirflowTask)
  local_time = time.strftime("%Y-%m-%d_%H_%M_%S", time.localtime())
  now_time = time.strftime("%Y-%m-%d", time.localtime())
  #now_time = "2021-04-18"
  hostname = socket.gethostname()
  local_dir = """%s/%s/sync/%s/%s/%s"""%(interface_data_dir,hostname,ExecDate,AirflowDag,AirflowTask)
  celery_first_page_status_file = "%s/celery_first_page_status_file.log"%(local_dir)
  celery_other_page_status_file = "%s/celery_other_page_status_file.log" % (local_dir)
  celery_rerun_page_status_file = "%s/celery_rerun_page_status_file.log" % (local_dir)
  first_page_task_file = "%s/first_page_task_file.log"%(local_dir)
  other_page_task_file = "%s/other_page_task_file.log" % (local_dir)
  rerun_page_task_file = "%s/rerun_page_task_file.log" % (local_dir)
  data_task_file = """%s/data_%s.log"""%(local_dir,AirflowTask)
  tmp_data_task_file = """%s/tmp_data_file.log""" % (local_dir)
  first_task_exception_file = "%s/first_task_exception_file.log"%(local_dir)
  other_task_exception_file = "%s/other_task_exception_file.log" % (local_dir)
  rerun_task_exception_file = "%s/rerun_task_exception_file.log" % (local_dir)
  data_file = data_task_file.split("/")[-1].split(".")[0] + "_1_%s." % (local_time) + data_task_file.split("/")[-1].split(".")[1]
  param_json = ast.literal_eval(json.loads(json.dumps(TaskInfo[5])))
  #设置查询日期
  if TaskInfo[6] is not None and len(TaskInfo[6]) > 0 and TaskInfo[6] != "":
     param_json["%s"%(TaskInfo[6])] = now_time
     param_json["%s"%(TaskInfo[7])] = now_time
  #设置查询filter_modify_time_name
  if TaskInfo[8] is not None and len(TaskInfo[8]) > 0 and TaskInfo[8] != "":
     for filter_date in TaskInfo[8].split(","):
        param_json["filtering"]["%s" % (filter_date)] = ExecDate
  url_path = TaskInfo[4]
  filter_db_name = TaskInfo[21]
  filter_table_name = TaskInfo[22]
  filter_column_name = TaskInfo[23]
  filter_config = TaskInfo[24]
  is_report = TaskInfo[18]
  is_page = TaskInfo[25]
  media_type = TaskInfo[26]
  is_advertiser_list = TaskInfo[27]
  filter_time = TaskInfo[29]
  interface_filter_list = TaskInfo[30]
  page_size = TaskInfo[31]
  is_rerun_firstpage = TaskInfo[32]
  customize_sql = TaskInfo[40]
  page_style =eval(TaskInfo[38]) if TaskInfo[38] is not None and len(TaskInfo[38]) >0 else TaskInfo[38]
  if page_size is None or len(str(page_size)) == 0 or page_size == 0:
    page_size = 1000
  filter_time_sql = ""
  if filter_time is not None and len(filter_time) > 0:
      filter_time_sql = """ and %s >= '%s 00:00:00' and %s <= '%s 23:59:59' """%(filter_time,ExecDate,filter_time,ExecDate)
  os.system("""mkdir -p %s"""%(local_dir))
  os.system("""chmod -R 777 %s""" % (local_dir))
  os.system("""rm -f %s/*"""%(local_dir))
  ##删除数据文件
  for oe_celery_works_hostname in oe_celery_works_hostnames:
    os.system("mkdir -p %s"%(local_dir.replace("ecsage_data","ecsage_data_%s"%oe_celery_works_hostname)))
    os.system("""chmod -R 777 %s""" % (local_dir.replace("ecsage_data","ecsage_data_%s"%oe_celery_works_hostname)))
    os.system("""rm -f %s/*""" % (local_dir.replace("ecsage_data","ecsage_data_%s"%oe_celery_works_hostname)))

  etl_md.execute_sql("""delete from metadb.celery_sync_status where task_id='%s' """%(task_flag))

  conn1 = pymysql.connect(host='192.168.30.186', port=3306, user='ecdc', passwd='y8#90d#s7f66a',
                          db='ec_cloud_crm', charset='utf8')
  conn2 = pymysql.connect(host='192.168.30.5', port=13306, user='root', passwd='06D567130266EB33098B9F',
                          db='metadb', charset='utf8')
  sql1 = '''
      select distinct t1.advertiser_id as account_id, t3.service_code
      from ec_cloud_crm.advertiser t1
           left join demons_media.media_account t2 on t1.advertiser_id = t2.account_id
           inner join demons_media.media_service_provider t3 on t2.service_provider_id = t3.id
      where t1.customer_id = 10484
      '''
  sql2 = '''
      select account_id, media_type, service_code, account_id as id, '%s' as flag,token
      from oe_service_account
  '''%(task_flag)
  account_df = pd.read_sql(sql1, con=conn1)

  token_df = pd.read_sql(sql2, con=conn2)
  res_df = pd.merge(token_df, account_df, how='inner', on=['service_code', 'account_id'])
  db_data = res_df.values.tolist()
  #处理翻页
  if int(is_page) == 1:
    print("处理分页逻辑！！！")
    etl_md.execute_sql("delete from metadb.oe_sync_page_interface where flag = '%s' " % (task_flag))
    set_first_page_info(IsRerun="N",DataRows=db_data, UrlPath=url_path, ParamJson=param_json,InterfaceFilterList=interface_filter_list,
                        DataFileDir=local_dir, DataFile=data_file, TaskExceptionFile=first_task_exception_file,
                        PageTaskFile=first_page_task_file, CeleryPageStatusFile=celery_first_page_status_file,TaskFlag=task_flag,
                        Page=1,PageSize=page_size,Pagestyle=page_style,ArrayFlag=ArrayFlag
                        )
    if int(is_rerun_firstpage) == 1:
      # 重试页数为0
      n = 3
      for i in range(n):
          sql = """
                select account_id, '222' media_type, service_code,request_filter,flag,token
                from metadb.oe_sync_page_interface a
                where page_num = 0
                  and remark = '正常'
                  and data like '%s'
                  and flag = '%s'
               group by account_id, service_code,request_filter,request_filter,flag,token
               """ % ("%OK%", task_flag)
          ok, db_data = etl_md.get_all_rows(sql)
          if db_data is not None and len(db_data) > 0:
              os.system("""rm -f %s*""" % (celery_rerun_page_status_file.split(".")[0]))
              os.system("""rm -f %s*""" % (rerun_page_task_file.split(".")[0]))
              os.system("""rm -f %s*""" % (rerun_task_exception_file.split(".")[0]))
              etl_md.execute_sql("""delete from metadb.celery_sync_status where task_id='%s' """%(task_flag))
              set_first_page_info(IsRerun="Y",DataRows=db_data, UrlPath=url_path,DataFileDir=local_dir,InterfaceFilterList=interface_filter_list,
                                  DataFile=data_file, TaskExceptionFile=rerun_task_exception_file,PageTaskFile=rerun_page_task_file,
                                  CeleryPageStatusFile=celery_rerun_page_status_file,TaskFlag=task_flag, Page=1, PageSize=page_size,
                                  Pagestyle=page_style,ArrayFlag=ArrayFlag
                                  )
              ok, db_data = etl_md.get_all_rows(sql)
              if db_data is not None and len(db_data) > 0:
                  time.sleep(10)
              else:
                  break
    #处理其它分页
    sql = """
        select a.account_id, a.media_type as media_type, a.service_code,a.page_num,a.request_filter,a.token
        from metadb.oe_sync_page_interface a 
        where page_num > 1
          and flag = '%s'
        group by a.account_id,  a.service_code,a.page_num,a.request_filter,a.media_type,a.token
    """ % (task_flag)
    ok, db_data = etl_md.get_all_rows(sql)
    if db_data is not None and len(db_data) > 0:
       etl_md.execute_sql("""delete from metadb.celery_sync_status where task_id='%s' """%(task_flag))
       set_other_page_info(DataRows=db_data, UrlPath=url_path, DataFileDir=local_dir,InterfaceFilterList=interface_filter_list,
                           DataFile=data_file, TaskExceptionFile=other_task_exception_file,PageTaskFile=other_page_task_file,
                           CeleryPageStatusFile=celery_other_page_status_file, TaskFlag=task_flag, PageSize=page_size,Pagestyle=page_style
                           ,ArrayFlag=ArrayFlag
                           )
  else:
    #不分页
    set_not_page_info(DataRows=db_data, UrlPath=url_path, ParamJson=param_json, DataFileDir=local_dir,InterfaceFilterList=interface_filter_list,
                      DataFile=data_file, TaskExceptionFile=other_task_exception_file,TaskFlag=task_flag,
                      IsAdvertiserList=is_advertiser_list, CeleryPageStatusFile=celery_other_page_status_file,
                      ArrayFlag=ArrayFlag)
  #获取数据文件
  data_task_file_list = []
  for oe_celery_works_hostname in oe_celery_works_hostnames:
    if os.path.exists(local_dir.replace("ecsage_data", "ecsage_data_%s" % oe_celery_works_hostname)):
      target_file = os.listdir(local_dir.replace("ecsage_data", "ecsage_data_%s" % oe_celery_works_hostname))
      for files in target_file:
          if str(data_task_file.split("/")[-1]).split(".")[0] in files and '.lock' not in files:
              data_task_file_list.append("%s/%s"%(local_dir.replace("ecsage_data", "ecsage_data_%s" % oe_celery_works_hostname), files))
  #数据落地至etl_mid
  load_data_2_etl_mid(BeelineSession=BeelineSession, LocalFileList=data_task_file_list, TargetDB=TargetDB,
                      TargetTable=TargetTable, ExecDate=ExecDate,MediaType=media_type)

#处理不分页
def set_not_page_info(DataRows="",UrlPath="",ParamJson="",DataFileDir="",DataFile="",
                      TaskExceptionFile="",IsAdvertiserList="",CeleryPageStatusFile="",
                      TaskFlag="",InterfaceFilterList="",ArrayFlag=""):
    for data in DataRows:
       if InterfaceFilterList is not None and len(InterfaceFilterList) > 0:
          filter_list = InterfaceFilterList.split(",")
          for lists in filter_list:
              get_list = lists.split(".")
          if len(get_list) == 1:
             list_value = get_list[0].split("##")#campaign_ids##[]##int
             ParamJson["%s" % (list_value[0])] = [eval(list_value[2])(data[3])] if list_value[1] == '[]' else eval(list_value[2])(data[3])
             #ParamJson["%s"%(list_value[0])] = int(data[3])
             #print(ParamJson)
          else:
             print("含有filter...")
       if int(IsAdvertiserList) == 1:
           ParamJson["advertiser_ids"] = [int(data[0])]
       else:
           ParamJson["advertiser_id"] = int(data[0])
       celery_task_id = get_not_page_celery_hour.delay(UrlPath=UrlPath, ParamJson=ParamJson,Token=data[5],
                                                  ServiceCode=data[2], ReturnAccountId=data[0],
                                                  TaskFlag=TaskFlag,DataFileDir=DataFileDir,
                                                  DataFile=DataFile, TaskExceptionFile=TaskExceptionFile
                                                  , ArrayFlag=ArrayFlag
                                                  )
       os.system("""echo "%s %s %s">>%s""" % (celery_task_id, data[0], data[2], CeleryPageStatusFile))
    if DataRows is not None and len(DataRows) > 0:
       # 获取状态
       print("总请求数：%s，正在等待celery队列执行完成！！！"%(len(DataRows)))
       #celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=CeleryPageStatusFile)
       #wait_for_celery_status(StatusList=celery_task_id,RequestRows=len(DataRows),TaskFlag=TaskFlag)
       wait_for_celery_status(StatusList=[], RequestRows=len(DataRows), TaskFlag=TaskFlag)
       print("celery队列执行完成！！！%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
       #重试异常
       rerun_exception_tasks_pages(DataFileDir=DataFileDir, ExceptionFile=TaskExceptionFile, IsPage="N",
                                   DataFile=DataFile, PageTaskFile="/tmp/loglog.log", CeleryTaskDataFile=CeleryPageStatusFile,
                                   InterfaceFlag=TaskFlag,
                                   Columns="interface_url,interface_param_json,service_code,account_id,interface_flag,token",
                                   ArrayFlag=ArrayFlag
                                   )

#处理首页
def set_first_page_info(IsRerun="",DataRows="",UrlPath="",ParamJson="",DataFileDir="",DataFile="",TaskExceptionFile=""
                        ,PageTaskFile="",CeleryPageStatusFile="",TaskFlag="",Page="",PageSize="",InterfaceFilterList="",
                        Pagestyle="",ArrayFlag=""):
    for data in DataRows:
       if IsRerun != "Y":
         if InterfaceFilterList is not None and len(InterfaceFilterList) > 0:
            filter_list = InterfaceFilterList.split(",")
            for lists in filter_list:
                get_list = lists.split(".")
            if len(get_list) == 1:
                list_value = get_list[0].split("##")  # campaign_ids##[]##int
                ParamJson["%s" % (list_value[0])] = [eval(list_value[2])(data[3])] if list_value[1] == '[]' else eval(list_value[2])(data[3])
                # ParamJson["%s"%(list_value[0])] = int(data[3])
            else:
                list_1 = get_list[0]
                list_value = get_list[1].split("##")
                list_value_1 = list_value[0]
                list_value_2 = list_value[1]
                list_value_3 = list_value[2]
                if list_value_2 == "[]" and list_value_3 == "int":
                   ParamJson["%s" % (list_1)]["%s" % (list_value_1)] = [int(data[3])]
                elif list_value_2 == "[]" and list_value_3 == "string":
                   ParamJson["%s" % (list_1)]["%s" % (list_value_1)] = [str(data[3])]
                elif list_value_2 != "[]" and list_value_3 == "string":
                   ParamJson["%s" % (list_1)]["%s" % (list_value_1)] = str(data[3])
                elif list_value_2 != "[]" and list_value_3 == "int":
                   ParamJson["%s" % (list_1)]["%s" % (list_value_1)] = int(data[3])
                else:
                   set_exit("red","请输入正确参数！！！")
       else:
         ParamJson = ast.literal_eval(json.loads(json.dumps(str(data[3]).replace("""'""", """\""""))))
       ParamJson["advertiser_id"] = data[0]
       if Pagestyle is not None and len(Pagestyle)>0:#page_style=[{"offset":0,"limit":100},"offset","limit"]
           ParamJson.update(Pagestyle[0])
       else:
           ParamJson["page"] = int(Page)
           ParamJson["page_size"] = int(PageSize)

       service_code = data[2]
       token = data[5]
       celery_task_id = get_pages_celery_hour.delay(UrlPath=UrlPath,ParamJson=ParamJson,ServiceCode=service_code,
                                               DataFileDir=DataFileDir,DataFile=DataFile,ReturnAccountId=data[0],
                                               TaskFlag=TaskFlag,PageTaskFile=PageTaskFile,TaskExceptionFile=TaskExceptionFile,
                                               Token=token,Pagestyle=Pagestyle,ArrayFlag=ArrayFlag
                                               )
       os.system("""echo "%s %s %s">>%s""" % (celery_task_id, data[0], data[2], CeleryPageStatusFile))
    if DataRows is not None and len(DataRows)>0:
       if "etl_mid_oe_set_insert_sync_account" not in TaskFlag:
          # 获取状态
          celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=CeleryPageStatusFile)
          print("总请求数：%s，正在等待获取页数celery队列执行完成！！！"%(len(DataRows)))
          #wait_for_celery_status(StatusList=celery_task_id,RequestRows=len(DataRows),TaskFlag=TaskFlag)
          wait_for_celery_status(StatusList=celery_task_id, RequestRows=len(DataRows), TaskFlag=TaskFlag)
          print("获取页数celery队列执行完成！！！")
          print("end %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
       else:
           print("总请求数：%s，正在等待获取页数celery队列执行完成！！！" % (len(DataRows)))
           # wait_for_celery_status(StatusList=celery_task_id,RequestRows=len(DataRows),TaskFlag=TaskFlag)
           wait_for_celery_status(StatusList=[], RequestRows=len(DataRows), TaskFlag=TaskFlag)
           print("获取页数celery队列执行完成！！！")
           print("end %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
       #重试异常
       rerun_exception_tasks_pages(DataFileDir=DataFileDir,ExceptionFile=TaskExceptionFile,IsPage="Y",
                                   DataFile=DataFile,PageTaskFile=PageTaskFile,CeleryTaskDataFile=CeleryPageStatusFile,
                                   InterfaceFlag=TaskFlag,Columns="interface_url,interface_param_json,service_code,account_id,interface_flag,token"
                                   ,ArrayFlag=ArrayFlag
                                  )
       # 保存MySQL
       columns = """page_num,account_id,service_code,remark,data,request_filter,flag,token"""
       load_data_mysql(AsyncAccountFile=DataFileDir, DataFile=PageTaskFile, DbName="metadb",
                       TableName="oe_sync_page_interface", Columns=columns)
    else:
       print("没有对应请求url！！！")

#处理其它分页
def set_other_page_info(DataRows="",UrlPath="",DataFileDir="",DataFile="",
                        TaskExceptionFile="",PageTaskFile="",CeleryPageStatusFile="",
                        TaskFlag="",PageSize="",InterfaceFilterList="",Pagestyle="",
                        ArrayFlag=""
                       ):
    n = 0
    for data in DataRows:
      page_number = int(data[3])
      for page in range(page_number):
        if page > 0:
           n = n + 1
           param_json = ast.literal_eval(json.loads(json.dumps(str(data[4]).replace("""'""", """\""""))))
           if Pagestyle is not None and len(Pagestyle) > 0:
               tmp_offset = page * Pagestyle[0][Pagestyle[2]] #Pagestyle=[{"offset":0,"limit":100},"offset","limit"]
               update_offset = {Pagestyle[1]:tmp_offset}#{"offset":page * offset}
               param_json.update(update_offset)
           else:
               pages = page + 1
               param_json["page"] = int(pages)
               param_json["page_size"] = int(PageSize)
           service_code = data[2]
           token = data[5]
           celery_task_id = get_pages_celery_hour.delay(UrlPath=UrlPath,ParamJson=param_json,ServiceCode=service_code,
                                                   DataFileDir=DataFileDir,DataFile=DataFile,ReturnAccountId=data[0],
                                                   TaskFlag=TaskFlag,PageTaskFile=PageTaskFile,TaskExceptionFile=TaskExceptionFile,
                                                   Token=token,Pagestyle=Pagestyle,ArrayFlag=ArrayFlag
                                                   )
           os.system("""echo "%s %s %s">>%s""" % (celery_task_id, data[0], data[2], CeleryPageStatusFile))
    if n > 0:
       # 获取状态
       celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=CeleryPageStatusFile)
       print("请求总页数：%s，正在等待获取页数celery队列执行完成！！！"%(n))
       #wait_for_celery_status(StatusList=celery_task_id,RequestRows=n,TaskFlag=TaskFlag)
       wait_for_celery_status(StatusList=celery_task_id, RequestRows=n, TaskFlag=TaskFlag)
       print("获取页数celery队列执行完成！！！")
       print("end %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
       #重试异常
       rerun_exception_tasks_pages(DataFileDir=DataFileDir,ExceptionFile=TaskExceptionFile,IsPage="Y",
                                   DataFile=DataFile,PageTaskFile=PageTaskFile,CeleryTaskDataFile=CeleryPageStatusFile,
                                   InterfaceFlag=TaskFlag,Columns="interface_url,interface_param_json,service_code,account_id,interface_flag,token"
                                   ,ArrayFlag=ArrayFlag
                                 )
    else:
       print("请求总页数：%s，正在等待获取页数celery队列执行完成！！！" % (n))

def get_service_page(DataRows="",LocalDir="",DataFile="",PageFileData="",TaskFlag="",CeleryGetDataStatus="",Page="",PageSize=""):
    for data in DataRows:
        celery_task_id = get_service_page_data_celery_hour.delay(ServiceId=data[0], ServiceCode=data[1],
                                                       Media=data[2], Page=str(Page), PageSize=str(PageSize),
                                                       DataFile=DataFile, PageFileData=PageFileData,
                                                       TaskFlag=TaskFlag
                                                       )
        os.system("""echo "%s %s %s %s ">>%s""" % (celery_task_id, data[0], data[1], data[2], CeleryGetDataStatus))
    # 获取状态
    celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=CeleryGetDataStatus)
    print("总请求数：%s，正在等待获取页数celery队列执行完成！！！"%(len(DataRows)))
    wait_for_celery_status(StatusList=celery_task_id,RequestRows=len(DataRows),TaskFlag=TaskFlag)
    print("获取页数celery队列执行完成！！！")
    print("end %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    # 保存MySQL
    columns = """page_num,account_id,service_code,remark,data,request_filter,flag,media_type"""
    load_data_mysql(AsyncAccountFile=LocalDir, DataFile=PageFileData, DbName="metadb",
                    TableName="oe_sync_page_interface", Columns=columns)

def get_service_info(AirflowDag="",AirflowTask="",TaskInfo="",ExecDate=""):
  task_flag = "%s.%s"%(AirflowDag,AirflowTask)
  local_time = time.strftime("%Y-%m-%d_%H_%M_%S", time.localtime())
  hostname = socket.gethostname()
  local_dir = """%s/%s/sync/%s/%s/%s"""%(interface_data_dir,hostname,ExecDate,AirflowDag,AirflowTask)
  celery_get_page_status = """%s/celery_get_page_status.log"""%(local_dir)
  celery_get_data_status = "%s/celery_get_data_status.log"%(local_dir)
  page_task_file = "%s/page_task_file.log"%(local_dir)
  data_task_file = """%s/data_task_file.log"""%(local_dir)
  tmp_data_task_file = """%s/tmp_data_file.log""" % (local_dir)
  task_exception_file = "%s/task_exception_file.log"%(local_dir)
  data_file = local_dir + "/" + data_task_file.split("/")[-1].split(".")[0] + "_1_%s." % (local_time) + data_task_file.split("/")[-1].split(".")[1]
  os.system("""mkdir -p %s"""%(local_dir))
  os.system("""rm -f %s/*"""%(local_dir))
  is_filter = False

  mysql_session = set_db_session(SessionType="mysql", SessionHandler="mysql_media")
  get_service_code_sql = """select account_id,service_code,media
                            from big_data_mdg.media_service_provider
                            where media = 2
                          """
  ok, all_rows = mysql_session.get_all_rows(get_service_code_sql)
  etl_md.execute_sql("delete from metadb.oe_sync_page_interface where flag = '%s' " % (task_flag))
  get_service_page(DataRows=all_rows, LocalDir=local_dir, DataFile=data_file,
                   PageFileData=page_task_file, TaskFlag=task_flag, CeleryGetDataStatus=celery_get_page_status,
                   Page="1",PageSize="1000")
  #重试异常
  n = 10
  for i in range(n):
    sql = """
      select tmp1.account_id, tmp1.media_type, tmp1.service_code,trim(replace(replace(tmp1.request_filter,'[',''),']','')),tmp1.flag
   from(select account_id,service_code,request_filter,count(distinct remark) as rn
        from metadb.oe_sync_page_interface
        where flag = '%s.%s'
        group by account_id,service_code,request_filter
        having count(distinct remark) = 1
       ) tmp
   inner join metadb.oe_sync_page_interface tmp1
   on tmp.account_id = tmp1.account_id
   and tmp.service_code = tmp1.service_code
   and tmp.request_filter = tmp1.request_filter
   where tmp1.remark = '异常'
     and tmp1.flag = '%s.%s'
   group by tmp1.account_id, tmp1.service_code,tmp1.request_filter,tmp1.request_filter,tmp1.flag,tmp1.media_type
  """%(AirflowDag,AirflowTask,AirflowDag,AirflowTask)
    ok, db_data = etl_md.get_all_rows(sql)
    if db_data is not None and len(db_data) > 0:
       os.system("""rm -f %s*""" % (celery_get_page_status.split(".")[0]))
       os.system("""rm -f %s*""" % (page_task_file.split(".")[0]))
       os.system("""rm -f %s*""" % (celery_get_data_status.split(".")[0]))
       os.system("""rm -f %s*""" % (task_exception_file.split(".")[0]))
       etl_md.execute_sql("""delete from metadb.celery_sync_status where task_id='%s' """%(task_flag))
       get_service_page(DataRows=db_data, LocalDir=local_dir, DataFile=data_file,
                        PageFileData=page_task_file, TaskFlag=task_flag, CeleryGetDataStatus=celery_get_page_status+"rerun",
                        Page="1", PageSize="1000")
       ok, db_data = etl_md.get_all_rows(sql)
       if db_data is not None and len(db_data) > 0:
         time.sleep(60)
       else:
          break

  sql = """
    select a.account_id, a.media_type as media_type, a.service_code,a.page_num,a.request_filter
    from metadb.oe_sync_page_interface a where page_num > 1
    and flag = '%s.%s'
    group by a.account_id,  a.service_code,a.page_num,a.request_filter,a.media_type
  """%(AirflowDag,AirflowTask)
  ok, datas = etl_md.get_all_rows(sql)
  n = 0
  if datas is not None and len(datas) > 0:
     etl_md.execute_sql("""delete from metadb.celery_sync_status where task_id='%s' """%(task_flag))
     for dt in datas:
        page_number = int(dt[3])
        for page in range(page_number):
         if page > 0:
           n = n + 1
           pages = page + 1
           celery_task_id = get_service_data_celery_hour.delay(ServiceId=dt[0], ServiceCode=dt[2],
                                                          Media=dt[1], Page=str(pages), PageSize=str(1000),
                                                          DataFile=data_file, PageFileData=page_task_file,
                                                          TaskFlag=task_flag,TaskExceptionFile=task_exception_file
                                                        )
           os.system("""echo "%s %s %s %s ">>%s""" % (celery_task_id, dt[0], dt[1], dt[2], celery_get_data_status))
     # 获取状态
     print("总请求数：%s，正在等待celery队列执行完成！！！"%(n))
     celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_get_data_status)
     wait_for_celery_status(StatusList=celery_task_id,RequestRows=n,TaskFlag=task_flag)
     print("celery队列执行完成！！！%s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
     print("正在等待获取重试异常执行完成！！！")
     rerun_service_exception_tasks(AsyncAccountDir=local_dir, ExceptionFile=task_exception_file,
                                   DataFile=data_file, CeleryTaskDataFile=celery_get_data_status,
                                   InterfaceFlag=task_flag, ExecDate=ExecDate,
                                   Columns="""account_id,service_code,interface_flag,media,page,page_size"""
                                   )
     print("获取重试异常执行完成！！！")
     #写入MySQL
     etl_md.execute_sql("delete from metadb.oe_service_account ")
     #加载201、203数据
     sql = """
       select concat_ws(' ',b.service_id,a.service_code,a.account_id,a.media)
       from big_data_mdg.media_advertiser a
       left join (select account_id as service_id,service_code 
                  from big_data_mdg.media_service_provider
                  where media in (201,203)
                  group by account_id,service_code
              ) b
       on a.service_code = b.service_code
       where a.media in (201,203)
     """
     ok = mysql_session.select_data_to_local_file(sql=sql,filename=data_file)
     if ok is False:
         msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                                SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                                TargetTable="%s.%s" % ("TargetDB", "TargetTable"),
                                BeginExecDate=ExecDate,
                                EndExecDate=ExecDate,
                                Status="Error",
                                Log="获取201、203数据，mysql入库失败！！！",
                                Developer="developer")
         set_exit(LevelStatu="red", MSG=msg)
     columns = """service_id,service_code,account_id,media_type"""
     load_data_mysql(AsyncAccountFile=local_dir, DataFile=data_file, DbName="metadb",TableName="oe_service_account", Columns=columns)
     #获取token
     sql = """
        select  service_code
        from metadb.oe_service_account a
        group by service_code
     """
     ok,token_data = etl_md.get_all_rows(sql)
     for service_code in token_data:
        token = get_oe_account_token(ServiceCode=service_code[0])
        update_sql = """
         update metadb.oe_service_account set token='%s' where service_code = '%s'
        """%(token,service_code[0])
        etl_md.execute_sql(update_sql)

def load_data_2_etl_mid(BeelineSession="",LocalFileList="",TargetDB="",TargetTable="",ExecDate="",MediaType=""):
   if LocalFileList is None or len(LocalFileList) == 0:
      msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (TargetDB, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="API采集没执行！！！",
                               Developer="developer")
      set_exit(LevelStatu="yellow", MSG=msg)
   else:
    mid_sql = """
        create table if not exists %s.%s
        (
         request_data string
        )partitioned by(etl_date string,request_type string)
        row format delimited fields terminated by '\\001' 
        ;
    """ % (TargetDB,TargetTable)
    BeelineSession.execute_sql(mid_sql)
    load_num = 0
    hdfs_dir = conf.get("Airflow_New", "hdfs_home")
    load_table_sqls = ""
    load_table_sql_0 = ""
    load_table_sql = ""
    for data in LocalFileList:
        print(data,"####################################")
        local_file = """%s""" % (data)
        # 落地mid表
        if load_num == 0:
            load_table_sql_0 = """
                         load data inpath '{hdfs_dir}/{file_name}' OVERWRITE  INTO TABLE {target_db}.{target_table}
                         partition(etl_date='{exec_date}',request_type='{request_type}')
                         ;\n
            """.format(hdfs_dir=hdfs_dir, file_name=local_file.split("/")[-1], target_db=TargetDB,
                       target_table=TargetTable,exec_date=ExecDate,request_type=MediaType)
        else:
            load_table_sql = """
                         load data inpath '{hdfs_dir}/{file_name}' INTO TABLE {target_db}.{target_table}
                         partition(etl_date='{exec_date}',request_type='{request_type}')
                         ;\n
                     """.format(hdfs_dir=hdfs_dir, file_name=local_file.split("/")[-1],
                                target_db=TargetDB,target_table=TargetTable,exec_date=ExecDate,request_type=MediaType
                                )
        load_table_sqls = load_table_sql + load_table_sqls
        load_num = load_num + 1
    load_table_sqls = load_table_sql_0 + load_table_sqls
    # 上传hdfs
    get_local_hdfs_thread(TargetDb=TargetDB, TargetTable=TargetTable, ExecDate=ExecDate, DataFileList=LocalFileList,HDFSDir=hdfs_dir)
    print("结束上传HDFS，启动load")
    # 落地至hive
    ok_data = BeelineSession.execute_sql(load_table_sqls)
    if ok_data is False:
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (TargetDB, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="HDFS数据文件load入仓临时表出现异常！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)

def load_data_mysql(AsyncAccountFile="",DataFile="",DbName="",TableName="",Columns=""):
    target_file = os.listdir(AsyncAccountFile)
    for files in target_file:
        n = 0
        set_run = True
        if DataFile.split("/")[-1] in files:
            print(files, "###############################################")
            # 记录子账户
            insert_sql = """
                  load data local infile '%s' into table %s.%s fields terminated by ' ' lines terminated by '\\n' (%s)
               """ % (AsyncAccountFile + "/" + files,DbName,TableName,Columns)
            while set_run:
              ok = etl_md.local_file_to_mysql(sql=insert_sql)
              if ok is False:
                 if n > 3:
                   set_run = False
                   msg = "写入MySQL出现异常！！！\n%s" % (DataFile)
                   msg = get_alert_info_d(DagId="airflow.dag", TaskId="airflow.task",
                                      SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                                      TargetTable="%s.%s" % ("", ""),
                                      BeginExecDate="",
                                      EndExecDate="",
                                      Status="Error",
                                      Log=msg,
                                      Developer="developer")
                   set_exit(LevelStatu="red", MSG=msg)
              else:
                  set_run = False
              n = n+1

def get_celery_job_status(CeleryTaskId=""):
    set_task = AsyncResult(id=str(CeleryTaskId))
    status = set_task.status
    if status == "SUCCESS":
       return True
    if status == "FAILURE":
        msg = "celery队列执行失败！！！"
        msg = get_alert_info_d(DagId="airflow.dag", TaskId="airflow.task",
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % ("", ""),
                               BeginExecDate="",
                               EndExecDate="",
                               Status="Error",
                               Log=msg,
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)
    else:
       #print(CeleryTaskId,"##",status)
       return False

#
def get_celery_status_list(CeleryTaskStatusFile=""):
    celery_task_id = []
    status_wait = []
    with open(CeleryTaskStatusFile) as lines:
        array = lines.readlines()
        for data in array:
            get_data1 = data.strip('\n').split(" ")
            if get_celery_job_status(CeleryTaskId=get_data1[0]) is False:
                status_wait.append(get_data1[0])
                celery_task_id.append(get_data1[0])
    return celery_task_id,status_wait

def wait_for_celery_status(StatusList="",RequestRows="",TaskFlag=""):
    status_false = []
    run_wait = True
    sleep_num = 1
    while run_wait:
      # 判断请求个数是否与请求完成个数一致
      sql = """select count(1) from metadb.celery_sync_status where task_id = '%s' """ % (TaskFlag)
      ok, request_task_finish_rows = etl_md.get_all_rows(sql=sql)
      if ok:
          print("等待完成个数：【源数%s】【目标数%s】" % (int(RequestRows), int(request_task_finish_rows[0][0])))
          if int(RequestRows) == int(request_task_finish_rows[0][0]):
              print("完成！！！")
              run_wait = False
              break;
          elif int(RequestRows) < int(request_task_finish_rows[0][0]):
              msg = "celery队列执行失败！！！"
              msg = get_alert_info_d(DagId="airflow.dag", TaskId="airflow.task",
                                     SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                                     TargetTable="%s.%s" % ("", ""),
                                     BeginExecDate="",
                                     EndExecDate="",
                                     Status="Error",
                                     Log=msg,
                                     Developer="developer")
              set_exit(LevelStatu="red", MSG=msg)
      if StatusList is not None and len(StatusList) > 0:
          for status in StatusList:
            #判断是否成功
            if get_celery_job_status(CeleryTaskId=status) is False:
               status_false.append(status)
            else:
               pass
          if len(status_false) > 0:
              wait_mins = 600
              if sleep_num <= wait_mins:
                  min = 60
                  print("等待第%s次%s秒"%(sleep_num,min))
                  time.sleep(min)
              else:
                  msg = "等待celery队列完成超时！！！\n%s" % (status_false)
                  msg = get_alert_info_d(DagId="airflow.dag", TaskId="airflow.task",
                                         SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                                         TargetTable="%s.%s" % ("", ""),
                                         BeginExecDate="",
                                         EndExecDate="",
                                         Status="Error",
                                         Log=msg,
                                         Developer="developer")
                  set_exit(LevelStatu="red", MSG=msg)
          else:
              run_wait = False
          status_false.clear()
          sleep_num = sleep_num + 1
      if StatusList is None or len(StatusList) == 0:
         time.sleep(60)

#重试代理商
def rerun_service_exception_tasks(AsyncAccountDir="",ExceptionFile="",DataFile="",
                                  CeleryTaskDataFile="",InterfaceFlag="",ExecDate="",
                                  IsfilterID="",Columns=""):
    celery_task_data_file = """%s/%s"""%(AsyncAccountDir,CeleryTaskDataFile.split("/")[-1])
    #先保留第一次
    delete_sql = """delete from metadb.oe_sync_exception_tasks_interface where interface_flag = '%s' """ % (InterfaceFlag)
    etl_md.execute_sql(delete_sql)
    columns = Columns
    db_name = "metadb"
    table_name = "oe_sync_exception_tasks_interface"
    save_exception_tasks(AsyncAccountDir=AsyncAccountDir,ExceptionFile=ExceptionFile,DbName=db_name,TableName=table_name,Columns=columns)
    #
    n = 10
    for i in range(n):
        sql = """
          select distinct %s
          from %s.%s a
          where interface_flag = '%s' 
        """% (columns,db_name,table_name,InterfaceFlag)
        ok,datas = etl_md.get_all_rows(sql)
        if datas is not None and len(datas) > 0:
           etl_md.execute_sql("""delete from metadb.celery_sync_status where task_id='%s' """%(InterfaceFlag))
           print("开始第%s次重试异常，请求总数：%s，时间：%s"%(i+1,len(datas),time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
           for data in datas:
               status_id = get_service_data_celery_hour.delay(ServiceId=data[0], ServiceCode=data[1],
                                                         Media=data[3], Page=str(data[4]), PageSize=str(data[5]),
                                                         DataFile=DataFile, PageFileData="",
                                                         TaskFlag=InterfaceFlag, TaskExceptionFile=ExceptionFile
                                                        )
               os.system("""echo "%s %s">>%s""" % (status_id, data[0], celery_task_data_file+".%s"%(i)))
           celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_data_file + ".%s"%i)
           wait_for_celery_status(StatusList=celery_task_id,RequestRows=len(datas),TaskFlag=InterfaceFlag)
           delete_sql = """delete from %s.%s where interface_flag = '%s' """ % (db_name,table_name,InterfaceFlag)
           etl_md.execute_sql(delete_sql)
           save_exception_tasks(AsyncAccountDir=AsyncAccountDir, ExceptionFile=ExceptionFile, DbName = db_name,TableName=table_name,Columns=columns)
           print("结束第%s次重试异常，时间：%s" % (i + 1, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
           #判断结果是否还有异常
           ex_sql = """
                     select %s
                     from %s.%s a
                     where interface_flag = '%s'
                     limit 1
              """% (columns,db_name,table_name,InterfaceFlag)
           ok, ex_datas = etl_md.get_all_rows(ex_sql)
           if ex_datas is not None and len(ex_datas) > 0:
               print("休眠中...，时间：%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
               if i == 0:
                 time.sleep(360)
               else:
                 time.sleep(180)
    ex_sql = """
         select %s
         from %s.%s a
         where interface_flag = '%s'
    """% (columns,db_name,table_name,InterfaceFlag)
    ok, ex_datas = etl_md.get_all_rows(ex_sql)
    if ex_datas is not None and len(ex_datas) > 0:
        print("还有特别异常任务存在！！！")
        print(ex_datas[0])

def save_exception_tasks(AsyncAccountDir="",ExceptionFile="",DbName="",TableName="",Columns=""):
    exception_file = ExceptionFile.split("/")[-1]
    exception_file_list = []
    target_file = os.listdir(AsyncAccountDir)
    for files in target_file:
      if exception_file in files:
         exception_file_list.append((AsyncAccountDir, files))
    if exception_file_list is not None and len(exception_file_list) > 0 :
       for file in exception_file_list:
           print(file,"##################################")
           load_data_mysql(AsyncAccountFile=file[0], DataFile=file[1],DbName=DbName,TableName=TableName, Columns=Columns)
           status = os.system("""rm -f %s/%s"""%(file[0],file[1]))
           if status != 0:
              os.system("""rm -f %s/%s""" % (file[0], file[1]))

#分页异常重试
def rerun_exception_tasks_pages(DataFileDir="",ExceptionFile="",DataFile="",
                                PageTaskFile="",CeleryTaskDataFile="",InterfaceFlag="",
                                Columns="",IsPage="",Pagestyle="",ArrayFlag=""
                                ):
    celery_task_data_file = """%s/%s"""%(DataFileDir,CeleryTaskDataFile.split("/")[-1])
    #先保留第一次
    delete_sql = """delete from metadb.oe_sync_exception_tasks_interface_bak where interface_flag = '%s' """ % (InterfaceFlag)
    etl_md.execute_sql(delete_sql)
    columns = Columns
    db_name = "metadb"
    table_name = "oe_sync_exception_tasks_interface_bak"
    save_exception_tasks(AsyncAccountDir=DataFileDir,ExceptionFile=ExceptionFile,DbName=db_name,TableName=table_name,Columns=columns)
    #
    n = 50
    for i in range(n):
        sql = """
          select distinct %s
          from %s.%s a
          where interface_flag = '%s'
        """% (columns,db_name,table_name,InterfaceFlag)
        ok,datas = etl_md.get_all_rows(sql)
        if datas is not None and len(datas) > 0:
           etl_md.execute_sql("""delete from metadb.celery_sync_status where task_id='%s' """%(InterfaceFlag))
           print("开始第%s次重试异常，总请求数%s，时间：%s"%(i+1,len(datas),time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
           for data in datas:
             param_json = ast.literal_eval(json.loads(json.dumps(str(data[1]).replace("""'""","""\""""))))
             if IsPage == "Y":
                status_id = get_pages_celery_hour.delay(UrlPath=data[0],ParamJson=param_json,ServiceCode=data[2],Token=data[5],
                                                   DataFileDir=DataFileDir,DataFile=DataFile,ReturnAccountId=data[3],
                                                   TaskFlag=data[4],PageTaskFile=PageTaskFile,TaskExceptionFile=ExceptionFile,
                                                   Pagestyle=Pagestyle,ArrayFlag=ArrayFlag
                                                  )
             else:
                status_id = get_not_page_celery_hour.delay(UrlPath=data[0], ParamJson=param_json,Token=data[5],
                                                      ServiceCode=data[2], ReturnAccountId=data[3],
                                                      TaskFlag=data[4], DataFileDir=DataFileDir,
                                                      DataFile=DataFile, TaskExceptionFile=ExceptionFile,ArrayFlag=ArrayFlag
                                                     )
             os.system("""echo "%s %s">>%s""" % (status_id, data[0], celery_task_data_file+".%s"%(i)))
           celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_data_file + ".%s"%i)
           wait_for_celery_status(StatusList=celery_task_id,RequestRows=len(datas),TaskFlag=InterfaceFlag)
           delete_sql = """delete from %s.%s where interface_flag = '%s' """ % (db_name,table_name,InterfaceFlag)
           etl_md.execute_sql(delete_sql)
           save_exception_tasks(AsyncAccountDir=DataFileDir, ExceptionFile=ExceptionFile, DbName = db_name,TableName=table_name,Columns=columns)
           print("结束第%s次重试异常，时间：%s" % (i + 1, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
           #判断结果是否还有异常
           ex_sql = """
                     select %s
                     from %s.%s a
                     where interface_flag = '%s'
                     limit 1
              """% (columns,db_name,table_name,InterfaceFlag)
           ok, ex_datas = etl_md.get_all_rows(ex_sql)
           if ex_datas is not None and len(ex_datas) > 0:
               print("休眠中...，时间：%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
               if i == 0:
                 time.sleep(180)
               else:
                 time.sleep(120)
    ex_sql = """
         select %s
         from %s.%s a
         where interface_flag = '%s'
    """% (columns,db_name,table_name,InterfaceFlag)
    ok, ex_datas = etl_md.get_all_rows(ex_sql)
    if ex_datas is not None and len(ex_datas) > 0:
        print("还有特别异常任务存在！！！")
        print(ex_datas[0])

def celery_task_status_log(CeleryFileLog="",ExecDate="",CeleryTaskID="",CeleryTaskFlag="",CeleryTaskStatus="",InterfaceURL="",InterfaceParamJson={},
                           InterfaceServiceCode="",InterfaceAccountID="",InterfaceFlag="",InterfaceToken=""):
    # 记录celery任务日志
    """
    ExecDate：执行日期
    CeleryTaskID：celery任务id
    CeleryTaskFlag：celery任务执行步骤标识
    CeleryTaskStatus：celery任务执行状态
    InterfaceURL：接口请求url
    InterfaceParamJson：接口请求json参数
    InterfaceServiceCode：代理商code，用来识别token
    InterfaceAccountID：请求子账户
    InterfaceFlag：接口标识
    InterfaceToken：请求接口token
    """
    celery_task_id = str(CeleryTaskID).replace(" ","")
    celery_task_flag = str(CeleryTaskFlag).replace(" ","")
    celery_task_status = str(CeleryTaskStatus).replace(" ","")
    interface_url = str(InterfaceURL).replace(" ","")
    interface_param_json = str(InterfaceParamJson).replace(" ","")
    interface_service_code = str(InterfaceServiceCode).replace(" ","")
    interface_account_id = str(InterfaceAccountID).replace(" ","")
    interface_flag = str(InterfaceFlag).replace(" ","")
    interface_token = str(InterfaceToken).replace(" ","")
    set_run = True
    n = 0
    while set_run:
        status = os.system("""echo "%s %s %s %s %s %s %s %s %s %s">>%s """ % (ExecDate,celery_task_id,celery_task_flag,celery_task_status,interface_url,interface_param_json,interface_service_code,interface_account_id,interface_flag,interface_token,CeleryFileLog))
        if int(status) == 0:
            set_run = False
        else:
            if n > 10:
                set_run = False
                msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                                       SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                                       TargetTable="%s.%s" % ("TargetDB", "TargetTable"),
                                       BeginExecDate=ExecDate,
                                       EndExecDate=ExecDate,
                                       Status="Error",
                                       Log="请确认配置表指定主键字段是否正确！！！",
                                       Developer="developer")
                set_exit(LevelStatu="red", MSG=msg)
            else:
                time.sleep(2)
        n = n + 1
