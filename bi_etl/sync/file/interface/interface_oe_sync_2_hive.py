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
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_oe_sync_tasks_data_return as get_oe_sync_tasks_data_return_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_oe_sync_tasks_data as get_oe_sync_tasks_data_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_write_local_files as get_write_local_files_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_local_hdfs_thread
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.get_account_tokens import get_oe_account_token
from ecsage_bigdata_etl_engineering.common.base.sync_method import get_table_columns_info
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.get_data_2_snap import exec_snap_hive_table
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread
from ecsage_bigdata_etl_engineering.common.base.get_config import Conf
import os
import time
import json
import ast

conf = Conf().conf
etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")

def set_sync_pages_number(DataList="",ParamJson="",UrlPath="",SyncDir="",PageTaskFile="",CelerySyncTaskFile=""):
    param_json = ParamJson
    db_data = DataList
    for data in db_data:
        param_json["advertiser_id"] = data[0]
        param_json["service_code"] = data[2]
        param_json["filtering"]["campaign_ids"] = [int(data[3])]
        celery_task_id = get_oe_sync_tasks_data_return_celery.delay(ParamJson=str(param_json), UrlPath=UrlPath,
                                                                    PageTaskFile=PageTaskFile)
        os.system("""echo "%s %s %s %s">>%s""" % (celery_task_id, data[0], data[1], data[2], CelerySyncTaskFile))
    # 获取状态
    celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=CelerySyncTaskFile)
    print("正在等待获取页数celery队列执行完成！！！")
    wait_for_celery_status(StatusList=celery_task_id)
    print("获取页数celery队列执行完成！！！")
    print("end %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())), "===================")
    # 保存MySQL
    columns = """page_num,account_id,service_code,remark,data,request_filter"""
    load_data_mysql(AsyncAccountFile=SyncDir, DataFile=PageTaskFile, TableName="oe_sync_page_interface",Columns=columns)

def get_sync_pages_number():
  print("begin %s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())),"===================")
  celery_sync_task_status = """/home/ecsage_data/oceanengine/async/2/celery_sync_task_status.log"""
  celery_sync_task_data_status = "/home/ecsage_data/oceanengine/async/2/celery_sync_task_data_status.log"
  page_task_file = "/home/ecsage_data/oceanengine/async/2/page_task_file.log"
  data_task_file = """/home/ecsage_data/oceanengine/async/2/testtest.log"""
  sync_data_file = """/home/ecsage_data/oceanengine/async/2/sync_data_file.log"""
  async_account_file = "/home/ecsage_data/oceanengine/async/2"
  task_exception_file = "/home/ecsage_data/oceanengine/async/2/task_exception_file.log"
  write_local_files_stauts = "/home/ecsage_data/oceanengine/async/2/write_local_files_stauts.log"
  param_json = {"end_date": "2020-12-02", "page_size": "1000", "start_date": "2020-12-02",
               "advertiser_id": "", "group_by": ['STAT_GROUP_BY_FIELD_ID', 'STAT_GROUP_BY_CITY_NAME'],
               "time_granularity": "STAT_TIME_GRANULARITY_DAILY",
               "page": 1,
               "filtering": {"campaign_ids": ""},
               "service_code": "data[2]"
               }
  url_path = "/open_api/2/report/creative/get/"
  os.system("""rm -f %s"""%(celery_sync_task_status))
  os.system("""rm -f %s""" % (sync_data_file))
  os.system("""rm -f %s*""" % (page_task_file))
  os.system("""rm -f %s*""" % (celery_sync_task_data_status))
  os.system("""rm -f %s*""" % (data_task_file))
  os.system("""rm -f %s*"""%(task_exception_file))
  os.system("""rm -f %s*""" % (write_local_files_stauts))
  sql = """
       select a.account_id, a.media_type, a.service_code,b.campaign_id
       from metadb.oe_account_interface a
       inner join metadb.campaign_test b
       on a.account_id = b.advertiser_id
       where a.exec_date = '2020-12-02'
       group by a.account_id, a.media_type, a.service_code,b.campaign_id
       limit 1
    """
  ok,db_data = etl_md.get_all_rows(sql)
  etl_md.execute_sql("delete from metadb.oe_sync_page_interface  ")
  set_sync_pages_number(DataList=db_data, ParamJson=param_json, UrlPath=url_path, SyncDir=async_account_file,
                        PageTaskFile=page_task_file, CelerySyncTaskFile=celery_sync_task_status)
  #重试异常
  ########n = 3
  ########for i in range(n):
  ########  os.system("""rm -f %s""" % (celery_sync_task_status))
  ########  os.system("""rm -f %s*""" % (page_task_file))
  ########  os.system("""rm -f %s*""" % (celery_sync_task_data_status))
  ########  os.system("""rm -f %s*""" % (data_task_file))
  ########  sql = """
  ########     select tmp1.account_id, '222' media_type, tmp1.service_code
  ########     from(select account_id,service_code,count(distinct remark) as rn
  ########          from metadb.oe_sync_page_interface
  ########          group by account_id,service_code
  ########          having count(distinct remark) = 1
  ########         ) tmp
  ########     inner join metadb.oe_sync_page_interface tmp1
  ########     on tmp.account_id = tmp1.account_id
  ########     and tmp.service_code = tmp1.service_code
  ########     where tmp1.remark = '异常'
  ########     group by tmp1.account_id, tmp1.service_code
  ########  """
  ########  ok, db_data = etl_md.get_all_rows(sql)
  ########  if db_data is not None and len(db_data) > 0:
  ########     set_sync_pages_number(DataList=db_data, ParamJson=param_json, UrlPath=url_path, SyncDir=async_account_file,
  ########                           PageTaskFile=page_task_file, CelerySyncTaskFile=celery_sync_task_status)
  ########  ok, db_data = etl_md.get_all_rows(sql)
  ########  if db_data is not None and len(db_data) > 0:
  ########      time.sleep(60)
  ########  else:
  ########      break

  sql = """
    select a.account_id, '' as media_type, a.service_code,a.page_num,a.request_filter
    from metadb.oe_sync_page_interface a where page_num > 1 -- and page_num <= 50
    group by a.account_id,  a.service_code,a.page_num,a.request_filter
  """
  ok,datas = etl_md.get_all_rows(sql)
  for dt in datas:
     page_number = int(dt[3])
     for page in range(page_number):
      if page > 0:
        pages = page + 1
        param_json["page"] = pages
        account_id = dt[0]
        param_json["advertiser_id"] = account_id
        param_json["service_code"] = dt[2]
        param_json["filtering"]["campaign_ids"] = eval(dt[4])
        celery_task_id = get_oe_sync_tasks_data_celery.delay(ParamJson=str(param_json), UrlPath=url_path,TaskExceptionFile=task_exception_file)
        os.system("""echo "%s %s">>%s""" % (celery_task_id,account_id, celery_sync_task_data_status))
  # 获取状态
  print("正在等待celery队列执行完成！！！")
  celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_sync_task_data_status)
  wait_for_celery_status(StatusList=celery_task_id)
  print("celery队列执行完成！！！%s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
  time.sleep(30)
  print("正在写入本地文件！！！%s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
  ####### target_file = ["celery_sync_task_data_status.log","celery_sync_task_status.log"] #os.listdir(async_account_file)
  ####### status_data_file = celery_sync_task_data_status.split("/")[-1]
  ####### th = []
  ####### i = 0
  ####### for files in target_file:
  #######     get_file = "%s/%s" % (async_account_file, files)
  #######
  #######
  #######     etl_thread = EtlThread(thread_id=i, thread_name="%d" % (i),
  #######                                my_run=run_thread,
  #######                                StatusFile=get_file, DataLocalFile=sync_data_file,
  #######                                WriteLocalFilesStauts=write_local_files_stauts
  #######                                )
  #######     etl_thread.start()
  #######     th.append(etl_thread)
  #######     i = i + 1
  ####### for etl_th in th:
  #######    etl_th.join()
  #######
  ####### print("等待写入本地文件！！！%s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
  ####### celery_write_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=write_local_files_stauts)
  ####### wait_for_celery_status(StatusList=celery_write_task_id)

      #if status_data_file in files:
          #get_file = "%s/%s" % (async_account_file, files)
          #with open(get_file) as lines:
              #array = lines.readlines()
              #for data in array:
                  #get_data1 = data.strip('\n').split(" ")
                  #get_celery_job_data(CeleryTaskId=get_data1[0],AccountId=account_id,DataLocalFile=sync_data_file)
  print("完成写入本地文件！！！%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
  print("执行完成！！！%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

def run_thread(StatusFile="",DataLocalFile="",WriteLocalFilesStauts="",arg=None):
   if arg is not None and len(arg)>0:
    StatusFile = arg["StatusFile"]
    DataLocalFile = arg["DataLocalFile"]
    #AccountId = arg["AccountId"]
    WriteLocalFilesStauts = arg["WriteLocalFilesStauts"]
    with open(StatusFile) as lines:
        array = lines.readlines()
        for data in array:
            get_data1 = data.strip('\n').split(" ")
            get_celery_job_data(CeleryTaskId=get_data1[0],AccountId=get_data1[1],DataLocalFile=DataLocalFile,WriteLocalFilesStauts=WriteLocalFilesStauts)

def get_celery_job_data(CeleryTaskId="",AccountId="",DataLocalFile="",WriteLocalFilesStauts=""):
    pass
    #task_write_id = get_write_local_files_celery.delay(CeleryTaskId=CeleryTaskId,AccountId=AccountId,DataLocalFile=DataLocalFile)
    #os.system("""echo '%s'>>%s """%(task_write_id,WriteLocalFilesStauts))

def rerun_data():
    sql = """
           select tmp1.account_id, '222' media_type, tmp1.service_code
           from(select account_id,service_code,count(distinct remark) as rn
                from metadb.oe_sync_page_interface
                group by account_id,service_code
                having count(distinct remark) = 1
               ) tmp
           inner join metadb.oe_sync_page_interface tmp1
           on tmp.account_id = tmp1.account_id
           and tmp.service_code = tmp1.service_code
           where tmp1.remark = '异常'
           group by tmp1.account_id, tmp1.service_code
        """
    ok, db_data = etl_md.get_all_rows(sql)
    return db_data

def load_data_mysql(AsyncAccountFile="",DataFile="",TableName="",Columns=""):
    target_file = os.listdir(AsyncAccountFile)
    for files in target_file:
        if DataFile.split("/")[-1] in files:
            print(files, "###############################################")
            # 记录子账户
            insert_sql = """
                  load data local infile '%s' into table metadb.%s fields terminated by ' ' lines terminated by '\\n' (%s)
               """ % (AsyncAccountFile + "/" + files,TableName,Columns)
            ok = etl_md.local_file_to_mysql(sql=insert_sql)
            if ok is False:
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

def get_celery_job_status(CeleryTaskId=""):
    set_task = AsyncResult(id=str(CeleryTaskId))
    status = set_task.status
    value = set_task.get()
    if status == "SUCCESS":
       return True,value
    else:
       return False,value

def get_celery_status_list(CeleryTaskStatusFile=""):
    celery_task_id = []
    status_wait = []
    with open(CeleryTaskStatusFile) as lines:
        array = lines.readlines()
        for data in array:
            get_data1 = data.strip('\n').split(" ")
            if get_celery_job_status(CeleryTaskId=get_data1[0])[0] is False:
                status_wait.append(get_data1[0])
                celery_task_id.append(get_data1[0])
    return celery_task_id,status_wait

def wait_for_celery_status(StatusList=""):
    status_false = []
    run_wait = True
    sleep_num = 1
    while run_wait:
      for status in StatusList:
        #判断是否成功
        if get_celery_job_status(CeleryTaskId=status)[0] is False:
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

def rerun_exception_create_tasks(AsyncAccountDir="",ExceptionFile="",DataFile="",CeleryTaskDataFile="",LogSession="",InterfaceFlag="",ExecDate=""):
    async_data_file = """%s/%s"""%(AsyncAccountDir,DataFile.split("/")[-1])
    celery_task_data_file = """%s/%s"""%(AsyncAccountDir,CeleryTaskDataFile.split("/")[-1])
    async_data_exception_file = """%s/%s""" % (AsyncAccountDir, ExceptionFile.split("/")[-1])
    #先保留第一次
    delete_sql = """delete from metadb.oe_async_exception_create_tasks_interface where interface_flag = '%s' """ % (InterfaceFlag)
    etl_md.execute_sql(delete_sql)
    columns = """account_id,interface_flag,media_type,service_code,group_by,fields,token_data"""
    table_name = "oe_async_exception_create_tasks_interface"
    save_exception_tasks(AsyncAccountDir=AsyncAccountDir,ExceptionFile=ExceptionFile,TableName=table_name,Columns=columns)
    #
    n = 3
    for i in range(n):
        sql = """
          select distinct a.account_id,a.interface_flag,a.media_type,a.service_code,a.group_by,a.fields,a.token_data
          from metadb.oe_async_exception_create_tasks_interface a
          where interface_flag = '%s'
        """% (InterfaceFlag)
        ok,datas = etl_md.get_all_rows(sql)
        if datas is not None and len(datas) > 0:
           print("开始第%s次重试异常，时间：%s"%(i+1,time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
           for data in datas:
               pass
               ###status_id = get_oe_async_tasks_create_celery.delay(AsyncTaskName="%s" % (i), AsyncTaskFile=async_data_file,
               ###                                               AsyncTaskExceptionFile=async_data_exception_file,
               ###                                               ExecData=data, ExecDate=ExecDate)
               #os.system("""echo "%s %s">>%s""" % (status_id, data[0], celery_task_data_file+".%s"%(i)))
           celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_data_file + ".%s"%i)
           wait_for_celery_status(StatusList=celery_task_id)
           delete_sql = """delete from metadb.oe_async_exception_create_tasks_interface where interface_flag = '%s' """ % (InterfaceFlag)
           etl_md.execute_sql(delete_sql)
           save_exception_tasks(AsyncAccountDir=AsyncAccountDir, ExceptionFile=ExceptionFile, TableName=table_name,Columns=columns)
           print("结束第%s次重试异常，时间：%s" % (i + 1, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
           #判断结果是否还有异常
           ex_sql = """
                     select a.account_id,a.interface_flag,a.media_type,a.service_code,a.group_by,a.fields,a.token_data
                     from metadb.oe_async_exception_create_tasks_interface a
                     where interface_flag = '%s'
                     limit 1
              """% (InterfaceFlag)
           ok, ex_datas = etl_md.get_all_rows(ex_sql)
           if ex_datas is not None and len(ex_datas) > 0:
               print("休眠中...，时间：%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
               if i == 0:
                 time.sleep(360)
               else:
                 time.sleep(180)
    ex_sql = """
         select a.account_id,a.interface_flag,a.media_type,a.service_code,a.group_by,a.fields,a.token_data
         from metadb.oe_async_exception_create_tasks_interface a
         where interface_flag = '%s'
    """% (InterfaceFlag)
    ok, ex_datas = etl_md.get_all_rows(ex_sql)
    if ex_datas is not None and len(ex_datas) > 0:
        print("还有特别异常任务存在！！！")
        print(ex_datas[0])

def save_exception_tasks(AsyncAccountDir="",ExceptionFile="",TableName="",Columns=""):
    exception_file = ExceptionFile.split("/")[-1]
    exception_file_list = []
    target_file = os.listdir(AsyncAccountDir)
    for files in target_file:
      if exception_file in files:
         exception_file_list.append((AsyncAccountDir, files))
    if exception_file_list is not None and len(exception_file_list) > 0 :
       for file in exception_file_list:
           load_data_mysql(AsyncAccountFile=file[0], DataFile=file[1],TableName=TableName, Columns=Columns)
           os.system("""rm -f %s/%s"""%(file[0],file[1]))

if __name__ == '__main__':
    get_sync_pages_number()