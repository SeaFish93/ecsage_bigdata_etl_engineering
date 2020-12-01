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

def get_sync_pages_number():
  print("begin %s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())),"===================")
  celery_sync_task_status = """/home/ecsage_data/oceanengine/async/2/celery_sync_task_status.log"""
  celery_sync_task_data_status = "/home/ecsage_data/oceanengine/async/2/celery_sync_task_data_status.log"
  page_task_file = "/home/ecsage_data/oceanengine/async/2/page_task_file.log"
  async_account_file = "/home/ecsage_data/oceanengine/async/2"
  param_json = {"end_date": "2020-11-29", "page_size": "200", "start_date": "2020-11-29",
               "advertiser_id": "", "group_by": ['STAT_GROUP_BY_FIELD_ID','STAT_GROUP_BY_CITY_NAME'],
               "time_granularity": "STAT_TIME_GRANULARITY_DAILY",
               "page": 1,
               "service_code": "data[2]"
               }
  url_path = "/open_api/2/report/creative/get/"
  os.system("""rm -f %s"""%(celery_sync_task_status))
  os.system("""rm -f %s*""" % (page_task_file))
  os.system("""rm -f %s*""" % (celery_sync_task_data_status))
  sql = """
       select a.account_id, a.media_type, a.service_code
       from metadb.oe_account_interface a
       where a.exec_date = '2020-11-29'
    """
  ok,db_data = etl_md.get_all_rows(sql)
  for data in db_data:
      param_json["advertiser_id"] = data[0]
      param_json["service_code"] = data[2]
      celery_task_id = get_oe_sync_tasks_data_return_celery.delay(ParamJson=str(param_json),UrlPath=url_path)
      os.system("""echo "%s %s %s %s">>%s""" % (celery_task_id,data[0],data[1],data[2], celery_sync_task_status))
  #获取状态
  celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_sync_task_status)
  print("正在等待celery队列执行完成！！！")
  wait_for_celery_status(StatusList=celery_task_id)
  print("celery队列执行完成！！！")
  print("end %s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())),"===================")
  # 保存MySQL
  columns = """page_num,account_id,service_code,remark"""
  etl_md.execute_sql("delete from metadb.oe_sync_page_interface  " )
  load_data_mysql(AsyncAccountFile=async_account_file, DataFile=page_task_file,TableName="oe_sync_page_interface", Columns=columns)
  sql = """
    select a.account_id, '' as media_type, a.service_code,a.page_num
    from metadb.oe_sync_page_interface a where page_num > 0
  """
  ok,datas = etl_md.get_all_rows(sql)
  print(type(param_json),"====================================")
  exit(0)
  for dt in datas:
     page_number = int(dt[3])
     for page in range(page_number):
      pages = page + 1
      param_json["page"] = pages
      param_json["advertiser_id"] = dt[0]
      param_json["service_code"] = dt[2]
      param_json = str(param_json)
      celery_task_id = get_oe_sync_tasks_data_celery.delay(ParamJson=str(param_json), UrlPath=url_path)
      os.system("""echo "%s">>%s""" % (celery_task_id, celery_sync_task_data_status))
  # 获取状态
  celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_sync_task_data_status)
  print("正在等待celery队列执行完成！！！")
  wait_for_celery_status(StatusList=celery_task_id)
  print("celery队列执行完成！！！%s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

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

if __name__ == '__main__':
    get_sync_pages_number()