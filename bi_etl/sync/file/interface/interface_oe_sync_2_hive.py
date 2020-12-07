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


#入口方法
def main(TaskInfo,Level="",**kwargs):
    global airflow
    global developer
    global regexp_extract_column
    airflow = Airflow(kwargs)
    media_type = TaskInfo[1]
    task_type = TaskInfo[4]
    print(TaskInfo,"####################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    exec_date = airflow.execution_date_utc8_str[0:10]
    get_sync_interface_2_local(AirflowDag=airflow.dag, AirflowTask=airflow.task, TaskInfo=TaskInfo, ExecDate=exec_date)

def set_sync_pages_number(DataList="",ParamJson="",UrlPath="",SyncDir="",PageTaskFile="",CelerySyncTaskFile="",DataFileDir="",DataFile=""):
    param_json = ParamJson
    db_data = DataList
    for data in db_data:
        param_json["advertiser_id"] = data[0]
        param_json["service_code"] = data[2]
        param_json["filtering"]["campaign_ids"] = [int(data[3])]
        celery_task_id = get_oe_sync_tasks_data_return_celery.delay(ParamJson=str(param_json), UrlPath=UrlPath,
                                                                    PageTaskFile=PageTaskFile,
                                                                    DataFileDir=DataFileDir,DataFile=DataFile
                                                                    )
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

def get_sync_interface_2_local(AirflowDag="",AirflowTask="",TaskInfo="",ExecDate=""):
  local_time = time.strftime("%Y-%m-%d_%H_%M_%S", time.localtime())
  local_dir = """/home/ecsage_data/oceanengine/sync/%s/%s/%s"""%(ExecDate,AirflowDag,AirflowTask)
  celery_get_page_status = """%s/celery_get_page_status.log"""%(local_dir)
  celery_get_data_status = "%s/celery_get_data_status.log"%(local_dir)
  page_task_file = "%s/page_task_file.log"%(local_dir)
  data_task_file = """%s/data_task_file.log"""%(local_dir)
  tmp_data_task_file = """%s/tmp_data_task_file.log""" % (local_dir)
  task_exception_file = "%s/task_exception_file.log"%(local_dir)
  param_json = ast.literal_eval(json.loads(json.dumps(TaskInfo[5])))
  ######param_json = {"end_date": "2020-12-06", "page_size": "1000", "start_date": "2020-12-06",
  ######             "advertiser_id": "", "group_by": ['STAT_GROUP_BY_FIELD_ID', 'STAT_GROUP_BY_CITY_NAME'],
  ######             "time_granularity": "STAT_TIME_GRANULARITY_DAILY",
  ######             "page": 1,
  ######             "filtering": {"campaign_ids": "","status":"CREATIVE_STATUS_ALL"},
  ######             "service_code": "data[2]"
  ######             }
  url_path = TaskInfo[4]
  filter_db_name = TaskInfo[21]
  filter_table_name = TaskInfo[22]
  filter_column_name = TaskInfo[23]
  filter_config = TaskInfo[24]
  os.system("""mkdir -p %s"""%(local_dir))
  os.system("""rm -f %s*"""%(celery_get_page_status.split(".")[0]))
  os.system("""rm -f %s*""" % (data_task_file.split(".")[0]))
  os.system("""rm -f %s*""" % (page_task_file.split(".")[0]))
  os.system("""rm -f %s*""" % (celery_get_data_status.split(".")[0]))
  os.system("""rm -f %s*"""%(task_exception_file.split(".")[0]))
  #判断是否从列表过滤
  if filter_db_name is not None and len(filter_db_name) > 0:
      filter_sql = """
      select concat_ws(' ',%s) from %s.%s where etl_date='%s' %s group by %s
      """%(filter_column_name,filter_db_name,filter_table_name,ExecDate,filter_config,filter_column_name)
      print(filter_sql,"#########################################")
      os.system("""spark-sql -S -e"%s"> %s"""%(filter_sql,tmp_data_task_file))
  exit(0)
  sql = """
       select a.account_id, a.media_type, a.service_code,b.campaign_id
       from metadb.oe_account_interface a
       inner join metadb.campaign_test b
       on a.account_id = b.advertiser_id
       where a.exec_date = '2020-12-06'
       --  and a.account_id in( '1681782749640718')
       group by a.account_id, a.media_type, a.service_code,b.campaign_id
    """
  ok,db_data = etl_md.get_all_rows(sql)
  etl_md.execute_sql("delete from metadb.oe_sync_page_interface  ")
  set_sync_pages_number(DataList=db_data, ParamJson=param_json, UrlPath=url_path, SyncDir=local_dir,
                        PageTaskFile=page_task_file, CelerySyncTaskFile=celery_get_page_status,DataFileDir=local_dir,
                        DataFile=data_task_file.split("/")[-1].split(".")[0]+"_1_%s."%(local_time)+data_task_file.split("/")[-1].split(".")[1])
  #重试异常
  n = 3
  for i in range(n):
    os.system("""rm -f %s*""" % (celery_get_page_status.split(".")[0]))
    os.system("""rm -f %s*""" % (page_task_file.split(".")[0]))
    os.system("""rm -f %s*""" % (celery_get_data_status.split(".")[0]))
    os.system("""rm -f %s*""" % (task_exception_file.split(".")[0]))
    sql = """
      select tmp1.account_id, '222' media_type, tmp1.service_code,trim(replace(replace(tmp1.request_filter,'[',''),']',''))
   from(select account_id,service_code,request_filter,count(distinct remark) as rn
        from metadb.oe_sync_page_interface
        group by account_id,service_code,request_filter
        having count(distinct remark) = 1
       ) tmp
   inner join metadb.oe_sync_page_interface tmp1
   on tmp.account_id = tmp1.account_id
   and tmp.service_code = tmp1.service_code
   and tmp.request_filter = tmp1.request_filter
   where tmp1.remark = '异常'
   group by tmp1.account_id, tmp1.service_code,tmp1.request_filter,tmp1.request_filter
      union all
   select account_id, '222' media_type, service_code,trim(replace(replace(request_filter,'[',''),']',''))
   from metadb.oe_sync_page_interface a 
   where page_num = 0
     and remark = '正常'
     and (data is null or length(data) = 0)
  group by account_id, service_code,request_filter,request_filter
  """
    ok, db_data = etl_md.get_all_rows(sql)
    if db_data is not None and len(db_data) > 0:
       set_sync_pages_number(DataList=db_data, ParamJson=param_json, UrlPath=url_path, SyncDir=local_dir,
                              PageTaskFile=page_task_file, CelerySyncTaskFile=celery_get_page_status,
                              DataFileDir=local_dir,
                              DataFile=data_task_file.split("/")[-1].split(".")[0] + "_1_%s." % (local_time) +
                                       data_task_file.split("/")[-1].split(".")[1])

       ok, db_data = etl_md.get_all_rows(sql)
       if db_data is not None and len(db_data) > 0:
         time.sleep(60)
       else:
          break

  sql = """
    select a.account_id, '' as media_type, a.service_code,a.page_num,a.request_filter
    from metadb.oe_sync_page_interface a where page_num > 1 -- and page_num <= 50
    group by a.account_id,  a.service_code,a.page_num,a.request_filter
  """
  ok, datas = etl_md.get_all_rows(sql)
  if datas is not None and len(datas) > 0:
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
           celery_task_id = get_oe_sync_tasks_data_celery.delay(ParamJson=str(param_json), UrlPath=url_path,
                                                                TaskExceptionFile=task_exception_file,
                                                                DataFileDir=local_dir,
                                                                DataFile=data_task_file.split("/")[-1].split(".")[0]+"_2_%s."%(local_time)+data_task_file.split("/")[-1].split(".")[1])
           os.system("""echo "%s %s">>%s""" % (celery_task_id,account_id, celery_get_data_status))
     # 获取状态
     print("正在等待celery队列执行完成！！！")
     celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_get_data_status)
     wait_for_celery_status(StatusList=celery_task_id)
     print("celery队列执行完成！！！%s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
     time.sleep(30)
     #重试异常
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
    if status == "SUCCESS":
       return True
    else:
       return False

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

def wait_for_celery_status(StatusList=""):
    status_false = []
    run_wait = True
    sleep_num = 1
    while run_wait:
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
