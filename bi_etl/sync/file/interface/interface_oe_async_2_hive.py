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
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_oe_async_tasks_status as get_oe_async_tasks_status_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_oe_async_tasks_data as get_oe_async_tasks_data_celery
import os
import time

etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")

#入口方法
def main(TaskInfo,**kwargs):
    global airflow
    global developer
    global regexp_extract_column
    airflow = Airflow(kwargs)
    media_type = TaskInfo[1]
    task_type = TaskInfo[4]
    print(TaskInfo,"####################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    exec_date = airflow.execution_date_utc8_str[0:10]
    """任务类型，1：创建异步任务，0：获取异步任务状态，2：获取异步任务数据，3：ods同步，4：snap同步"""
    if task_type == 2:
       #get_oe_async_tasks_status(MediaType=media_type,ExecDate=exec_date)
       get_oe_async_tasks_data(AirflowDagId=airflow.dag,AirflowTaskId=airflow.task,TaskInfo=TaskInfo,MediaType=media_type,ExecDate=exec_date)

def get_oe_async_tasks_status(MediaType="",ExecDate=""):
    media_type = MediaType
    async_account_file = "/home/ecsage_data/oceanengine/async"
    async_status_exception_file = """%s/async_status_exception_%s.log""" % (async_account_file,media_type)
    async_notempty_file = """%s/async_notempty_%s.log""" % (async_account_file,media_type)
    async_empty_file = """%s/async_empty_%s.log""" % (async_account_file,media_type)
    async_not_succ_file = """%s/async_not_succ_file_%s.log""" % (async_account_file,media_type)
    celery_task_status_file = """%s/celery_task_status_file_%s.log"""%(async_account_file,media_type)
    os.system("""mkdir -p %s"""%(async_account_file))
    os.system("""rm -f %s*""" % (async_not_succ_file))
    os.system("""rm -f %s*""" % (async_notempty_file))
    os.system("""rm -f %s*""" % (async_empty_file))
    os.system("""rm -f %s*""" % (async_status_exception_file))
    os.system("""rm -f /tmp/sql_%s.sql""")
    os.system("""rm -f %s*"""%(celery_task_status_file))
    etl_md.execute_sql("""delete from metadb.oe_valid_account_interface where media_type=%s and exec_date = '%s' """ % (media_type,ExecDate))
    etl_md.execute_sql("""delete from metadb.oe_not_valid_account_interface where media_type=%s and exec_date = '%s' """ % (media_type,ExecDate))
    #获取子账户
    source_data_sql = """
                 select distinct account_id,media_type,service_code,token_data,task_id,task_name
                 from metadb.oe_async_task_interface
                 where media_type = %s
    """%(media_type)
    ok, datas = etl_md.get_all_rows(source_data_sql)
    for get_data in datas:
          status_id = get_oe_async_tasks_status_celery.delay(AsyncNotemptyFile=async_notempty_file,AsyncEmptyFile=async_empty_file,
                                               AsyncStatusExceptionFile=async_status_exception_file,ExecData=get_data,ExecDate=ExecDate)
          os.system("""echo "%s">>%s"""%(status_id,celery_task_status_file))
    #获取状态
    celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_status_file)
    print("正在等待celery队列执行完成！！！")
    wait_for_celery_status(StatusList=celery_task_id)
    print("celery队列执行完成！！！")
    print("等待重试异常任务！！！")
    rerun_exception_tasks(AsyncAccountDir=async_account_file,ExceptionFile=async_status_exception_file,
                          AsyncNotemptyFile=async_notempty_file,AsyncemptyFile=async_empty_file,
                          CeleryTaskStatusFile=celery_task_status_file,ExecDate=ExecDate)
    print("重试异常任务执行完成！！！")
    time.sleep(60)
    #落地有数据
    load_data_mysql(AsyncAccountFile=async_account_file, DataFile=async_notempty_file, TableName="oe_valid_account_interface")
    #落地没数据
    load_data_mysql(AsyncAccountFile=async_account_file, DataFile=async_empty_file,TableName="oe_not_valid_account_interface")

def get_celery_job_status(CeleryTaskId=""):
    set_task = AsyncResult(CeleryTaskId)
    status = set_task.status
    if status == "SUCCESS":
       return True
    else:
       return False

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
              msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
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

#重跑异常任务
def rerun_exception_tasks(AsyncAccountDir="",ExceptionFile="",AsyncNotemptyFile="",AsyncemptyFile="",CeleryTaskStatusFile="",ExecDate=""):
    exception_file = ExceptionFile.split("/")[-1]
    async_notempty_file = """%s/%s.last_runned"""%(AsyncAccountDir,AsyncNotemptyFile.split("/")[-1])
    async_empty_file = """%s/%s.last_runned"""%(AsyncAccountDir,AsyncemptyFile.split("/")[-1])
    celery_task_status_file = """%s/%s.last_runned"""%(AsyncAccountDir,CeleryTaskStatusFile.split("/")[-1])
    async_status_exception_file = """%s/%s.last_runned""" % (AsyncAccountDir, ExceptionFile.split("/")[-1])
    target_file = os.listdir(AsyncAccountDir)
    exception_file_list = []
    for files in target_file:
        if exception_file in files:
            exception_file_list.append(files)
            exception_dir_file = """%s/%s"""%(AsyncAccountDir,files)
            with open(exception_dir_file) as lines:
                array = lines.readlines()
                for data in array:
                    get_data = data.strip('\n').split(" ")
                    status_id = get_oe_async_tasks_status_celery.delay(AsyncNotemptyFile=async_notempty_file,
                                                         AsyncEmptyFile=async_empty_file,
                                                         AsyncStatusExceptionFile=async_status_exception_file,
                                                         ExecData=get_data,ExecDate=ExecDate)
                    os.system("""echo "%s %s">>%s""" % (status_id, get_data[0],celery_task_status_file))
    if len(exception_file_list) > 0:
        celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_status_file)
        wait_for_celery_status(StatusList=celery_task_id)
        print("重试异常完成！！！")

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

def load_data_mysql(AsyncAccountFile="",DataFile="",TableName=""):
    target_file = os.listdir(AsyncAccountFile)
    for files in target_file:
        if DataFile.split("/")[-1] in files:
            print(files, "###############################################")
            # 记录子账户
            insert_sql = """
                  load data local infile '%s' into table metadb.%s fields terminated by ' ' lines terminated by '\\n' (exec_date,account_id,media_type,service_code,token_data,task_id,task_name)
               """ % (AsyncAccountFile + "/" + files,TableName)
            ok = etl_md.local_file_to_mysql(sql=insert_sql)
            if ok is False:
                msg = "写入MySQL出现异常！！！\n%s" % (DataFile)
                msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                                       SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                                       TargetTable="%s.%s" % ("", ""),
                                       BeginExecDate="",
                                       EndExecDate="",
                                       Status="Error",
                                       Log=msg,
                                       Developer="developer")
                set_exit(LevelStatu="red", MSG=msg)

def get_oe_async_tasks_data(AirflowDagId="",AirflowTaskId="",TaskInfo="",MediaType="",ExecDate=""):
    media_type = MediaType
    target_handle = TaskInfo[8]
    target_db = TaskInfo[9]
    target_table = TaskInfo[10]
    async_account_file = "/home/ecsage_data/oceanengine/async/%s"%(media_type)
    async_data_exception_file = """%s/%s_%s_exception.%s.log""" % (AirflowDagId,AirflowTaskId,async_account_file,ExecDate)
    async_data_file = """%s/%s_%s_data.%s.log""" % (async_account_file,AirflowDagId,AirflowTaskId,ExecDate)
    celery_task_data_file = """%s/%s_%s_celery_status.%s.log""" % (async_account_file,AirflowDagId,AirflowTaskId,ExecDate)
    os.system("""mkdir -p %s""" % (async_account_file))
    os.system("""rm -f %s/*""" % (async_account_file))
    #os.system("""rm -f %s*""" % (async_data_exception_file))
    #os.system("""rm -f %s*""" % (celery_task_data_file))
    # 获取子账户
    source_data_sql = """
        select a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.task_name
        from metadb.oe_valid_account_interface a
        left join metadb.oe_not_valid_account_interface b
        on a.media_type = b.media_type
        and a.account_id = b.account_id
        and a.service_code = b.service_code
        and a.exec_date = b.exec_date
        where b.service_code is null
          and a.media_type = %s
          and a.exec_date = '%s'
        group by a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.task_name
        """ % (media_type,ExecDate)
    ok, datas = etl_md.get_all_rows(source_data_sql)
    for get_data in datas:
        status_id = get_oe_async_tasks_data_celery.delay(DataFile=async_data_file,ExceptionFile=async_data_exception_file,ExecData=get_data,ExecDate=ExecDate)
        os.system("""echo "%s %s">>%s""" % (status_id,get_data[0], celery_task_data_file))

    #获取状态
    celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_data_file)
    print("正在等待celery队列执行完成！！！")
    wait_for_celery_status(StatusList=celery_task_id)
    print("celery队列执行完成！！！")
    print("等待重试异常任务！！！")
    time.sleep(60)
    rerun_exception_downfile_tasks(AsyncAccountDir=async_account_file, ExceptionFile=async_data_exception_file, DataFile=async_data_file, CeleryTaskDataFile=celery_task_data_file)
    time.sleep(30)
    #上传至hdfs
    get_local_file_hdfs(TargetHandle=target_handle, TargetDb=target_db, TargetTable=target_table,DataFile=async_data_file,ExecDate=ExecDate)

def get_local_file_hdfs(TargetHandle="",TargetDb="",TargetTable="",AsyncAccountDir="",DataFile="",ExecDate=""):
    target_file = os.listdir(AsyncAccountDir)
    data_file = DataFile.split("/")[-1]
    hdfs_dir = "/tmp/datafolder_new"
    load_sqls = ""
    print("hadoop fs -rmr %s*" % (hdfs_dir+"/"+data_file), "************************************")
    print("hadoop fs -put %s* %s" % (DataFile, hdfs_dir), "************************************")
    ok_data_1 = os.system("hadoop fs -rmr %s*" % (hdfs_dir+"/"+data_file))
    ok_data = os.system("hadoop fs -put %s* %s" % (DataFile, hdfs_dir))
    if ok_data != 0 and ok_data_1 != 0:
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (TargetDb, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="上传本地数据文件至HDFS出现异常！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)
    for files in target_file:
        if data_file in files:
            print(files,"==================================")
            load_sql = """load data inpath '/tmp/datafolder_new1/all.log.bd17-node.bak' INTO TABLE etl_mid.test_test;\n"""
            load_sqls = load_sql + load_sqls

def rerun_exception_downfile_tasks(AsyncAccountDir="",ExceptionFile="",DataFile="",CeleryTaskDataFile=""):
    exception_file = ExceptionFile.split("/")[-1]
    async_data_file = """%s/%s"""%(AsyncAccountDir,DataFile.split("/")[-1])
    celery_task_data_file = """%s/%s.last_runned"""%(AsyncAccountDir,CeleryTaskDataFile.split("/")[-1])
    async_data_exception_file = """%s/%s.last_runned""" % (AsyncAccountDir, ExceptionFile.split("/")[-1])
    run_true = True
    n = 0
    while run_true:
     exception_file_list = []
     target_file = os.listdir(AsyncAccountDir)
     for files in target_file:
        if exception_file in files:
            exception_file_list.append(files)
            exception_dir_file = """%s/%s"""%(AsyncAccountDir,files)
            with open(exception_dir_file) as lines:
                array = lines.readlines()
                for data in array:
                    get_data = data.strip('\n').split(" ")
                    #判断此任务是否有创建，若是没有，则调用创建，只限两次，第三次还没创建，自动放弃
                    status_id = get_oe_async_tasks_data_celery.delay(DataFile=async_data_file,ExceptionFile=async_data_exception_file+".%s"%n,ExecData=get_data)
                    os.system("""echo "%s %s">>%s""" % (status_id, get_data[0],celery_task_data_file+".%s"%n))
            os.system("""rm -f %s"""%(exception_dir_file))
     if len(exception_file_list) > 0:
        celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_data_file+".%s"%n)
        wait_for_celery_status(StatusList=celery_task_id)
        os.system("""rm -f %s"""%(celery_task_data_file +".%s"%n))
        print("重试异常完成！！！")
     if len(exception_file_list) == 0 or n == 10:
         if len(exception_file_list) >0:
             print("还有特别子账户出现异常！！！")
         run_true = False
     n = n + 1
