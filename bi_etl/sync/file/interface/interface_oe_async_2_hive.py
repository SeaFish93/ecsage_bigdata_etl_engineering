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
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_oe_async_tasks_data_return as get_oe_async_tasks_data_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_oe_async_tasks_create as get_oe_async_tasks_create_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_oe_async_tasks_create_all as get_oe_async_tasks_create_all_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_oe_async_tasks_create_all_exception as get_oe_async_tasks_create_all_exception_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_local_hdfs_thread
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.get_account_tokens import get_oe_account_token
from ecsage_bigdata_etl_engineering.common.base.sync_method import get_table_columns_info
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.get_data_2_snap import exec_snap_hive_table
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread
from ecsage_bigdata_etl_engineering.common.base.get_config import Conf
import os
import time

conf = Conf().conf
etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")

#入口方法
def main(TaskInfo,**kwargs):
    global airflow
    global developer
    global regexp_extract_column
    airflow = Airflow(kwargs)
    media_type = TaskInfo[1]
    task_type = TaskInfo[4]
    task_id = TaskInfo[2]
    print(TaskInfo,"####################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    exec_date = airflow.execution_date_utc8_str[0:10]
    """任务类型，1：创建异步任务，0：获取异步任务状态，2：获取异步任务数据，3：ods同步，4：snap同步，5：获取token"""
    if task_type == 2:
       get_oe_async_tasks_data(AirflowDagId=airflow.dag,AirflowTaskId=airflow.task,TaskInfo=TaskInfo,MediaType=media_type,ExecDate=exec_date)
    elif task_type == 3:
       get_etl_mid_2_ods(AirflowDagId=airflow.dag,AirflowTaskId=airflow.task,TaskInfo=TaskInfo,MediaType=media_type,ExecDate=exec_date)
    elif task_type == 4:
       get_ods_2_snap(AirflowDagId=airflow.dag,AirflowTaskId=airflow.task,TaskInfo=TaskInfo,ExecDate=exec_date)
    elif task_type == 5:
       get_oe_async_tasks_token(MediaType=media_type)
    elif task_type == 1:
       if task_id == "set_create_oe_async_account":
         print("执行创建筛选子账户")
         get_oe_async_tasks_create_all(AirflowDagId=airflow.dag, AirflowTaskId=airflow.task, TaskInfo=TaskInfo,MediaType=media_type, ExecDate=exec_date)
       else:
         get_oe_async_tasks_create(AirflowDagId=airflow.dag,AirflowTaskId=airflow.task,TaskInfo=TaskInfo,MediaType=media_type,ExecDate=exec_date)
    elif task_type == 0:
        get_oe_async_tasks_status_all(AirflowDagId=airflow.dag, AirflowTaskId=airflow.task,TaskInfo=TaskInfo,ExecDate=exec_date)

def get_oe_async_tasks_status_all(AirflowDagId="", AirflowTaskId="",TaskInfo="",ExecDate=""):
    media_type = 2
    interface_flag = TaskInfo[20]
    async_account_file = "/home/ecsage_data/oceanengine/async/%s/%s" % (AirflowDagId, AirflowTaskId)
    async_status_exception_file = """%s/async_status_exception.log""" % (async_account_file)
    async_notempty_file = """%s/async_notempty.log""" % (async_account_file)
    async_empty_file = """%s/async_empty.log""" % (async_account_file)
    celery_task_status_file = """%s/celery_task_status_file.log"""%(async_account_file)
    os.system("""mkdir -p %s""" % (async_account_file))
    os.system("""rm -f %s/*""" % (async_account_file))
    etl_md.execute_sql("""delete from metadb.oe_valid_account_interface where  exec_date = '%s' """ % (ExecDate))
    etl_md.execute_sql("""delete from metadb.oe_not_valid_account_interface where exec_date = '%s' """ % (ExecDate))
    #获取子账户
    source_data_sql = """
        select a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.task_name
        from(
         select a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.task_name
         from oe_async_create_task a
         left join (select a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.task_name
                    from metadb.oe_async_create_task a
                    where task_id = '0'
                      and task_name = '999999'
                      and interface_flag = '%s'
                    group by a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.task_name
         ) b
         on a.media_type = b.media_type
         and a.service_code = b.service_code
         and a.account_id = b.account_id
         where a.interface_flag = '%s'
           and a.task_id <> '0'
                     union all
         select a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.task_name
         from(select a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.task_name
              from metadb.oe_async_create_task a
              where task_id = '0'
                and task_name = '999999'
                and interface_flag = '%s'
              group by a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.task_name
             ) a
         left join oe_async_create_task b
         on a.media_type = b.media_type
         and a.service_code = b.service_code
         and a.account_id = b.account_id
         and b.interface_flag = '%s'
         and b.task_id <> '0'
         where b.task_id is null
      ) a group by a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.task_name
    """%(interface_flag,interface_flag,interface_flag,interface_flag)
    ok, datas = etl_md.get_all_rows(source_data_sql)
    if datas is not None and len(datas) > 0:
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
       columns = "exec_date,account_id,media_type,service_code,token_data,task_id,task_name"
       #落地有数据
       load_data_mysql(AsyncAccountFile=async_account_file, DataFile=async_notempty_file, TableName="oe_valid_account_interface",Columns=columns)
       #落地没数据
       load_data_mysql(AsyncAccountFile=async_account_file, DataFile=async_empty_file,TableName="oe_not_valid_account_interface",Columns=columns)
       insert_sql = """
             insert into metadb.`oe_account_interface`
             select a.account_id, a.media_type, a.service_code,a.token_data,a.exec_date
             from metadb.oe_valid_account_interface a
             left join metadb.oe_not_valid_account_interface b
             on a.media_type = b.media_type
             and a.account_id = b.account_id
             and a.service_code = b.service_code
             and a.exec_date = b.exec_date
             where b.service_code is null
               and a.exec_date = '%s'
             group by a.account_id, a.media_type, a.service_code,a.token_data,a.exec_date
           """ % (ExecDate)
       etl_md.execute_sql("delete from metadb.oe_account_interface where exec_date = '%s' " % (ExecDate))
       ok = etl_md.execute_sql(insert_sql)
       if ok is False:
           msg = "写入目标MySQL筛选子账户表出现异常！！！"
           msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                                  SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                                  TargetTable="%s.%s" % ("", ""),
                                  BeginExecDate=ExecDate,
                                  EndExecDate=ExecDate,
                                  Status="Error",
                                  Log=msg,
                                  Developer="developer")
           set_exit(LevelStatu="red", MSG=msg)

#创建oe异步任务
def get_oe_async_tasks_create_all(AirflowDagId="", AirflowTaskId="", TaskInfo="", MediaType="", ExecDate=""):
    interface_flag = TaskInfo[20]
    group_by = TaskInfo[11]
    fields = TaskInfo[21]
    async_account_file = "/home/ecsage_data/oceanengine/async/%s/%s" % (AirflowDagId, AirflowTaskId)
    async_create_task_file = """%s/async_create.log""" % (async_account_file)
    async_task_exception_file = """%s/async_exception.log""" % (async_account_file)
    celery_task_status_file = """%s/celery_task_status.log""" % (async_account_file)
    os.system("""mkdir -p %s""" % (async_account_file))
    os.system("""rm -f %s/*""" % (async_account_file))
    account_sql = """
      select account_id,'%s' as interface_flag,media_type,service_code,'%s' as group_by,'%s' as fields,token_code 
      from metadb.media_advertiser
    """ % (interface_flag, group_by, fields)
    ok, all_rows = etl_md.get_all_rows(account_sql)
    n = 1
    for data in all_rows:
        status_id = get_oe_async_tasks_create_all_celery.delay(AsyncTaskName="%s" % (n),
                                                               AsyncTaskFile=async_create_task_file,
                                                               AsyncTaskExceptionFile=async_task_exception_file,
                                                               ExecData=data, ExecDate=ExecDate,LocalDir=async_account_file)
        os.system("""echo "%s %s %s %s %s">>%s""" % (status_id, data[0], data[1], data[2], data[3], celery_task_status_file))
        n = n + 1

    # 获取状态
    celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_status_file)
    print("正在等待celery队列执行完成！！！")
    wait_for_celery_status(StatusList=celery_task_id)
    print("celery队列执行完成！！！")
    print("等待重试异常任务！！！")
    rerun_exception_account_tasks(AsyncAccountDir=async_account_file, ExceptionFile=async_task_exception_file,
                                   DataFile=async_create_task_file, CeleryTaskDataFile=celery_task_status_file,
                                   Columns= "account_id,interface_flag,media_type,service_code,group_by,fields,token_data",
                                   InterfaceFlag=interface_flag,ExecDate=ExecDate)
    print("等待重试异常任务完成！！！")
    # 保存MySQL
    columns = """account_id,interface_flag,media_type,service_code,interface_group_by,interface_columns,token_data,task_id,task_name"""
    etl_md.execute_sql("delete from metadb.oe_async_create_task where interface_flag='%s' " % (interface_flag))
    load_data_mysql(AsyncAccountFile=async_account_file, DataFile=async_create_task_file,
                    TableName="oe_async_create_task", Columns=columns)
    #加载因网络抖动写入nfs系统漏数
    sql = """
       insert into metadb.oe_async_create_task
       select a.media_type,a.token_code,a.service_code
              ,a.account_id,'0' as task_id,'999999' as task_name,'##'
              ,'##','%s' as interface_flag
       from metadb.media_advertiser a
       left join metadb.oe_async_create_task b
       on a.account_id = b.account_id
       and a.service_code = b.service_code
       where b.account_id is null
    """%(interface_flag)
    etl_md.execute_sql(sql)

def get_oe_async_tasks_create(AirflowDagId="",AirflowTaskId="",TaskInfo="",MediaType="",ExecDate=""):
    media_type = int(MediaType)
    interface_flag = TaskInfo[20]
    group_by = TaskInfo[11]
    fields = TaskInfo[21]
    async_account_file = "/home/ecsage_data/oceanengine/async/%s/%s/%s"%(AirflowDagId,AirflowTaskId,media_type)
    async_create_task_file = """%s/async_create_%s.log"""%(async_account_file, media_type)
    async_task_exception_file = """%s/async_exception_%s.log"""%(async_account_file, media_type)
    celery_task_status_file = """%s/celery_task_status_%s.log"""%(async_account_file, media_type)
    os.system("""mkdir -p %s"""%(async_account_file))
    os.system("""rm -f %s/*""" % (async_account_file))
    
    source_data_sql = """
            select a.account_id,'%s' as interface_flag,a.media_type,a.service_code,'%s' as group_by
                   ,'%s' as fields,a.token_data
            from metadb.oe_account_interface a
            where a.exec_date = '%s'
            --  and a.account_id = '1645016270409747'
           -- limit 1
            """ % (interface_flag,group_by,fields, ExecDate)
    ok, all_rows = etl_md.get_all_rows(source_data_sql)
    n = 1
    for data in all_rows:
        status_id = get_oe_async_tasks_create_celery.delay(AsyncTaskName="%s" % (n),LocalDir=async_account_file,
                                                           AsyncTaskFile=async_create_task_file,
                                                           AsyncTaskExceptionFile=async_task_exception_file,
                                                           ExecData=data, ExecDate=ExecDate)
        os.system("""echo "%s %s %s %s %s">>%s""" % (status_id, data[0], data[1], data[2], data[3], celery_task_status_file))
        n = n + 1
    #获取状态
    celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_status_file)
    print("正在等待celery队列执行完成！！！")
    #time.sleep(180)
    wait_for_celery_status(StatusList=celery_task_id)
    print("celery队列执行完成！！！")
    print("等待重试异常任务！！！")
    rerun_exception_create_tasks(AsyncAccountDir=async_account_file, ExceptionFile=async_task_exception_file,
                                   DataFile=async_create_task_file, CeleryTaskDataFile=celery_task_status_file,
                                   LogSession="log.logger",InterfaceFlag=interface_flag,ExecDate=ExecDate)
    print("等待重试异常任务完成！！！")
    # 保存MySQL
    columns = """account_id,interface_flag,media_type,service_code,group_by,fields,token_data,task_id,task_name"""
    etl_md.execute_sql("delete from metadb.oe_async_create_task_interface where interface_flag='%s' " % (interface_flag))
    load_data_mysql(AsyncAccountFile=async_account_file, DataFile=async_create_task_file,TableName="oe_async_create_task_interface", Columns=columns)

#存储token
def get_oe_async_tasks_token(MediaType=""):
    mysql_session = set_db_session(SessionType="mysql", SessionHandler="mysql_media")
    token_file = """/tmp/oe_token.log.log"""
    os.system("""rm -f %s"""%(token_file))
    media_types = MediaType.split(",")
    get_media_type = ""
    for media_type in media_types:
        get_media_type = get_media_type + "," + media_type
    get_media_type = get_media_type.replace(",","",1)
    get_service_code_sql = """
             select  service_code
             from big_data_mdg.media_advertiser a
             where media in (%s)
               and is_actived = '1' 
             group by service_code
            """ % (get_media_type)
    ok, all_rows = mysql_session.get_all_rows(get_service_code_sql)
    token_dict = {}
    for data in all_rows:
        token = get_oe_account_token(ServiceCode=data[0])
        token_dict["%s"%(data[0])] = token
    #子账户对应代理商
    get_account_sql = """
                 select  account_id, media, service_code
                 from big_data_mdg.media_advertiser a
                 where media in (%s)
                   and is_actived = '1'
                 group by account_id, media, service_code
                """ % (get_media_type)
    ok, all_rows = mysql_session.get_all_rows(get_account_sql)
    for data in all_rows:
        tokens = token_dict["%s"%(data[2])]
        os.system("""echo "%s %s %s %s">>%s """ % (data[0], data[1],data[2],tokens,token_file))
    insert_sql = """
          load data local infile '%s' into table metadb.media_advertiser fields terminated by ' ' lines terminated by '\\n' (account_id, media_type, service_code,token_code)
        """ % (token_file)
    etl_md.execute_sql("""delete from metadb.media_advertiser""")
    ok = etl_md.local_file_to_mysql(sql=insert_sql)
    if ok is False:
        msg = "获取token出现异常！！！"
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % ("", ""),
                               BeginExecDate="",
                               EndExecDate="",
                               Status="Error",
                               Log=msg,
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)

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
    columns = """exec_date,account_id,media_type,service_code,token_data,task_id,task_name"""
    load_data_mysql(AsyncAccountFile=async_account_file, DataFile=async_notempty_file, TableName="oe_valid_account_interface",Columns=columns)
    #落地没数据
    load_data_mysql(AsyncAccountFile=async_account_file, DataFile=async_empty_file,TableName="oe_not_valid_account_interface",Columns=columns)

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
    media_type = int(MediaType)
    target_handle = TaskInfo[8]
    beeline_handler = "beeline"
    target_db = TaskInfo[9]
    target_table = TaskInfo[10]
    interface_flag = TaskInfo[20]
    airflow_instance = "%s.%s"%(AirflowDagId,AirflowTaskId)
    async_account_file = "/home/ecsage_data/oceanengine/async/%s"%(media_type)
    async_data_exception_file = """%s/%s_%s_exception.%s.log""" % (async_account_file,AirflowDagId,AirflowTaskId,ExecDate)
    async_data_file = """%s/%s_%s_data.%s.log""" % (async_account_file,AirflowDagId,AirflowTaskId,ExecDate)
    celery_task_data_file = """%s/%s_%s_celery_status.%s.log""" % (async_account_file,AirflowDagId,AirflowTaskId,ExecDate)
    os.system("""mkdir -p %s""" % (async_account_file))
    os.system("""rm -f %s/*""" % (async_account_file))
    os.system("""rm -f %s*""" % (async_data_exception_file))
    os.system("""rm -f %s*""" % (celery_task_data_file))
    # 获取子账户
    source_data_sql = """
        -- 正常创建异步任务
        select  a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.task_name 
        from metadb.oe_async_create_task_interface a
        where interface_flag = '%s'
          and media_type = %s
          and task_id <> '0'
                    union all
        -- 异常创建异步任务
        select a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.task_name
        from (select  a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.task_name
              from metadb.oe_async_create_task_interface a
              where interface_flag = '%s'
                and media_type = %s
                and task_id = '0'
           ) a
       left join (select  a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.task_name
              from metadb.oe_async_create_task_interface a
              where interface_flag = '%s'
                and media_type = %s
                and task_id <> '0'
           ) b
       on a.account_id = b.account_id
       and a.service_code = b.service_code
       where b.account_id is null
       group by a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.task_name
    """ % (interface_flag,media_type,interface_flag,media_type,interface_flag,media_type)
    ok, datas = etl_md.get_all_rows(source_data_sql)
    for get_data in datas:
        status_id = get_oe_async_tasks_data_celery.delay(DataFile=async_data_file,ExceptionFile=async_data_exception_file,ExecData=get_data,ExecDate=ExecDate,AirflowInstance=airflow_instance)
        os.system("""echo "%s %s">>%s""" % (status_id,get_data[0], celery_task_data_file))

    #获取状态
    celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_data_file)
    print("正在等待celery队列执行完成，时间：%s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    wait_for_celery_status(StatusList=celery_task_id)
    print("celery队列执行完成，时间：%s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    print("等待重试异常任务，时间：%s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    rerun_exception_downfile_tasks(AsyncAccountDir=async_account_file, ExceptionFile=async_data_exception_file, DataFile=async_data_file, CeleryTaskDataFile=celery_task_data_file,
                                   InterfaceFlag=airflow_instance)
    print("等待重试异常任务完成，时间：%s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    time.sleep(30)
    open_file_session = open(async_data_file, mode="w")
    target_file = os.listdir(async_account_file)
    status_data_file = celery_task_data_file.split("/")[-1]
    for files in target_file:
        if status_data_file in files:
            get_file = "%s/%s"%(async_account_file,files)
            with open(get_file) as lines:
                array = lines.readlines()
                for data in array:
                    get_data1 = data.strip('\n').split(" ")
                    get_celery_job_data(CeleryTaskId=get_data1[0],OpenFileSession=open_file_session)
    open_file_session.close()
    #上传至hdfs
    get_local_file_2_hive(MediaType=MediaType,TargetHandleHive=target_handle, TargetHandleBeeline=beeline_handler,TargetDb=target_db, TargetTable=target_table,AsyncAccountDir=async_account_file,DataFile=async_data_file,ExecDate=ExecDate)

def get_celery_job_data(CeleryTaskId="",OpenFileSession=""):
    set_task = AsyncResult(id=str(CeleryTaskId))
    value = set_task.get()
    print(CeleryTaskId,type(value),"##################################################")
    if 'str' in str(type(value)):
      OpenFileSession.write(value+"\n")
    else:
      OpenFileSession.write(str(value.decode())+"\n")
    OpenFileSession.flush()

#本地数据落地至hive
def get_local_file_2_hive(MediaType="",TargetHandleHive="", TargetHandleBeeline="",TargetDb="",TargetTable="",AsyncAccountDir="",DataFile="",ExecDate=""):
    etl_mid_tmp_table = """%s.%s_%s_tmp""" % (TargetDb, TargetTable, MediaType)
    etl_mid_table = """%s.%s""" % (TargetDb, TargetTable)
    beeline_session = set_db_session(SessionType="beeline", SessionHandler=TargetHandleBeeline)
    target_file = os.listdir(AsyncAccountDir)
    data_file = DataFile.split("/")[-1]
    hdfs_dir = conf.get("Airflow_New", "hdfs_home") #"/tmp/datafolder_new"
    data_file_list = []
    load_sqls = ""
    load_sql_0 = ""
    load_sql = ""
    n = 0
    for files in target_file:
        if data_file in files:
            data_file_list.append("""%s/%s"""%(AsyncAccountDir,files))
            if n == 0:
               load_sql_0 = """load data inpath '%s/%s' OVERWRITE INTO TABLE %s;\n"""%(hdfs_dir,files,etl_mid_tmp_table)
            else:
                load_sql = """load data inpath '%s/%s' INTO TABLE %s;\n"""%(hdfs_dir,files,etl_mid_tmp_table)
            load_sqls = load_sql + load_sqls
            n = n + 1
    load_sqls = load_sql_0 + load_sqls
    if len(load_sqls) == 0:
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (TargetDb, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="API采集没执行！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)
    print("hadoop fs -rmr %s*" % (hdfs_dir + "/" + data_file), "************************************")
    print("hadoop fs -put %s* %s" % (DataFile, hdfs_dir), "************************************")
    os.system("hadoop fs -rmr %s*" % (hdfs_dir + "/" + data_file))
    if data_file_list is not None and len(data_file_list) > 0:
      get_local_hdfs_thread(TargetDb=TargetDb, TargetTable=TargetTable, ExecDate=ExecDate, DataFileList=data_file_list, HDFSDir=hdfs_dir)
    #获取列名
    get_source_columns = os.popen("""grep -v 'empty result' %s |head -1""" % (data_file_list[0]))
    source_columns = get_source_columns.read().split()[0]
    source_columns_list = source_columns.split(",")
    if len(source_columns_list) <= 1:
       print("获取字段出现异常！！！")
    #source_columns = source_columns_list[1]
    columns = ""
    select_colums = ""
    col_n = 0
    #for source_column in source_columns.split(","):
    for source_column in source_columns.split(","):
        columns = columns + ",`" + source_column.strip() + "` string"
        select_colums = select_colums + "," + "request_data[%s]"%(col_n)
        col_n = col_n + 1
    #创建etl_mid临时表，以英文逗号分隔
    create_tmp_sql = """
     drop table if exists %s;
     create table %s(
       request_data string
     )row format delimited fields terminated by '\\001'
     ;
    """%(etl_mid_tmp_table,etl_mid_tmp_table)
    beeline_session.execute_sql(create_tmp_sql)
    create_sql = """
         create table if not exists %s(
           %s
           ,extract_system_time string
         )partitioned by(etl_date string,request_type string)
         row format delimited fields terminated by ','
         ;
        """ % (etl_mid_table, columns.replace(",", "", 1))
    beeline_session.execute_sql(create_sql)
    #load hdfs文件落地至hive
    ok = beeline_session.execute_sql(load_sqls)
    if ok is False:
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (TargetDb, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="hdfs文件落地至hive出现异常！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)
    #落地至etl_mid
    insert_sql = """
     insert overwrite table %s
     partition(etl_date = '%s',request_type = '%s')
     select %s,FROM_UNIXTIME(UNIX_TIMESTAMP()) as extract_system_time
     from(select split(request_data,',') as request_data 
          from(select a.request_data
               from %s a 
              ) tmp
          where trim(request_data) != 'empty result'
          and md5(trim(request_data)) != md5('%s')
     ) tmp1
     ;
    """%(etl_mid_table,ExecDate,MediaType,select_colums.replace(",","",1),etl_mid_tmp_table,source_columns.strip())
    ok = beeline_session.execute_sql(insert_sql)
    if ok is False:
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (TargetDb, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="hdfs文件落地至etl_mid出现异常！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)

#多线程上传hdfs
def local_hdfs_thread(TargetDb="",TargetTable="",ExecDate="",DataFile="",HDFSDir="",arg=None):
    if arg is not None or len(arg) > 0:
       TargetDb = arg["TargetDb"]
       TargetTable = arg["TargetTable"]
       ExecDate = arg["ExecDate"]
       DataFile = arg["DataFile"]
       HDFSDir = arg["HDFSDir"]
       ok_data = os.system("hadoop fs -put %s %s/" % (DataFile, HDFSDir))
       if ok_data != 0:
           msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                                  SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                                  TargetTable="%s.%s" % (TargetDb, TargetTable),
                                  BeginExecDate=ExecDate,
                                  EndExecDate=ExecDate,
                                  Status="Error",
                                  Log="上传本地数据文件至HDFS出现异常！！！",
                                  Developer="developer")
           set_exit(LevelStatu="red", MSG=msg)

#落地数据至ods
def get_etl_mid_2_ods(AirflowDagId="",AirflowTaskId="",TaskInfo="",MediaType="",ExecDate=""):
    media_type = int(MediaType)
    source_handler = TaskInfo[5]
    source_db = TaskInfo[6]
    source_table = TaskInfo[7]
    target_handle = TaskInfo[8]
    hive_handler = "hive"
    target_db = TaskInfo[9]
    target_table = TaskInfo[10]
    key_cols = TaskInfo[12]
    if len(key_cols) == 0 or key_cols is None:
        msg = get_alert_info_d(DagId=AirflowDagId, TaskId=AirflowTaskId,
                               SourceTable="%s.%s" % (source_db, source_table),
                               TargetTable="%s.%s" % (target_db, target_table),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="请确认任务配置指定业务主键字段是否准确！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)
    key = ""
    for keys in key_cols.split(","):
       key = key + ",`" + keys + "`"
    key = key.replace(",","",1)
    hive_session = set_db_session(SessionType="hive", SessionHandler=hive_handler)
    select_target_columns, assign_target_columns,select_source_columns, assign_source_columns = get_table_columns_info(HiveSession=hive_session, SourceDB=source_db, SourceTable=source_table, TargetDB=target_db,
                           TargetTable=target_table,IsTargetPartition="Y")
    insert_sql = """
       insert overwrite table %s.%s
       partition(etl_date='%s')
       select %s
       from(select %s,row_number()over(partition by %s order by 1) as rn
            from %s.%s
            where etl_date = '%s'
           ) tmp
       where rn = 1
    """%(target_db,target_table,ExecDate,select_target_columns,select_source_columns,key,source_db,source_table,ExecDate)
    ok = hive_session.execute_sql(insert_sql)
    if ok is False:
        msg = get_alert_info_d(DagId=AirflowTaskId, TaskId=AirflowTaskId,
                               SourceTable="%s.%s" % (source_db, source_table),
                               TargetTable="%s.%s" % (target_db, target_table),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="写入ods出现异常！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)

#落地数据至snap
def get_ods_2_snap(AirflowDagId="",AirflowTaskId="",TaskInfo="",ExecDate=""):
    source_db = TaskInfo[6]
    source_table = TaskInfo[7]
    hive_handler = "hive"
    beeline_handler = "beeline"
    target_db = TaskInfo[9]
    target_table = TaskInfo[10]

    hive_session = set_db_session(SessionType="hive", SessionHandler=hive_handler)
    beeline_session = set_db_session(SessionType="beeline", SessionHandler=beeline_handler)
    exec_snap_hive_table(AirflowDagId=AirflowDagId, AirflowTaskId=AirflowTaskId, HiveSession=hive_session, BeelineSession=beeline_session,
                         SourceDB=source_db,SourceTable=source_table,TargetDB=target_db, TargetTable=target_table, IsReport=1,
                         KeyColumns="", ExecDate=ExecDate)

def rerun_exception_account_tasks(AsyncAccountDir="",ExceptionFile="",DataFile="",CeleryTaskDataFile="",InterfaceFlag="",Columns="",ExecDate=""):
    async_data_file = """%s/%s"""%(AsyncAccountDir,DataFile.split("/")[-1])
    celery_task_data_file = """%s/%s"""%(AsyncAccountDir,CeleryTaskDataFile.split("/")[-1])
    async_data_exception_file = """%s/%s""" % (AsyncAccountDir, ExceptionFile.split("/")[-1])
    # 先保留第一次
    delete_sql = """delete from metadb.oe_async_exception_create_tasks_interface where interface_flag = '%s' """ % (InterfaceFlag)
    etl_md.execute_sql(delete_sql)
    columns = Columns
    table_name = "oe_async_exception_create_tasks_interface"
    save_exception_tasks(AsyncAccountDir=AsyncAccountDir, ExceptionFile=ExceptionFile, TableName=table_name,Columns=columns)
    n = 10
    for i in range(n):
        sql = """
              select distinct %s
              from metadb.oe_async_exception_create_tasks_interface a
              where interface_flag = '%s'
            """ % (columns,InterfaceFlag)
        ok, datas = etl_md.get_all_rows(sql)
        if datas is not None and len(datas) > 0:
            print("开始第%s次重试异常，时间：%s" % (i + 1, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            for data in datas:
                status_id = get_oe_async_tasks_create_all_celery.delay(AsyncTaskName="%s" % (n),
                                                                       AsyncTaskFile=async_data_file,
                                                                       AsyncTaskExceptionFile=async_data_exception_file,
                                                                       ExecData=data, ExecDate=ExecDate,LocalDir=AsyncAccountDir)
                os.system("""echo "%s %s">>%s""" % (status_id, data[0], celery_task_data_file + ".%s" % (i)))
                n = n + 1
            celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_data_file + ".%s" % i)
            wait_for_celery_status(StatusList=celery_task_id)
            delete_sql = """delete from metadb.oe_async_exception_create_tasks_interface where interface_flag = '%s' """ % (InterfaceFlag)
            etl_md.execute_sql(delete_sql)
            save_exception_tasks(AsyncAccountDir=AsyncAccountDir, ExceptionFile=ExceptionFile, TableName=table_name,Columns=columns)
            print("结束第%s次重试异常，时间：%s" % (i + 1, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            # 判断结果是否还有异常
            ex_sql = """
                         select %s
                         from metadb.oe_async_exception_create_tasks_interface a
                         where interface_flag = '%s'
                         limit 1
                  """ % (Columns,InterfaceFlag)
            ok, ex_datas = etl_md.get_all_rows(ex_sql)
            if ex_datas is not None and len(ex_datas) > 0:
                print("休眠中...，时间：%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                time.sleep(60)
    ex_sql = """
             select %s
             from metadb.oe_async_exception_create_tasks_interface a
             where interface_flag = '%s'
        """ % (Columns,InterfaceFlag)
    ok, ex_datas = etl_md.get_all_rows(ex_sql)
    if ex_datas is not None and len(ex_datas) > 0:
        print("还有特别异常任务存在！！！")
        print(ex_datas[0])

def rerun_exception_downfile_tasks(AsyncAccountDir="",ExceptionFile="",DataFile="",CeleryTaskDataFile="",InterfaceFlag=""):
    async_data_file = """%s/%s"""%(AsyncAccountDir,DataFile.split("/")[-1])
    celery_task_data_file = """%s/%s"""%(AsyncAccountDir,CeleryTaskDataFile.split("/")[-1])
    async_data_exception_file = """%s/%s""" % (AsyncAccountDir, ExceptionFile.split("/")[-1])
    # 先保留第一次
    delete_sql = """delete from metadb.oe_async_exception_create_tasks_interface where interface_flag = '%s' """ % (InterfaceFlag)
    etl_md.execute_sql(delete_sql)
    columns = """account_id,media_type,service_code,token_data,task_id,task_name,interface_flag"""
    table_name = "oe_async_exception_create_tasks_interface"
    save_exception_tasks(AsyncAccountDir=AsyncAccountDir, ExceptionFile=ExceptionFile, TableName=table_name,Columns=columns)
    n = 100
    for i in range(n):
        sql = """
              select distinct %s
              from metadb.oe_async_exception_create_tasks_interface a
              where interface_flag = '%s'
            """ % (columns,InterfaceFlag)
        ok, datas = etl_md.get_all_rows(sql)
        if datas is not None and len(datas) > 0:
            print("开始第%s次重试异常，时间：%s" % (i + 1, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            for data in datas:
                status_id = get_oe_async_tasks_data_celery.delay(DataFile=async_data_file,ExceptionFile=async_data_exception_file,
                                                                 ExecData=data)
                os.system("""echo "%s %s">>%s""" % (status_id, data[0], celery_task_data_file + ".%s" % (i)))
            celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_data_file + ".%s" % i)
            wait_for_celery_status(StatusList=celery_task_id)
            delete_sql = """delete from metadb.oe_async_exception_create_tasks_interface where interface_flag = '%s' """ % (InterfaceFlag)
            etl_md.execute_sql(delete_sql)
            save_exception_tasks(AsyncAccountDir=AsyncAccountDir, ExceptionFile=ExceptionFile, TableName=table_name,Columns=columns)
            print("结束第%s次重试异常，时间：%s" % (i + 1, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            # 判断结果是否还有异常
            ex_sql = """
                         select a.account_id,a.interface_flag,a.media_type,a.service_code,a.group_by,a.fields,a.token_data
                         from metadb.oe_async_exception_create_tasks_interface a
                         where interface_flag = '%s'
                         limit 1
                  """ % (InterfaceFlag)
            ok, ex_datas = etl_md.get_all_rows(ex_sql)
            if ex_datas is not None and len(ex_datas) > 0:
                print("休眠中...，时间：%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                time.sleep(60)
    ex_sql = """
             select a.account_id,a.interface_flag,a.media_type,a.service_code,a.group_by,a.fields,a.token_data
             from metadb.oe_async_exception_create_tasks_interface a
             where interface_flag = '%s'
        """ % (InterfaceFlag)
    ok, ex_datas = etl_md.get_all_rows(ex_sql)
    if ex_datas is not None and len(ex_datas) > 0:
        print("还有特别异常任务存在！！！")
        print(ex_datas[0])

#
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
    n = 100
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
               status_id = get_oe_async_tasks_create_celery.delay(AsyncTaskName="%s" % (i), LocalDir=AsyncAccountDir,
                                                                  AsyncTaskFile=async_data_file,
                                                                  AsyncTaskExceptionFile=async_data_exception_file,
                                                                  ExecData=data, ExecDate=ExecDate)
               os.system("""echo "%s %s">>%s""" % (status_id, data[0], celery_task_data_file+".%s"%(i)))
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