# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: interface_oe_async_2_hive.py
# @Software: PyCharm
# function info：定义tc异步接口

from celery.result import AsyncResult
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_alert_info_d
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.common.base.airflow_instance import Airflow
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_oe_async_tasks_status as get_oe_async_tasks_status_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_tc_async_tasks_data_return as get_tc_async_tasks_data_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_oe_async_tasks_create as get_oe_async_tasks_create_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_oe_async_tasks_create_all as get_oe_async_tasks_create_all_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_oe_create_async_tasks as get_oe_create_async_tasks_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_tc_add_async_tasks as get_tc_add_async_tasks_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_oe_status_async_tasks as get_oe_status_async_tasks_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_tc_status_async_tasks as get_tc_status_async_tasks_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_local_hdfs_thread
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_oe_tasks_status
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.get_account_tokens import get_oe_account_token
from ecsage_bigdata_etl_engineering.common.base.sync_method import get_table_columns_info
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.get_data_2_snap import exec_snap_hive_table
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_data_2_snap
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread
from ecsage_bigdata_etl_engineering.common.base.get_config import Conf
import os
import time
import ast
import json
import socket
import ast

conf = Conf().conf
etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")

# 入口方法
def main(TaskInfo, **kwargs):
    global airflow
    global developer
    global regexp_extract_column
    airflow = Airflow(kwargs)
    media_type = TaskInfo[1]
    task_type = TaskInfo[4]
    task_id = TaskInfo[2]
    is_filter_account = int(TaskInfo[22])
    source_db = TaskInfo[6]
    source_table = TaskInfo[7]
    target_db = TaskInfo[9]
    target_table = TaskInfo[10]
    key_columns= TaskInfo[12]
    is_report = TaskInfo[25]
    print(TaskInfo, "####################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    exec_date = airflow.execution_date_utc8_str[0:10]
    beeline_session = set_db_session(SessionType="beeline", SessionHandler="beeline")
    hive_session = set_db_session(SessionType="hive", SessionHandler="hive")
    """任务类型，1：创建异步任务，0：获取异步任务状态，2：获取异步任务数据，3：ods同步，4：snap同步，5：获取token"""
    if task_type == 2:
        get_tc_async_tasks_data(AirflowDagId=airflow.dag, AirflowTaskId=airflow.task, TaskInfo=TaskInfo,
                                MediaType=media_type, ExecDate=exec_date)
    elif task_type == 3:
        get_etl_mid_2_ods(AirflowDagId=airflow.dag, AirflowTaskId=airflow.task, TaskInfo=TaskInfo, MediaType=media_type,
                          ExecDate=exec_date)
    elif task_type == 4:
        #get_ods_2_snap(AirflowDagId=airflow.dag, AirflowTaskId=airflow.task, TaskInfo=TaskInfo, ExecDate=exec_date)
        get_data_2_snap(HiveSession=hive_session, BeelineSession=beeline_session, SourceDB=source_db, SourceTable=source_table,
                             TargetDB=target_db, TargetTable=target_table, IsReport=is_report
                            ,KeyColumns=key_columns, ExecDate=exec_date
                            ,DagId=airflow.dag,TaskId=airflow.task)
    elif task_type == 5:
        get_oe_async_tasks_token(MediaType=media_type)
    elif task_type == 1:
        get_tc_async_tasks_add(AirflowDagId=airflow.dag, AirflowTaskId=airflow.task, TaskInfo=TaskInfo,
                                             MediaType=media_type, ExecDate=exec_date,IsFilterAccount=is_filter_account)
    elif task_type == 0:
        get_tc_async_tasks_status(AirflowDagId=airflow.dag, AirflowTaskId=airflow.task, MediaType=media_type,
                                         TaskInfo=TaskInfo, ExecDate=exec_date
                                         )
    elif task_type == 7:
        account_info_data(AirflowDagId=airflow.dag, AirflowTaskId=airflow.task, TaskInfo=TaskInfo, MediaType=media_type,
                          ExecDate=exec_date
                                         )

def get_oe_async_tasks_status_all(AirflowDagId="", AirflowTaskId="", TaskInfo="", ExecDate=""):
    media_type = 2
    interface_flag = TaskInfo[20]
    hostname = socket.gethostname()
    async_account_file = "/data/ecsage_data/oceanengine/%s/async/%s/%s" % (hostname,AirflowDagId, AirflowTaskId)
    async_status_exception_file = """%s/async_status_exception.log""" % (async_account_file)
    async_notempty_file = """%s/async_notempty.log""" % (async_account_file)
    async_empty_file = """%s/async_empty.log""" % (async_account_file)
    celery_task_status_file = """%s/celery_task_status_file.log""" % (async_account_file)
    os.system("""mkdir -p %s""" % (async_account_file))
    os.system("""rm -f %s/*""" % (async_account_file))
    os.system("""chmod -R 777 %s""" % (async_account_file))
    etl_md.execute_sql("""delete from metadb.oe_valid_account_interface where  exec_date = '%s' """ % (ExecDate))
    etl_md.execute_sql("""delete from metadb.oe_not_valid_account_interface where exec_date = '%s' """ % (ExecDate))
    # 获取子账户
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
    """ % (interface_flag, interface_flag, interface_flag, interface_flag)
    ok, datas = etl_md.get_all_rows(source_data_sql)
    if datas is not None and len(datas) > 0:
        for get_data in datas:
            status_id = get_oe_async_tasks_status_celery.delay(AsyncNotemptyFile=async_notempty_file,
                                                               AsyncEmptyFile=async_empty_file,
                                                               AsyncStatusExceptionFile=async_status_exception_file,
                                                               ExecData=get_data, ExecDate=ExecDate)
            os.system("""echo "%s">>%s""" % (status_id, celery_task_status_file))
        # 获取状态
        celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_status_file)
        print("正在等待celery队列执行完成！！！")
        wait_for_celery_status(StatusList=celery_task_id)
        print("celery队列执行完成！！！")
        print("等待重试异常任务！！！")
        rerun_exception_tasks(AsyncAccountDir=async_account_file, ExceptionFile=async_status_exception_file,
                              AsyncNotemptyFile=async_notempty_file, AsyncemptyFile=async_empty_file,
                              CeleryTaskStatusFile=celery_task_status_file, ExecDate=ExecDate)
        print("重试异常任务执行完成！！！")
        time.sleep(60)
        columns = "exec_date,account_id,media_type,service_code,token_data,task_id,task_name"
        # 落地有数据
        load_data_mysql(AsyncAccountFile=async_account_file, DataFile=async_notempty_file,
                        TableName="oe_valid_account_interface", Columns=columns)
        # 落地没数据
        load_data_mysql(AsyncAccountFile=async_account_file, DataFile=async_empty_file,
                        TableName="oe_not_valid_account_interface", Columns=columns)
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

def get_tc_async_tasks_status(AirflowDagId="", AirflowTaskId="", MediaType="",TaskInfo="", ExecDate=""):
    media_type = MediaType
    level = TaskInfo[20]
    local_time = time.strftime("%Y-%m-%d_%H_%M_%S", time.localtime())
    hostname = socket.gethostname()
    local_dir = """/data/ecsage_data/tencentengine/%s/async/%s/%s/%s""" % (hostname,ExecDate, AirflowDagId, AirflowTaskId)
    data_task_file = """%s/data_%s.log""" % (local_dir, AirflowTaskId)
    task_exception_file = "%s/task_exception_file.log" % (local_dir)
    celery_task_status_file = "%s/celery_task_status_file.log" % (local_dir)
    data_file = data_task_file.split("/")[-1].split(".")[0] + "_%s." % (local_time) + data_task_file.split("/")[-1].split(".")[1]
    os.system("""mkdir -p %s""" % (local_dir))
    os.system("""rm -f %s/*""" % (local_dir))
    os.system("""chmod -R 777 %s""" % (local_dir))
    etl_md.execute_sql("""delete from metadb.tc_valid_account_interface where  exec_date = '%s' and airflow_task_id='%s.%s' """ % (ExecDate,AirflowDagId, AirflowTaskId))
    # 获取子账户
    source_data_sql = """
        select a.account_id,a.media_type,a.service_code,a.token_data,a.task_id
        from metadb.tc_async_add_task a
        where task_id <> '111111'
          and media_type = '%s'
          and level = '%s'
          and task_id <> '0'
        group by a.account_id,a.media_type,a.service_code,a.token_data,a.task_id
    """ % (media_type, level)
    ok, datas = etl_md.get_all_rows(source_data_sql)
    params = []
    if datas is not None and len(datas) > 0:
        for get_data in datas:
            task_id = get_data[4]
            status_id = get_tc_status_async_tasks_celery.delay(ExecDate=ExecDate,DataFileDir=local_dir,
                                                               DataFile=data_file,UrlPath="async_reports/get",
                                                               TaskId=task_id, Token=get_data[3], ReturnAccountId=get_data[0],
                                                               ServiceCode=get_data[2],MediaType=media_type,
                                                               TaskFlag="%s.%s"%(AirflowDagId, AirflowTaskId),
                                                               TaskExceptionFile=task_exception_file
                                                               )
            os.system("""echo "%s">>%s""" % (status_id, celery_task_status_file))
        # 获取状态
        celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_status_file)
        print("正在等待celery队列执行完成！！！")
        wait_for_celery_status(StatusList=celery_task_id)
        print("celery队列执行完成！！！")
        print("等待重试异常任务！！！")
        rerun_async_create_tasks_exception(DataFileDir=local_dir, ExceptionFile=task_exception_file,Flag="status",
                                           DataFile=data_file, CeleryTaskDataFile=celery_task_status_file,
                                           InterfaceFlag="%s.%s" % (AirflowDagId, AirflowTaskId),ExecDate=ExecDate,
                                           Columns="interface_url,interface_param_json,service_code,account_id,media_type,token,interface_flag"
                                           )
        print("重试异常任务执行完成！！！")
        time.sleep(15)
        columns = """exec_date,account_id,media_type,service_code,token_data,task_id,remark,airflow_task_id,trace_id,file_info_list,page_info"""
        # 落地有数据
        load_data_mysql_tab(AsyncAccountFile=local_dir, DataFile=data_file,
                        TableName="tc_valid_account_interface", Columns=columns)


# 创建oe异步任务
def get_oe_async_tasks_create_all(AirflowDagId="", AirflowTaskId="", TaskInfo="", MediaType="", ExecDate=""):
    interface_flag = TaskInfo[20]
    group_by = TaskInfo[11]
    fields = TaskInfo[21]
    hostname = socket.gethostname()
    async_account_file = "/data/ecsage_data/oceanengine/%s/async/%s/%s" % (hostname,AirflowDagId, AirflowTaskId)
    async_create_task_file = """%s/async_create.log""" % (async_account_file)
    async_task_exception_file = """%s/async_exception.log""" % (async_account_file)
    celery_task_status_file = """%s/celery_task_status.log""" % (async_account_file)
    os.system("""mkdir -p %s""" % (async_account_file))
    os.system("""rm -f %s/*""" % (async_account_file))
    os.system("""chmod -R 777 %s"""%(async_account_file))
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
                                                               ExecData=data, ExecDate=ExecDate,
                                                               LocalDir=async_account_file)
        os.system(
            """echo "%s %s %s %s %s">>%s""" % (status_id, data[0], data[1], data[2], data[3], celery_task_status_file))
        n = n + 1

    # 获取状态
    celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_status_file)
    print("正在等待celery队列执行完成！！！")
    wait_for_celery_status(StatusList=celery_task_id)
    print("celery队列执行完成！！！")
    print("等待重试异常任务！！！")
    rerun_exception_account_tasks(AsyncAccountDir=async_account_file, ExceptionFile=async_task_exception_file,
                                  DataFile=async_create_task_file, CeleryTaskDataFile=celery_task_status_file,
                                  Columns="account_id,interface_flag,media_type,service_code,group_by,fields,token_data",
                                  InterfaceFlag=interface_flag, ExecDate=ExecDate)
    print("等待重试异常任务完成！！！")
    # 保存MySQL
    columns = """account_id,interface_flag,media_type,service_code,interface_group_by,interface_columns,token_data,task_id,task_name"""
    etl_md.execute_sql("delete from metadb.oe_async_create_task where interface_flag='%s' " % (interface_flag))
    load_data_mysql(AsyncAccountFile=async_account_file, DataFile=async_create_task_file,
                    TableName="oe_async_create_task", Columns=columns)
    # 加载因网络抖动写入nfs系统漏数
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
    """ % (interface_flag)
    etl_md.execute_sql(sql)



# 创建tc异步任务
def get_tc_async_tasks_add(AirflowDagId="", AirflowTaskId="", TaskInfo="", MediaType="", ExecDate="",IsFilterAccount=""):
    level = TaskInfo[20]
    task_flag = """%s.%s"""%(AirflowDagId,AirflowTaskId)
    group_by = TaskInfo[11]
    report_fields = TaskInfo[21]
    time_line = TaskInfo[23]
    granularity = TaskInfo[24]
    local_time = time.strftime("%Y-%m-%d_%H_%M_%S", time.localtime())
    hostname = socket.gethostname()
    local_dir = """/data/ecsage_data/tencentengine/%s/async/%s/%s/%s""" % (hostname,ExecDate, AirflowDagId, AirflowTaskId)
    data_task_file = """%s/data_%s.log""" % (local_dir, AirflowTaskId)
    task_exception_file = "%s/task_exception_file.log" % (local_dir)
    celery_task_status_file = "%s/celery_task_status_file.log" % (local_dir)
    data_file = data_task_file.split("/")[-1].split(".")[0] + "_%s." % (local_time) + data_task_file.split("/")[-1].split(".")[1]
    os.system("""mkdir -p %s""" % (local_dir))
    os.system("""rm -f %s/*""" % (local_dir))
    os.system("""chmod -R 777 %s""" % (local_dir))
    if IsFilterAccount == 1:
      account_sql = """
         select ma.account_id,'%s' as level,ma.media_type,ma.service_code,'%s' as group_by
                ,'%s' as report_fields,ma.token, '%s' as time_line,'%s' as granularity
         from (select account_id from metadb.adgroup_info group by account_id)ai inner join metadb.tc_service_account ma on ai.account_id = ma.account_id
         where ma.media_type = '%s'
      """ % (level, group_by, report_fields, time_line, granularity, MediaType)
    else:
      account_sql = """
         select a.account_id,'%s' as level,a.media_type,a.service_code,'%s' as group_by
                ,'%s' as report_fields,a.token_data
         from metadb.oe_account_interface a
         where a.exec_date = '%s'
      """ % (level, group_by, report_fields, ExecDate)
    ok, all_rows = etl_md.get_all_rows(account_sql)
    n = 1
    parameters  = {}
    parameters["group_by"] = group_by.split(",")
    parameters["date"] = ExecDate
    for data in all_rows:
        token = data[6]
        account_id = data[0]
        level = data[1]
        time_line = data[7]
        granularity = data[8]
        service_code = data[3]
        media_type = data[2]
        if report_fields is None or len(report_fields) == 0:
            parameters["account_id"] = str(account_id)
            parameters["task_name"] = str(n)
        else:
            parameters["account_id"] = str(account_id)
            parameters["task_name"] = str(n)
            report_fields_list = report_fields.split(",")
            parameters["level"] = str(level)
            parameters["report_fields"] = report_fields_list
            parameters["time_line"] = str(time_line)
            parameters["granularity"] = str(granularity)

        status_id = get_tc_add_async_tasks_celery.delay(DataFileDir=local_dir,DataFile=data_file,
                                                           UrlPath="async_reports/add", ParamJson=parameters,
                                                           Token=token, ReturnAccountId=account_id, ServiceCode=service_code,
                                                           Level=level, TaskFlag=task_flag,
                                                           MediaType=media_type, TaskExceptionFile=task_exception_file
                                                          )
        os.system("""echo "%s %s %s %s %s">>%s""" % (status_id, data[0], data[1], data[2], data[3], celery_task_status_file))
        n = n + 1

    # 获取状态
    celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_status_file)
    print("正在等待celery队列执行完成！！！")
    wait_for_celery_status(StatusList=celery_task_id)
    print("celery队列执行完成！！！")
    print("等待重试异常任务！！！")
    rerun_async_create_tasks_exception(DataFileDir=local_dir, ExceptionFile=task_exception_file,Flag="create",
                                       DataFile=data_file,CeleryTaskDataFile=celery_task_status_file,
                                       InterfaceFlag="%s.%s##%s"%(AirflowDagId,AirflowTaskId,level),
                                       Columns="interface_url,interface_param_json,service_code,account_id,media_type,token,interface_flag",
                                      GroupBy=group_by,ReportFieldsList=report_fields_list)
    print("等待重试异常任务完成！！！")
    # 保存MySQL
    columns = """account_id,level,media_type,service_code,token_data,task_id,return_data,airflow_task_id"""
    etl_md.execute_sql("delete from metadb.tc_async_add_task where airflow_task_id='%s.%s' " % (AirflowDagId,AirflowTaskId))
    load_data_mysql(AsyncAccountFile=local_dir, DataFile=data_file,TableName="tc_async_add_task", Columns=columns)
    # 加载因网络抖动写入nfs系统漏数
    sql = """
       insert into metadb.tc_async_add_task
       (media_type,token_data,service_code,account_id,task_id,task_name,level)
       select b.media_type,b.token_data,b.service_code
              ,a.account_id,'0' as task_id,'999999' as task_name,'%s' as level
       from (select account_id,mt,service_code from metadb.adgroup_info group by account_id,mt,service_code) a
       left join metadb.tc_async_add_task b
       on a.account_id = b.account_id
       and a.service_code = b.service_code
       and b.media_type = '%s'
       and b.airflow_task_id = '%s.%s'
       where b.account_id is null
         and a.mt = '%s'
    """ % (level,media_type,AirflowDagId,AirflowTaskId,media_type)
    etl_md.execute_sql(sql)

#异常重试
def rerun_async_create_tasks_exception(ExecDate="",DataFileDir="",ExceptionFile="",DataFile="",CeleryTaskDataFile="",InterfaceFlag="",Columns="",Flag="create",GroupBy="",ReportFieldsList=""):
    celery_task_data_file = """%s/%s"""%(DataFileDir,CeleryTaskDataFile.split("/")[-1])
    #先保留第一次
    delete_sql = """delete from metadb.oe_sync_exception_tasks_interface_bak where interface_flag = '%s' """ % (InterfaceFlag)
    etl_md.execute_sql(delete_sql)
    columns = Columns
    db_name = "metadb"
    table_name = "oe_sync_exception_tasks_interface_bak"
    save_exception_tasks(AsyncAccountDir=DataFileDir,ExceptionFile=ExceptionFile,TableName=table_name,Columns=columns)
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
           print("开始第%s次重试异常，时间：%s"%(i+1,time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
           for data in datas:
             param_json = ast.literal_eval(json.loads(json.dumps(str(data[1]).replace("""'""","""\""""))))
             """
             interface_url,interface_param_json,service_code,account_id,media_type,token,interface_flag
             """
             if Flag == "create":
               param_json["report_fields"] = ReportFieldsList
               param_json["group_by"] = GroupBy.split(",")
               status_id = get_tc_add_async_tasks_celery.delay(DataFileDir=DataFileDir, DataFile=DataFile,
                                                                UrlPath=data[0],ParamJson=param_json,
                                                                Token=data[5], ReturnAccountId=data[3],
                                                                ServiceCode=data[2],Level=str(data[6]).split("##")[1],
                                                                TaskFlag=str(data[6]).split("##")[0],
                                                                MediaType=data[4],
                                                                TaskExceptionFile=ExceptionFile
                                                                )
             elif Flag == "status":
               status_id = get_tc_status_async_tasks_celery.delay(ExecDate=ExecDate, DataFileDir=DataFileDir,
                                                                  DataFile=DataFile,UrlPath=data[0],
                                                                  TaskId=data[1], Token=data[5],
                                                                  ReturnAccountId=data[3],ServiceCode=data[2], MediaType=data[4],
                                                                  TaskFlag=str(data[6]).split("##")[0],
                                                                  TaskExceptionFile=ExceptionFile
                                                                )
             os.system("""echo "%s %s">>%s""" % (status_id, data[3], celery_task_data_file+".%s"%(i)))
           celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_data_file + ".%s"%i)
           wait_for_celery_status(StatusList=celery_task_id)
           delete_sql = """delete from %s.%s where interface_flag = '%s' """ % (db_name,table_name,InterfaceFlag)
           etl_md.execute_sql(delete_sql)
           save_exception_tasks(AsyncAccountDir=DataFileDir, ExceptionFile=ExceptionFile,TableName=table_name,Columns=columns)
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
                 time.sleep(60)
               else:
                 time.sleep(60)
    ex_sql = """
         select %s
         from %s.%s a
         where interface_flag = '%s'
    """% (columns,db_name,table_name,InterfaceFlag)
    ok, ex_datas = etl_md.get_all_rows(ex_sql)
    if ex_datas is not None and len(ex_datas) > 0:
        print("还有特别异常任务存在！！！")
        print(ex_datas[0])

def get_oe_async_tasks_create(AirflowDagId="", AirflowTaskId="", TaskInfo="", MediaType="", ExecDate=""):
    media_type = int(MediaType)
    interface_flag = TaskInfo[20]
    group_by = TaskInfo[11]
    fields = TaskInfo[21]
    hostname = socket.gethostname()
    async_account_file = "/data/ecsage_data/oceanengine/%s/async/%s/%s/%s" % (hostname,AirflowDagId, AirflowTaskId, media_type)
    async_create_task_file = """%s/async_create_%s.log""" % (async_account_file, media_type)
    async_task_exception_file = """%s/async_exception_%s.log""" % (async_account_file, media_type)
    celery_task_status_file = """%s/celery_task_status_%s.log""" % (async_account_file, media_type)
    os.system("""mkdir -p %s""" % (async_account_file))
    os.system("""rm -f %s/*""" % (async_account_file))
    os.system("""chmod -R 777 %s""" % (async_account_file))

    source_data_sql = """
            select a.account_id,'%s' as interface_flag,a.media_type,a.service_code,'%s' as group_by
                   ,'%s' as fields,a.token_data
            from metadb.oe_account_interface a
            where a.exec_date = '%s'
            --  and a.account_id = '1645016270409747'
           -- limit 1
            """ % (interface_flag, group_by, fields, ExecDate)
    ok, all_rows = etl_md.get_all_rows(source_data_sql)
    n = 1
    for data in all_rows:
        status_id = get_oe_async_tasks_create_celery.delay(AsyncTaskName="%s" % (n), LocalDir=async_account_file,
                                                           AsyncTaskFile=async_create_task_file,
                                                           AsyncTaskExceptionFile=async_task_exception_file,
                                                           ExecData=data, ExecDate=ExecDate)
        os.system(
            """echo "%s %s %s %s %s">>%s""" % (status_id, data[0], data[1], data[2], data[3], celery_task_status_file))
        n = n + 1
    # 获取状态
    celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_status_file)
    print("正在等待celery队列执行完成！！！")
    # time.sleep(180)
    wait_for_celery_status(StatusList=celery_task_id)
    print("celery队列执行完成！！！")
    print("等待重试异常任务！！！")
    rerun_exception_create_tasks(AsyncAccountDir=async_account_file, ExceptionFile=async_task_exception_file,
                                 DataFile=async_create_task_file, CeleryTaskDataFile=celery_task_status_file,
                                 LogSession="log.logger", InterfaceFlag=interface_flag, ExecDate=ExecDate)
    print("等待重试异常任务完成！！！")
    # 保存MySQL
    columns = """account_id,interface_flag,media_type,service_code,group_by,fields,token_data,task_id,task_name"""
    etl_md.execute_sql(
        "delete from metadb.oe_async_create_task_interface where interface_flag='%s' " % (interface_flag))
    load_data_mysql(AsyncAccountFile=async_account_file, DataFile=async_create_task_file,
                    TableName="oe_async_create_task_interface", Columns=columns)
    # 检测是否有媒体创建异步异常任务
    ######time.sleep(60)
    ######sql = """
    ######  select account_id,task_id,token_data
    ######  from metadb.oe_async_create_task_interface
    ######  where interface_flag='%s'
    ######    and task_id != '0'
    ######"""% (interface_flag)
    ######ok,datas = etl_md.get_all_rows(sql)
    ######for data in datas:
    ###### try:
    ######   resp_data = get_oe_tasks_status(AccountId=data[0], TaskId=data[1], Token=data[2])
    ######   task_status = resp_data["data"]["list"][0]["task_status"]
    ######   if task_status == "ASYNC_TASK_STATUS_FAILED":
    ######       msg = "创建异步任务出现异常！！！"
    ######       msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
    ######                              SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
    ######                              TargetTable="%s.%s" % ("", ""),
    ######                              BeginExecDate="",
    ######                              EndExecDate="",
    ######                              Status="Error",
    ######                              Log=msg,
    ######                              Developer="developer")
    ######       set_exit(LevelStatu="red", MSG=msg)
    ###### except:
    ######    pass


# 存储token
def get_oe_async_tasks_token(MediaType=""):
    mysql_session = set_db_session(SessionType="mysql", SessionHandler="mysql_media")
    token_file = """/tmp/oe_token.log.log"""
    os.system("""rm -f %s""" % (token_file))
    media_types = MediaType.split(",")
    get_media_type = ""
    for media_type in media_types:
        get_media_type = get_media_type + "," + media_type
    get_media_type = get_media_type.replace(",", "", 1)
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
        token_dict["%s" % (data[0])] = token
    # 子账户对应代理商
    get_account_sql = """
                 select  account_id, media, service_code
                 from big_data_mdg.media_advertiser a
                 where media in (%s)
                   and is_actived = '1'
                 group by account_id, media, service_code
                """ % (get_media_type)
    ok, all_rows = mysql_session.get_all_rows(get_account_sql)
    for data in all_rows:
        tokens = token_dict["%s" % (data[2])]
        os.system("""echo "%s %s %s %s">>%s """ % (data[0], data[1], data[2], tokens, token_file))
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


def get_oe_async_tasks_status(MediaType="", ExecDate=""):
    media_type = MediaType
    hostname = socket.gethostname()
    async_account_file = "/data/ecsage_data/oceanengine/%s/async"%(hostname)
    async_status_exception_file = """%s/async_status_exception_%s.log""" % (async_account_file, media_type)
    async_notempty_file = """%s/async_notempty_%s.log""" % (async_account_file, media_type)
    async_empty_file = """%s/async_empty_%s.log""" % (async_account_file, media_type)
    async_not_succ_file = """%s/async_not_succ_file_%s.log""" % (async_account_file, media_type)
    celery_task_status_file = """%s/celery_task_status_file_%s.log""" % (async_account_file, media_type)
    os.system("""mkdir -p %s""" % (async_account_file))
    os.system("""rm -f %s*""" % (async_not_succ_file))
    os.system("""rm -f %s*""" % (async_notempty_file))
    os.system("""rm -f %s*""" % (async_empty_file))
    os.system("""rm -f %s*""" % (async_status_exception_file))
    os.system("""rm -f /tmp/sql_%s.sql""")
    os.system("""rm -f %s*""" % (celery_task_status_file))
    etl_md.execute_sql("""delete from metadb.oe_valid_account_interface where media_type=%s and exec_date = '%s' """ % (
    media_type, ExecDate))
    etl_md.execute_sql(
        """delete from metadb.oe_not_valid_account_interface where media_type=%s and exec_date = '%s' """ % (
        media_type, ExecDate))
    # 获取子账户
    source_data_sql = """
                 select distinct account_id,media_type,service_code,token_data,task_id,task_name
                 from metadb.oe_async_task_interface
                 where media_type = %s
    """ % (media_type)
    ok, datas = etl_md.get_all_rows(source_data_sql)
    for get_data in datas:
        status_id = get_oe_async_tasks_status_celery.delay(AsyncNotemptyFile=async_notempty_file,
                                                           AsyncEmptyFile=async_empty_file,
                                                           AsyncStatusExceptionFile=async_status_exception_file,
                                                           ExecData=get_data, ExecDate=ExecDate)
        os.system("""echo "%s">>%s""" % (status_id, celery_task_status_file))
    # 获取状态
    celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_status_file)
    print("正在等待celery队列执行完成！！！")
    wait_for_celery_status(StatusList=celery_task_id)
    print("celery队列执行完成！！！")
    print("等待重试异常任务！！！")
    rerun_exception_tasks(AsyncAccountDir=async_account_file, ExceptionFile=async_status_exception_file,
                          AsyncNotemptyFile=async_notempty_file, AsyncemptyFile=async_empty_file,
                          CeleryTaskStatusFile=celery_task_status_file, ExecDate=ExecDate)
    print("重试异常任务执行完成！！！")
    time.sleep(60)
    # 落地有数据
    columns = """exec_date,account_id,media_type,service_code,token_data,task_id,task_name"""
    load_data_mysql(AsyncAccountFile=async_account_file, DataFile=async_notempty_file,
                    TableName="oe_valid_account_interface", Columns=columns)
    # 落地没数据
    load_data_mysql(AsyncAccountFile=async_account_file, DataFile=async_empty_file,
                    TableName="oe_not_valid_account_interface", Columns=columns)


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
            # 判断是否成功
            if get_celery_job_status(CeleryTaskId=status) is False:
                status_false.append(status)
            else:
                pass
        if len(status_false) > 0:
            wait_mins = 600
            if sleep_num <= wait_mins:
                min = 60
                print("等待第%s次%s秒" % (sleep_num, min))
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


# 重跑异常任务
def rerun_exception_tasks(AsyncAccountDir="", ExceptionFile="", AsyncNotemptyFile="", AsyncemptyFile="",
                          CeleryTaskStatusFile="", ExecDate=""):
    exception_file = ExceptionFile.split("/")[-1]
    async_notempty_file = """%s/%s.last_runned""" % (AsyncAccountDir, AsyncNotemptyFile.split("/")[-1])
    async_empty_file = """%s/%s.last_runned""" % (AsyncAccountDir, AsyncemptyFile.split("/")[-1])
    celery_task_status_file = """%s/%s.last_runned""" % (AsyncAccountDir, CeleryTaskStatusFile.split("/")[-1])
    async_status_exception_file = """%s/%s.last_runned""" % (AsyncAccountDir, ExceptionFile.split("/")[-1])
    target_file = os.listdir(AsyncAccountDir)
    exception_file_list = []
    for files in target_file:
        if exception_file in files:
            exception_file_list.append(files)
            exception_dir_file = """%s/%s""" % (AsyncAccountDir, files)
            with open(exception_dir_file) as lines:
                array = lines.readlines()
                for data in array:
                    get_data = data.strip('\n').split(" ")
                    status_id = get_oe_async_tasks_status_celery.delay(AsyncNotemptyFile=async_notempty_file,
                                                                       AsyncEmptyFile=async_empty_file,
                                                                       AsyncStatusExceptionFile=async_status_exception_file,
                                                                       ExecData=get_data, ExecDate=ExecDate)
                    os.system("""echo "%s %s">>%s""" % (status_id, get_data[0], celery_task_status_file))
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
    return celery_task_id, status_wait


def load_data_mysql(AsyncAccountFile="", DataFile="", TableName="", Columns=""):
    target_file = os.listdir(AsyncAccountFile)
    for files in target_file:
        if str(DataFile.split("/")[-1]).split(".")[0] in files and """.lock""" not in files:
            print(files, "###############################################")
            # 记录子账户
            insert_sql = """
                  load data local infile '%s' into table metadb.%s fields terminated by ' ' lines terminated by '\\n' (%s)
               """ % (AsyncAccountFile + "/" + files, TableName, Columns)
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
def load_data_mysql_tab(AsyncAccountFile="", DataFile="", TableName="", Columns=""):
    target_file = os.listdir(AsyncAccountFile)
    for files in target_file:
        if str(DataFile.split("/")[-1]).split(".")[0] in files and """.lock""" not in files:
            print(files, "###############################################")
            # 记录子账户
            insert_sql = """
                  load data local infile '%s' into table metadb.%s fields terminated by '\t' lines terminated by '\\n' (%s)
               """ % (AsyncAccountFile + "/" + files, TableName, Columns)
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


def get_tc_async_tasks_data(AirflowDagId="", AirflowTaskId="", TaskInfo="", MediaType="", ExecDate=""):
    media_type = int(MediaType)
    target_handle = TaskInfo[8]
    beeline_handler = "beeline"
    target_db = TaskInfo[9]
    target_table = TaskInfo[10]
    level = TaskInfo[20]
    task_flag = "%s.%s" % (AirflowDagId, AirflowTaskId)
    local_time = time.strftime("%Y-%m-%d_%H_%M_%S", time.localtime())
    hostname = socket.gethostname()
    local_dir = """/data/ecsage_data/tencentengine/%s/async/%s/%s/%s""" % (hostname,ExecDate, AirflowDagId, AirflowTaskId)
    celery_status_file = "%s/celery_status_file.log" % (local_dir)
    data_task_file = """%s/data_%s.log""" % (local_dir, AirflowTaskId)
    task_exception_file = "%s/task_exception_file.log" % (local_dir)
    data_file = data_task_file.split("/")[-1].split(".")[0] + "%s." % (local_time) + \
                data_task_file.split("/")[-1].split(".")[1]
    os.system("""mkdir -p %s""" % (local_dir))
    os.system("""rm -f %s/*""" % (local_dir))
    os.system("""chmod -R 777 %s""" % (local_dir))
    # 获取子账户
    source_data_sql = """
            select a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.file_info_list
            from metadb.tc_valid_account_interface a
                 inner join metadb.tc_async_add_task b
            where  a.media_type = '%s'
              and b.media_type = '%s' and a.task_id=b.task_id
              and b.level = '%s'
            group by a.account_id,a.media_type,a.service_code,a.token_data,a.task_id,a.file_info_list
    """ % (media_type, media_type, level)
    ok, datas = etl_md.get_all_rows(source_data_sql)
    params = {}
    for get_data in datas:
        params["account_id"] = get_data[0]
        params["task_id"] = get_data[4]
        file_info_list = ast.literal_eval(get_data[5])
        file_id = file_info_list[0]["file_id"]
        params["file_id"] = file_id
        status_id = get_tc_async_tasks_data_celery.delay(DataFileDir=local_dir, DataFile=data_file,
                                                         UrlPath="async_report_files/get",
                                                         ParamJson=params, Token=get_data[3],
                                                         ReturnAccountId=get_data[0],
                                                         ServiceCode=get_data[2],
                                                         TaskExceptionFile=task_exception_file)
        os.system("""echo "%s %s">>%s""" % (status_id, get_data[0], celery_status_file))
    # 获取状态
    celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_status_file)
    print("正在等待celery队列执行完成，时间：%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    wait_for_celery_status(StatusList=celery_task_id)
    print("celery队列执行完成，时间：%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    print("等待重试异常任务，时间：%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    rerun_exception_async_tasks(DataFileDir=local_dir, ExceptionFile=task_exception_file, DataFile=data_file,
                                CeleryTaskDataFile=celery_status_file, InterfaceFlag=level,
                                Columns="interface_url,interface_param_json,service_code,account_id,interface_flag,token"
                                )
    print("等待重试异常任务完成，时间：%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    time.sleep(30)
    # 上传至hdfs
    target_file = os.listdir(local_dir)
    data_task_file_list = []
    for files in target_file:
        if str(data_task_file.split("/")[-1]).split(".")[0] in files and '.lock' not in files:
            data_task_file_list.append("%s/%s" % (local_dir, files))
    get_local_file_2_hive(MediaType=MediaType, TargetHandleHive=target_handle, TargetHandleBeeline=beeline_handler,
                          TargetDb=target_db, TargetTable=target_table, AsyncAccountDir=local_dir, DataFile=data_file,
                          ExecDate=ExecDate)


def get_celery_job_data(CeleryTaskId="", OpenFileSession=""):
    set_task = AsyncResult(id=str(CeleryTaskId))
    value = set_task.get()
    print(CeleryTaskId, type(value), "##################################################")
    if 'str' in str(type(value)):
        OpenFileSession.write(value + "\n")
    else:
        OpenFileSession.write(str(value.decode()) + "\n")
    OpenFileSession.flush()


# 本地数据落地至hive
def get_local_file_2_hive(MediaType="", TargetHandleHive="", TargetHandleBeeline="", TargetDb="", TargetTable="",
                          AsyncAccountDir="", DataFile="", ExecDate=""):
    etl_mid_tmp_table = """%s.%s_%s_tmp""" % (TargetDb, TargetTable, MediaType)
    etl_mid_table = """%s.%s""" % (TargetDb, TargetTable)
    beeline_session = set_db_session(SessionType="beeline", SessionHandler=TargetHandleBeeline)
    target_file = os.listdir(AsyncAccountDir)
    data_file = str(DataFile.split("/")[-1]).split(".")[0]
    hdfs_dir = conf.get("Airflow_New", "hdfs_home")  # "/tmp/datafolder_new"
    data_file_list = []
    load_sqls = ""
    load_sql_0 = ""
    load_sql = ""
    n = 0
    for files in target_file:
        if data_file in files and '.lock' not in files:
            data_file_list.append("""%s/%s""" % (AsyncAccountDir, files))
            if n == 0:
                load_sql_0 = """load data inpath '%s/%s' OVERWRITE INTO TABLE %s;\n""" % (
                hdfs_dir, files, etl_mid_tmp_table)
            else:
                load_sql = """load data inpath '%s/%s' INTO TABLE %s;\n""" % (hdfs_dir, files, etl_mid_tmp_table)
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
    # 获取列名
    get_source_columns = os.popen(
        """grep -v 'empty result' %s |head -1|sed "s/trace_id#&#ds{.*}trace_id#&#ds//g" """ % (data_file_list[0]))
    source_columns = get_source_columns.read().split()[0]
    source_columns_list = source_columns.split(",")
    if len(source_columns_list) <= 1:
        print("获取字段出现异常！！！")
    # source_columns = source_columns_list[1]
    columns = ""
    select_colums = ""
    col_n = 0
    # for source_column in source_columns.split(","):
    for source_column in source_columns.split(","):
        columns = columns + ",`" + source_column.strip() + "` string"
        select_colums = select_colums + "," + "request_data[%s]" % (col_n)
        col_n = col_n + 1
    # 创建etl_mid临时表，以英文逗号分隔
    create_tmp_sql = """
     drop table if exists %s;
     create table %s(
       request_data string
     )row format delimited fields terminated by '\\001'
     ;
    """ % (etl_mid_tmp_table, etl_mid_tmp_table)
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
    print("hadoop fs -rmr %s*" % (hdfs_dir + "/" + data_file), "************************************")
    print("hadoop fs -put %s* %s" % (DataFile, hdfs_dir), "************************************")
    os.system("hadoop fs -rmr %s*" % (hdfs_dir + "/" + data_file))
    if data_file_list is not None and len(data_file_list) > 0:
        get_local_hdfs_thread(TargetDb=TargetDb, TargetTable=TargetTable, ExecDate=ExecDate,
                              DataFileList=data_file_list, HDFSDir=hdfs_dir)
    # load hdfs文件落地至hive
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
    # 落地至etl_mid
    insert_sql = """
     insert overwrite table %s
     partition(etl_date = '%s',request_type = '%s')
     select %s,FROM_UNIXTIME(UNIX_TIMESTAMP()) as extract_system_time
     from(select split(request_data,',') as request_data 
          from(select a.request_data
               from %s a 
              ) tmp
          where trim(request_data) != 'empty result' and request_data is not null and  trim(request_data) != ' '
          and trim(request_data) not like 'trace_id#&#ds{%s' and trim(request_data) != ''
     ) tmp1
     ;
    """ % (etl_mid_table, ExecDate, MediaType, select_colums.replace(",", "", 1), etl_mid_tmp_table, "%")
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


# 多线程上传hdfs
def local_hdfs_thread(TargetDb="", TargetTable="", ExecDate="", DataFile="", HDFSDir="", arg=None):
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


# 落地数据至ods
def get_etl_mid_2_ods(AirflowDagId="", AirflowTaskId="", TaskInfo="", MediaType="", ExecDate=""):
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
    key = key.replace(",", "", 1)
    hive_session = set_db_session(SessionType="hive", SessionHandler=hive_handler)
    select_target_columns, assign_target_columns, select_source_columns, assign_source_columns = get_table_columns_info(
        HiveSession=hive_session, SourceDB=source_db, SourceTable=source_table, TargetDB=target_db,
        TargetTable=target_table, IsTargetPartition="Y")
    insert_sql = """
       insert overwrite table %s.%s
       partition(etl_date='%s')
       select %s
       from(select %s,row_number()over(partition by %s order by 1) as rn
            from %s.%s
            where etl_date = '%s'
           ) tmp
       where rn = 1
    """ % (target_db, target_table, ExecDate, select_target_columns,
           select_source_columns.replace("""null as `returns_account_id`""",
                                         """`advertiser_id` as `returns_account_id`"""), key, source_db, source_table,
           ExecDate)
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


# 落地数据至snap
# def get_ods_2_snap(AirflowDagId="", AirflowTaskId="", TaskInfo="", ExecDate=""):
#     source_db = TaskInfo[6]
#     source_table = TaskInfo[7]
#     hive_handler = "hive"
#     beeline_handler = "beeline"
#     target_db = TaskInfo[9]
#     target_table = TaskInfo[10]
#
#     hive_session = set_db_session(SessionType="hive", SessionHandler=hive_handler)
#     beeline_session = set_db_session(SessionType="beeline", SessionHandler=beeline_handler)
#     exec_snap_hive_table(AirflowDagId=AirflowDagId, AirflowTaskId=AirflowTaskId, HiveSession=hive_session,
#                          BeelineSession=beeline_session,
#                          SourceDB=source_db, SourceTable=source_table, TargetDB=target_db, TargetTable=target_table,
#                          IsReport=1,
#                          KeyColumns="", ExecDate=ExecDate)


def rerun_exception_account_tasks(AsyncAccountDir="", ExceptionFile="", DataFile="", CeleryTaskDataFile="",
                                  InterfaceFlag="", Columns="", ExecDate=""):
    async_data_file = """%s/%s""" % (AsyncAccountDir, DataFile.split("/")[-1])
    celery_task_data_file = """%s/%s""" % (AsyncAccountDir, CeleryTaskDataFile.split("/")[-1])
    async_data_exception_file = """%s/%s""" % (AsyncAccountDir, ExceptionFile.split("/")[-1])
    # 先保留第一次
    delete_sql = """delete from metadb.oe_async_exception_create_tasks_interface where interface_flag = '%s' """ % (
        InterfaceFlag)
    etl_md.execute_sql(delete_sql)
    columns = Columns
    table_name = "oe_async_exception_create_tasks_interface"
    save_exception_tasks(AsyncAccountDir=AsyncAccountDir, ExceptionFile=ExceptionFile, TableName=table_name,
                         Columns=columns)
    n = 10
    for i in range(n):
        sql = """
              select distinct %s
              from metadb.oe_async_exception_create_tasks_interface a
              where interface_flag = '%s'
            """ % (columns, InterfaceFlag)
        ok, datas = etl_md.get_all_rows(sql)
        if datas is not None and len(datas) > 0:
            print("开始第%s次重试异常，时间：%s" % (i + 1, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            for data in datas:
                status_id = get_oe_async_tasks_create_all_celery.delay(AsyncTaskName="%s" % (n),
                                                                       AsyncTaskFile=async_data_file,
                                                                       AsyncTaskExceptionFile=async_data_exception_file,
                                                                       ExecData=data, ExecDate=ExecDate,
                                                                       LocalDir=AsyncAccountDir)
                os.system("""echo "%s %s">>%s""" % (status_id, data[0], celery_task_data_file + ".%s" % (i)))
                n = n + 1
            celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_data_file + ".%s" % i)
            wait_for_celery_status(StatusList=celery_task_id)
            delete_sql = """delete from metadb.oe_async_exception_create_tasks_interface where interface_flag = '%s' """ % (
                InterfaceFlag)
            etl_md.execute_sql(delete_sql)
            save_exception_tasks(AsyncAccountDir=AsyncAccountDir, ExceptionFile=ExceptionFile, TableName=table_name,
                                 Columns=columns)
            print("结束第%s次重试异常，时间：%s" % (i + 1, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            # 判断结果是否还有异常
            ex_sql = """
                         select %s
                         from metadb.oe_async_exception_create_tasks_interface a
                         where interface_flag = '%s'
                         limit 1
                  """ % (Columns, InterfaceFlag)
            ok, ex_datas = etl_md.get_all_rows(ex_sql)
            if ex_datas is not None and len(ex_datas) > 0:
                print("休眠中...，时间：%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                time.sleep(60)
    ex_sql = """
             select %s
             from metadb.oe_async_exception_create_tasks_interface a
             where interface_flag = '%s'
        """ % (Columns, InterfaceFlag)
    ok, ex_datas = etl_md.get_all_rows(ex_sql)
    if ex_datas is not None and len(ex_datas) > 0:
        print("还有特别异常任务存在！！！")
        print(ex_datas[0])


def rerun_exception_async_tasks(DataFileDir="", ExceptionFile="", DataFile="", CeleryTaskDataFile="", InterfaceFlag="",
                                Columns=""):
    celery_task_data_file = """%s/%s""" % (DataFileDir, CeleryTaskDataFile.split("/")[-1])
    # 先保留第一次
    delete_sql = """delete from metadb.oe_sync_exception_tasks_interface_bak where interface_flag = '%s' """ % (
        InterfaceFlag)
    etl_md.execute_sql(delete_sql)
    columns = Columns
    db_name = "metadb"
    table_name = "oe_sync_exception_tasks_interface_bak"
    save_exception_tasks(AsyncAccountDir=DataFileDir, ExceptionFile=ExceptionFile, TableName=table_name,
                         Columns=columns)
    #
    n = 15
    for i in range(n):
        sql = """
          select distinct %s
          from %s.%s a
          where interface_flag = '%s'
        """ % (columns, db_name, table_name, InterfaceFlag)
        ok, datas = etl_md.get_all_rows(sql)
        if datas is not None and len(datas) > 0:
            print("开始第%s次重试异常，时间：%s" % (i + 1, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            for data in datas:
                param_json = ast.literal_eval(json.loads(json.dumps(str(data[1]).replace("""'""", """\""""))))
                status_id = get_tc_async_tasks_data_celery.delay(DataFileDir=DataFileDir, DataFile=DataFile,
                                                                 UrlPath="async_report_files/get",
                                                                 ParamJson=param_json, Token=data[5],
                                                                 ReturnAccountId=data[3],
                                                                 ServiceCode=data[2], TaskFlag=InterfaceFlag,
                                                                 TaskExceptionFile=ExceptionFile
                                                                 )
                os.system("""echo "%s %s">>%s""" % (status_id, data[0], celery_task_data_file + ".%s" % (i)))
            celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_data_file + ".%s" % i)
            wait_for_celery_status(StatusList=celery_task_id)
            delete_sql = """delete from %s.%s where interface_flag = '%s' """ % (db_name, table_name, InterfaceFlag)
            etl_md.execute_sql(delete_sql)
            save_exception_tasks(AsyncAccountDir=DataFileDir, ExceptionFile=ExceptionFile, TableName=table_name,
                                 Columns=columns)
            print("结束第%s次重试异常，时间：%s" % (i + 1, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            # 判断结果是否还有异常
            ex_sql = """
                     select %s
                     from %s.%s a
                     where interface_flag = '%s'
                     limit 1
              """ % (columns, db_name, table_name, InterfaceFlag)
            ok, ex_datas = etl_md.get_all_rows(ex_sql)
            if ex_datas is not None and len(ex_datas) > 0:
                print("休眠中...，时间：%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                if i == 0:
                    time.sleep(60)
                else:
                    time.sleep(60)
    ex_sql = """
         select %s
         from %s.%s a
         where interface_flag = '%s'
    """ % (columns, db_name, table_name, InterfaceFlag)
    ok, ex_datas = etl_md.get_all_rows(ex_sql)
    if ex_datas is not None and len(ex_datas) > 0:
        print("还有特别异常任务存在！！！")
        print(ex_datas[0])


def rerun_exception_downfile_tasks(AsyncAccountDir="", ExceptionFile="", DataFile="", CeleryTaskDataFile="",
                                   InterfaceFlag=""):
    async_data_file = """%s/%s""" % (AsyncAccountDir, DataFile.split("/")[-1])
    celery_task_data_file = """%s/%s""" % (AsyncAccountDir, CeleryTaskDataFile.split("/")[-1])
    async_data_exception_file = """%s/%s""" % (AsyncAccountDir, ExceptionFile.split("/")[-1])
    # 先保留第一次
    delete_sql = """delete from metadb.oe_async_exception_create_tasks_interface where interface_flag = '%s' """ % (
        InterfaceFlag)
    etl_md.execute_sql(delete_sql)
    columns = """account_id,media_type,service_code,token_data,task_id,task_name,interface_flag"""
    table_name = "oe_async_exception_create_tasks_interface"
    save_exception_tasks(AsyncAccountDir=AsyncAccountDir, ExceptionFile=ExceptionFile, TableName=table_name,
                         Columns=columns)
    n = 100
    for i in range(n):
        sql = """
              select distinct %s
              from metadb.oe_async_exception_create_tasks_interface a
              where interface_flag = '%s'
            """ % (columns, InterfaceFlag)
        ok, datas = etl_md.get_all_rows(sql)
        if datas is not None and len(datas) > 0:
            print("开始第%s次重试异常，时间：%s" % (i + 1, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            for data in datas:
                status_id = get_tc_async_tasks_data_celery.delay(DataFile=async_data_file,
                                                                 ExceptionFile=async_data_exception_file,
                                                                 ExecData=data)
                os.system("""echo "%s %s">>%s""" % (status_id, data[0], celery_task_data_file + ".%s" % (i)))
            celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_data_file + ".%s" % i)
            wait_for_celery_status(StatusList=celery_task_id)
            delete_sql = """delete from metadb.oe_async_exception_create_tasks_interface where interface_flag = '%s' """ % (
                InterfaceFlag)
            etl_md.execute_sql(delete_sql)
            save_exception_tasks(AsyncAccountDir=AsyncAccountDir, ExceptionFile=ExceptionFile, TableName=table_name,
                                 Columns=columns)
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
def save_exception_tasks(AsyncAccountDir="", ExceptionFile="", TableName="", Columns=""):
    exception_file = ExceptionFile.split("/")[-1]
    exception_file_list = []
    target_file = os.listdir(AsyncAccountDir)
    for files in target_file:
        if exception_file in files:
            exception_file_list.append((AsyncAccountDir, files))
    if exception_file_list is not None and len(exception_file_list) > 0:
        for file in exception_file_list:
            load_data_mysql(AsyncAccountFile=file[0], DataFile=file[1], TableName=TableName, Columns=Columns)
            os.system("""rm -f %s/%s""" % (file[0], file[1]))


def rerun_exception_create_tasks(AsyncAccountDir="", ExceptionFile="", DataFile="", CeleryTaskDataFile="",
                                 LogSession="", InterfaceFlag="", ExecDate=""):
    async_data_file = """%s/%s""" % (AsyncAccountDir, DataFile.split("/")[-1])
    celery_task_data_file = """%s/%s""" % (AsyncAccountDir, CeleryTaskDataFile.split("/")[-1])
    async_data_exception_file = """%s/%s""" % (AsyncAccountDir, ExceptionFile.split("/")[-1])
    # 先保留第一次
    delete_sql = """delete from metadb.oe_async_exception_create_tasks_interface where interface_flag = '%s' """ % (
        InterfaceFlag)
    etl_md.execute_sql(delete_sql)
    columns = """account_id,interface_flag,media_type,service_code,group_by,fields,token_data"""
    table_name = "oe_async_exception_create_tasks_interface"
    save_exception_tasks(AsyncAccountDir=AsyncAccountDir, ExceptionFile=ExceptionFile, TableName=table_name,
                         Columns=columns)
    #
    n = 100
    for i in range(n):
        sql = """
          select distinct a.account_id,a.interface_flag,a.media_type,a.service_code,a.group_by,a.fields,a.token_data
          from metadb.oe_async_exception_create_tasks_interface a
          where interface_flag = '%s'
        """ % (InterfaceFlag)
        ok, datas = etl_md.get_all_rows(sql)
        if datas is not None and len(datas) > 0:
            print("开始第%s次重试异常，时间：%s" % (i + 1, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            for data in datas:
                status_id = get_oe_async_tasks_create_celery.delay(AsyncTaskName="%s" % (i), LocalDir=AsyncAccountDir,
                                                                   AsyncTaskFile=async_data_file,
                                                                   AsyncTaskExceptionFile=async_data_exception_file,
                                                                   ExecData=data, ExecDate=ExecDate)
                os.system("""echo "%s %s">>%s""" % (status_id, data[0], celery_task_data_file + ".%s" % (i)))
            celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_data_file + ".%s" % i)
            wait_for_celery_status(StatusList=celery_task_id)
            delete_sql = """delete from metadb.oe_async_exception_create_tasks_interface where interface_flag = '%s' """ % (
                InterfaceFlag)
            etl_md.execute_sql(delete_sql)
            save_exception_tasks(AsyncAccountDir=AsyncAccountDir, ExceptionFile=ExceptionFile, TableName=table_name,
                                 Columns=columns)
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
                if i == 0:
                    time.sleep(360)
                else:
                    time.sleep(180)
    ex_sql = """
         select a.account_id,a.interface_flag,a.media_type,a.service_code,a.group_by,a.fields,a.token_data
         from metadb.oe_async_exception_create_tasks_interface a
         where interface_flag = '%s'
    """ % (InterfaceFlag)
    ok, ex_datas = etl_md.get_all_rows(ex_sql)
    if ex_datas is not None and len(ex_datas) > 0:
        print("还有特别异常任务存在！！！")
        print(ex_datas[0])
def account_info_data(AirflowDagId="", AirflowTaskId="", MediaType="",TaskInfo="", ExecDate=""):
    beeline_session = set_db_session(SessionType="beeline", SessionHandler="beeline")
    hostname = socket.gethostname()
    async_account_file = "/data/ecsage_data/tencentengine/%s/async/%s/%s/%s" % (hostname,ExecDate, AirflowDagId, AirflowTaskId)
    adgroup_info_data_file = """%s/adgroup_info_data_file.log""" % (async_account_file)
    os.system("""mkdir -p %s""" % (async_account_file))
    os.system("""chmod -R 777 %s""" % (async_account_file))
    os.system("""rm -f %s/*""" % (async_account_file))
    filter_sql = """
    select account_id,adgroup_id,mt,service_code from (
     select account_id, adgroup_id, '1' as mt, service_code from
     (select tc_inder.account_id, tc_inder.campaign_id, tc_inder.adgroup_id, ma.service_code, sum(cast(tc_inder.view_count as double)) as vc, sum(cast(tc_inder.cost as double)) as cost
     from ods.tc_getdailyreports_daily_reports tc_inder
     left join ods.media_account_big_data_mdg_media_advertiser ma
     on tc_inder.account_id = ma.account_id
     where tc_inder.etl_date='%s' and tc_inder.time_line='REQUEST_TIME' and request_type='1'
     group by tc_inder.account_id, tc_inder.campaign_id, tc_inder.adgroup_id, ma.service_code) a
     where --a.vc > 0 or
     cost > 0
     union all
     select account_id, adgroup_id, '101' as mt, service_code from
     (select tc_outer.account_id, tc_outer.campaign_id, tc_outer.adgroup_id, ma.service_code, sum(cast(tc_outer.view_count as double)) as vc, sum(cast(tc_outer.cost as double)) as cost
     from ods.tc_getdailyreports_daily_reports tc_outer
     left join ods.media_account_big_data_mdg_media_advertiser ma
     on tc_outer.account_id = ma.account_id
     where tc_outer.etl_date='%s' and tc_outer.time_line='REQUEST_TIME' and request_type='101'
     group by tc_outer.account_id, tc_outer.campaign_id, tc_outer.adgroup_id, ma.service_code) a
     where -- a.vc > 0 or
     cost > 0
     union all
     select account_id, adgroup_id, '102' as mt, service_code from
     (select tc_wechat.account_id, tc_wechat.campaign_id, tc_wechat.adgroup_id, ma.service_code, sum(cast(tc_wechat.view_count as double)) as vc, sum(cast(tc_wechat.cost as double)) as cost
     from ods.tc_getdailyreports_wechat_daily_reports tc_wechat
     left join ods.media_account_big_data_mdg_media_advertiser ma
     on tc_wechat.account_id = ma.account_id
     where tc_wechat.etl_date='%s' and tc_wechat.time_line='REPORTING_TIME'
     group by tc_wechat.account_id, tc_wechat.campaign_id, tc_wechat.adgroup_id, ma.service_code) a
     where --a.vc > 0 or
     cost > 0
     ) t  where service_code is not null and service_code !='' order by mt
     """ % (ExecDate, ExecDate, ExecDate)
    print("过滤sql：%s" % (filter_sql))
    ok = beeline_session.execute_sql_result_2_local_file(sql=filter_sql,file_name=adgroup_info_data_file)
    if ok is False:
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % ("metadb", "adgroup_info"),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="导出文件出现异常！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)
    etl_md.execute_sql("truncate metadb.adgroup_info")
    columns = """account_id,adgroup_id,mt,service_code"""
    load_data_mysql_tab(AsyncAccountFile=async_account_file, DataFile=adgroup_info_data_file,
                    TableName="adgroup_info",  Columns=columns)
