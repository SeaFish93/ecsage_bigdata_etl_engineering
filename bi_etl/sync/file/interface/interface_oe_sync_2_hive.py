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
    print(TaskInfo,"####################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    exec_date = airflow.execution_date_utc8_str[0:10]
    target_db = TaskInfo[14]
    target_table = TaskInfo[15]
    hive_session = set_db_session(SessionType="hive", SessionHandler="hive")
    beeline_session = set_db_session(SessionType="beeline", SessionHandler="beeline")
    if Level == "file":
       get_sync_interface_2_local(BeelineSession=beeline_session,TargetDB=target_db,TargetTable=target_table,
                                  AirflowDag=airflow.dag, AirflowTask=airflow.task,
                                  TaskInfo=TaskInfo, ExecDate=exec_date)

def set_sync_pages_number(DataList="",ParamJson="",UrlPath="",SyncDir="",PageTaskFile="",CelerySyncTaskFile="",DataFileDir="",DataFile="",IsFilter=""):
    param_json = ParamJson
    db_data = DataList
    for data in db_data:
        param_json["advertiser_id"] = data[0]
        param_json["service_code"] = data[2]
        param_json["filtering"]["campaign_ids"] = [int(data[3])]
        task_flag = data[4]
        celery_task_id = get_oe_sync_tasks_data_return_celery.delay(ParamJson=str(param_json), UrlPath=UrlPath,
                                                                    PageTaskFile=PageTaskFile,
                                                                    DataFileDir=DataFileDir,DataFile=DataFile,
                                                                    TaskFlag=task_flag
                                                                    )
        os.system("""echo "%s %s %s %s">>%s""" % (celery_task_id, data[0], data[1], data[2], CelerySyncTaskFile))
    # 获取状态
    celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=CelerySyncTaskFile)
    print("正在等待获取页数celery队列执行完成！！！")
    wait_for_celery_status(StatusList=celery_task_id)
    print("获取页数celery队列执行完成！！！")
    print("end %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    # 保存MySQL
    columns = """page_num,account_id,service_code,remark,data,request_filter,flag"""
    load_data_mysql(AsyncAccountFile=SyncDir, DataFile=PageTaskFile, TableName="oe_sync_page_interface",Columns=columns)

def get_sync_interface_2_local(BeelineSession="",TargetDB="",TargetTable="",AirflowDag="",AirflowTask="",TaskInfo="",ExecDate=""):
  local_time = time.strftime("%Y-%m-%d_%H_%M_%S", time.localtime())
  local_dir = """/home/ecsage_data/oceanengine/sync/%s/%s/%s"""%(ExecDate,AirflowDag,AirflowTask)
  celery_get_page_status = """%s/celery_get_page_status.log"""%(local_dir)
  celery_get_data_status = "%s/celery_get_data_status.log"%(local_dir)
  page_task_file = "%s/page_task_file.log"%(local_dir)
  data_task_file = """%s/data_task_file.log"""%(local_dir)
  tmp_data_task_file = """%s/tmp_data_file.log""" % (local_dir)
  task_exception_file = "%s/task_exception_file.log"%(local_dir)
  param_json = ast.literal_eval(json.loads(json.dumps(TaskInfo[5])))
  #设置查询日期
  param_json["start_date"] = ExecDate
  param_json["end_date"] = ExecDate
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
  is_filter = False
  #判断是否从列表过滤
  if filter_db_name is not None and len(filter_db_name) > 0:
      filter_sql = """
      select concat_ws(' ',%s,'%s.%s') from %s.%s where etl_date='%s' %s group by %s
      """%(filter_column_name,AirflowDag,AirflowTask,filter_db_name,filter_table_name,ExecDate,filter_config,filter_column_name)
      os.system("""spark-sql -S -e"%s"> %s"""%(filter_sql,tmp_data_task_file))
      etl_md.execute_sql("delete from metadb.oe_sync_filter_info where flag = '%s.%s' "%(AirflowDag,AirflowTask))
      columns = """advertiser_id,filter_id,flag"""
      load_data_mysql(AsyncAccountFile=local_dir, DataFile=tmp_data_task_file, TableName="oe_sync_filter_info",Columns=columns)
      sql = """
            select a.account_id, a.media_type, a.service_code,b.filter_id as id,b.flag
            from metadb.oe_account_interface a
            inner join metadb.oe_sync_filter_info b
            on a.account_id = b.advertiser_id
            where a.exec_date = '%s'
              and b.flag = '%s.%s'
            group by a.account_id, a.media_type, a.service_code,b.filter_id,b.flag
       """%(ExecDate,AirflowDag,AirflowTask)
      is_filter = True
  else:
      sql = """
            select a.account_id, a.media_type, a.service_code,'' as id,'%s.%s'
            from metadb.oe_account_interface a
            where a.exec_date = '%s'
            group by a.account_id, a.media_type, a.service_code
       """%(AirflowDag,AirflowTask,ExecDate)
  ok,db_data = etl_md.get_all_rows(sql)
  etl_md.execute_sql("delete from metadb.oe_sync_page_interface where flag = '%s.%s' "%(AirflowDag,AirflowTask))
  set_sync_pages_number(DataList=db_data, ParamJson=param_json, UrlPath=url_path, SyncDir=local_dir,IsFilter=is_filter,
                        PageTaskFile=page_task_file, CelerySyncTaskFile=celery_get_page_status,DataFileDir=local_dir,
                        DataFile=data_task_file.split("/")[-1].split(".")[0]+"_1_%s."%(local_time)+data_task_file.split("/")[-1].split(".")[1])
  #重试异常
  n = 2
  for i in range(n):
    sql = """
      select tmp1.account_id, '222' media_type, tmp1.service_code,trim(replace(replace(tmp1.request_filter,'[',''),']','')),tmp1.flag
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
   group by tmp1.account_id, tmp1.service_code,tmp1.request_filter,tmp1.request_filter,tmp1.flag
      union all
   select account_id, '222' media_type, service_code,trim(replace(replace(request_filter,'[',''),']','')),flag
   from metadb.oe_sync_page_interface a 
   where page_num = 0
     and remark = '正常'
     and data like '%s'
     and flag = '%s.%s'
  group by account_id, service_code,request_filter,request_filter,flag
  """%(AirflowDag,AirflowTask,AirflowDag,AirflowTask,"%OK%",AirflowDag,AirflowTask)
    ok, db_data = etl_md.get_all_rows(sql)
    if db_data is not None and len(db_data) > 0:
       os.system("""rm -f %s*""" % (celery_get_page_status.split(".")[0]))
       os.system("""rm -f %s*""" % (page_task_file.split(".")[0]))
       os.system("""rm -f %s*""" % (celery_get_data_status.split(".")[0]))
       os.system("""rm -f %s*""" % (task_exception_file.split(".")[0]))
       set_sync_pages_number(DataList=db_data, ParamJson=param_json, UrlPath=url_path, SyncDir=local_dir,IsFilter=is_filter,
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
    from metadb.oe_sync_page_interface a where page_num > 1
    and flag = '%s.%s'
    group by a.account_id,  a.service_code,a.page_num,a.request_filter
  """%(AirflowDag,AirflowTask)
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
     #获取数据文件
     target_file = os.listdir(local_dir)
     data_task_file_list = []
     for files in target_file:
         if str(data_task_file.split("/")[-1]).split(".")[0] in files:
             data_task_file_list.append("%s/%s"%(local_dir, files))
     #数据落地至etl_mid
     load_data_2_etl_mid(BeelineSession=BeelineSession, LocalFileList=data_task_file_list, TargetDB=TargetDB,
                         TargetTable=TargetTable, ExecDate=ExecDate)

def load_data_2_etl_mid(BeelineSession="",LocalFileList="",TargetDB="",TargetTable="",ExecDate=""):
    if LocalFileList is None and len(LocalFileList) == 0:
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (TargetDB, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="API采集没执行！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)
    mid_sql = """
        drop table if exists %s.%s;
        create table if not exists %s.%s
        (
         request_data string
        )partitioned by(etl_date string,request_type string,delete_type string)
        row format delimited fields terminated by '\\001' 
        ;
    """ % (TargetDB,TargetTable,TargetDB,TargetTable)
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
                         partition(etl_date='{exec_date}')
                         ;\n
            """.format(hdfs_dir=hdfs_dir, file_name=local_file.split("/")[-1], target_db=TargetDB,
                       target_table=TargetTable,exec_date=ExecDate)
        else:
            load_table_sql = """
                         load data inpath '{hdfs_dir}/{file_name}' INTO TABLE {target_db}.{target_table}
                         partition(etl_date='{exec_date}')
                         ;\n
                     """.format(hdfs_dir=hdfs_dir, file_name=local_file.split("/")[-1],
                                target_db=TargetDB,target_table=TargetTable,exec_date=ExecDate)
        load_table_sqls = load_table_sql + load_table_sqls
        load_num = load_num + 1
    load_table_sqls = load_table_sql_0 + load_table_sqls
    # 上传hdfs
    get_local_hdfs_thread(TargetDb=TargetDB, TargetTable=TargetTable, ExecDate=ExecDate, DataFileList=LocalFileList,HDFSDir=hdfs_dir)
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
