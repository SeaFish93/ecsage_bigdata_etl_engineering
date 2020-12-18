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
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_creative_detail_data as get_creative_detail_data_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_advertisers_data as get_advertisers_data_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_service_page_data as get_service_page_data_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_not_page as get_not_page_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_service_data as get_service_data_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_local_hdfs_thread
from ecsage_bigdata_etl_engineering.common.base.def_table_struct import def_ods_structure as get_ods_columns
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.get_data_2_snap import exec_snap_hive_table
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
    source_db = TaskInfo[11]
    source_table = TaskInfo[12]
    hive_session = set_db_session(SessionType="hive", SessionHandler="hive")
    beeline_session = set_db_session(SessionType="beeline", SessionHandler="beeline")
    if Level == "file":
       if TaskInfo[0] == "metadb_oe_service_account":
          get_service_info(AirflowDag=airflow.dag,AirflowTask=airflow.task,TaskInfo=TaskInfo,ExecDate=exec_date)
       else:
          get_data_2_etl_mid(BeelineSession=beeline_session, TargetDB=target_db, TargetTable=target_table,
                             AirflowDag=airflow.dag, AirflowTask=airflow.task,
                             TaskInfo=TaskInfo, ExecDate=exec_date
                            )
          #get_advertisers_info(AirflowDag=airflow.dag, AirflowTask=airflow.task, BeelineSession=beeline_session,
          #                      TargetDB=target_db, TargetTable=target_table, TaskInfo=TaskInfo, ExecDate=exec_date)
    ###elif Level == "file" and TaskInfo[0] == "etl_mid_oe_getadvertiser_advertiser":
    ###    get_advertisers_info(AirflowDag=airflow.dag, AirflowTask=airflow.task, BeelineSession=beeline_session,
    ###                         TargetDB=target_db, TargetTable=target_table, TaskInfo=TaskInfo,ExecDate=exec_date)
    ###elif Level == "file" and TaskInfo[0] == "etl_mid_oe_getcreativedetail_creativedetail_test":
    ###    get_creative_detail_data(BeelineSession=beeline_session, AirflowDag=airflow.dag, AirflowTask=airflow.task, TaskInfo=TaskInfo, ExecDate=exec_date)
    elif Level == "ods":
        get_data_2_ods(HiveSession=hive_session,BeelineSession=beeline_session,SourceDB=source_db,
                       SourceTable=source_table,TargetDB=target_db,TargetTable=target_table,
                       ExecDate=exec_date,ArrayFlag="",KeyColumns="id")
    elif Level == "snap":
        get_ods_2_snap(AirflowDagId=airflow.dag,AirflowTaskId=airflow.task,
                       SourceDB=source_db,SourceTable=source_table,TargetDB=target_db,
                       TargetTable=target_table,TaskInfo=TaskInfo,ExecDate=exec_date)

def get_data_2_etl_mid(BeelineSession="",TargetDB="",TargetTable="",AirflowDag="",AirflowTask="",TaskInfo="",ExecDate=""):
  task_flag = "%s.%s"%(AirflowDag,AirflowTask)
  local_time = time.strftime("%Y-%m-%d_%H_%M_%S", time.localtime())
  local_dir = """/home/ecsage_data/oceanengine/sync/%s/%s/%s"""%(ExecDate,AirflowDag,AirflowTask)
  celery_get_page_status = """%s/celery_get_page_status.log"""%(local_dir)
  celery_get_data_status = "%s/celery_get_data_status.log"%(local_dir)
  page_task_file = "%s/page_task_file.log"%(local_dir)
  data_task_file = """%s/data_task_file.log"""%(local_dir)
  tmp_data_task_file = """%s/tmp_data_file.log""" % (local_dir)
  task_exception_file = "%s/task_exception_file.log"%(local_dir)
  data_file = data_task_file.split("/")[-1].split(".")[0] + "_1_%s." % (local_time) + data_task_file.split("/")[-1].split(".")[1]
  param_json = ast.literal_eval(json.loads(json.dumps(TaskInfo[5])))
  #设置查询日期
  if TaskInfo[6] is not None and len(TaskInfo[6]) > 0 and TaskInfo[6] != "":
     param_json["start_date"] = ExecDate
     param_json["end_date"] = ExecDate
  url_path = TaskInfo[4]
  filter_db_name = TaskInfo[21]
  filter_table_name = TaskInfo[22]
  filter_column_name = TaskInfo[23]
  filter_config = TaskInfo[24]
  is_report = TaskInfo[18]
  is_page = TaskInfo[25]
  media_type = TaskInfo[26]
  is_advertiser_list = TaskInfo[27]
  os.system("""mkdir -p %s"""%(local_dir))
  os.system("""rm -f %s/*"""%(local_dir))
  is_filter = False
  #判断是否从列表过滤
  if filter_db_name is not None and len(filter_db_name) > 0:
      filter_sql = """
      select concat_ws(' ',%s,'%s') from %s.%s where etl_date='%s' %s group by %s
      """%(filter_column_name,task_flag,filter_db_name,filter_table_name,ExecDate,filter_config,filter_column_name)
      os.system("""spark-sql -S -e"%s"> %s"""%(filter_sql,tmp_data_task_file))
      etl_md.execute_sql("delete from metadb.oe_sync_filter_info where flag = '%s' "%(task_flag))
      columns = """advertiser_id,filter_id,flag"""
      load_data_mysql(AsyncAccountFile=local_dir, DataFile=tmp_data_task_file, DbName="metadb", TableName="oe_sync_filter_info",Columns=columns)
      sql = """
            select a.account_id, a.media_type, a.service_code,b.filter_id as id,b.flag
            from metadb.oe_account_interface a
            inner join metadb.oe_sync_filter_info b
            on a.account_id = b.advertiser_id
            where a.exec_date = '%s'
              and b.flag = '%s'
            group by a.account_id, a.media_type, a.service_code,b.filter_id,b.flag
       """%(ExecDate,task_flag)
      is_filter = True
  else:
      #处理维度表分支
      if int(is_report) == 0:
       sql = """
            select a.account_id, a.media_type, a.service_code,'' as id,'%s'
            from metadb.oe_service_account a
            where a.media_type = '%s'
            group by a.account_id, a.media_type, a.service_code
            limit 10
       """%(task_flag,media_type)
  ok,db_data = etl_md.get_all_rows(sql)
  #处理翻页
  if int(is_page) == 1:
     pass
  else:
    for data in db_data:
      if int(is_advertiser_list) == 1:
        param_json["advertiser_ids"] = [int(data[0])]
      else:
        param_json["advertiser_id"] = int(data[0])
      celery_task_id = get_not_page_celery.delay(UrlPath=url_path,ParamJson=param_json,
                                                 ServiceCode=data[2],ReturnAccountId=data[0],
                                                 ReturnColumns="",TaskFlag=task_flag,
                                                 DataFileDir=local_dir,
                                                 DataFile=data_file,TaskExceptionFile=task_exception_file
                                                )
      os.system("""echo "%s %s %s">>%s""" % (celery_task_id, data[0], data[2], celery_get_data_status))
  # 获取状态
  print("正在等待celery队列执行完成！！！")
  celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_get_data_status)
  wait_for_celery_status(StatusList=celery_task_id)
  print("celery队列执行完成！！！%s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
  #获取数据文件
  target_file = os.listdir(local_dir)
  data_task_file_list = []
  for files in target_file:
      if str(data_task_file.split("/")[-1]).split(".")[0] in files and '.lock' not in files:
          data_task_file_list.append("%s/%s"%(local_dir, files))
  #数据落地至etl_mid
  load_data_2_etl_mid(BeelineSession=BeelineSession, LocalFileList=data_task_file_list, TargetDB=TargetDB,
                      TargetTable=TargetTable, ExecDate=ExecDate,MediaType=media_type
                      )

#落地数据至snap
def get_ods_2_snap(AirflowDagId="",AirflowTaskId="",SourceDB="",SourceTable="",TargetDB="",TargetTable="",TaskInfo="",ExecDate=""):
    source_db = SourceDB
    source_table = SourceTable
    hive_handler = "hive"
    beeline_handler = "beeline"
    target_db = TargetDB
    target_table = TargetTable

    hive_session = set_db_session(SessionType="hive", SessionHandler=hive_handler)
    beeline_session = set_db_session(SessionType="beeline", SessionHandler=beeline_handler)
    exec_snap_hive_table(AirflowDagId=AirflowDagId, AirflowTaskId=AirflowTaskId, HiveSession=hive_session, BeelineSession=beeline_session,
                         SourceDB=source_db,SourceTable=source_table,TargetDB=target_db, TargetTable=target_table, IsReport=0,
                         KeyColumns="id", ExecDate=ExecDate)

def get_data_2_ods(HiveSession="",BeelineSession="",SourceDB="",SourceTable="",TargetDB="",TargetTable="",ExecDate="",ArrayFlag="",KeyColumns="",SelectExcludeColumns=""):
    etl_ods_field_diff = get_ods_columns(HiveSession=HiveSession, BeelineSession=BeelineSession
                                         , SourceTable=SourceTable, TargetDB=TargetDB, TargetTable=TargetTable
                                         , IsTargetPartition="Y", ExecDate=ExecDate, ArrayFlag=None,IsReplace="N")
    print("返回的表差异 %s || %s || %s" % (etl_ods_field_diff[0], etl_ods_field_diff[1], etl_ods_field_diff[2]))
    source_target_columns_diff = etl_ods_field_diff[0]
    target_columns = etl_ods_field_diff[1]
    source_columns = etl_ods_field_diff[2]
    ok, get_ods_column = HiveSession.get_column_info(TargetDB, TargetTable)
    system_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    system_table_columns = "returns_account_id,returns_colums,request_type,extract_system_time,etl_date"
    select_system_table_column = "returns_account_id,returns_colums,request_type,'%s' as extract_system_time"%(system_time)
    is_key_columns(SourceDB=SourceDB, SourceTable=SourceTable, TargetDB=TargetDB, TargetTable=TargetTable,
                   ExecDate=ExecDate, KeyColumns=KeyColumns)
    row_number_columns = ""
    key_column_list = KeyColumns.split(",")
    for key in key_column_list:
        row_number_columns = row_number_columns + "," + "`" + key + "`"
    row_number_columns = row_number_columns.replace(",", "", 1)
    select_exclude_columns = SelectExcludeColumns
    if select_exclude_columns is None or len(select_exclude_columns) == 0:
        select_exclude_columns = "000000"
    columns = ""
    for column in get_ods_column:
        columns = columns + "," + column[0]
        if column[0] == "etl_date":
            break;
    columns = columns.replace(",", "", 1)
    json_tuple_columns = ""
    for get_json_tuple_column in columns.split(","):
        if get_json_tuple_column not in select_exclude_columns.split(",") and get_json_tuple_column not in system_table_columns.split(","):
            json_tuple_columns = json_tuple_columns + "," + "'%s'" % (get_json_tuple_column)
    json_tuple_columns = json_tuple_columns.replace(",", "", 1)
    json_tuple_column = json_tuple_columns.replace("'", "")
    select_json_tuple_column = json_tuple_columns.replace("'", "`")
    columns = ','.join("`%s`" % (x) for x in columns.split(",") if x != 'etl_date')
    array_flag = ArrayFlag
    if array_flag in ["list", "custom_audience_list"]:
        regexp_extract = """get_json_object(a.request_data,'$.data.%s') as data_colums""" % (array_flag)
    else:
        regexp_extract = """get_json_object(a.request_data,'$.data') as data_colums"""
    return_regexp_extract = """'returns_colums' as returns_colums"""
    returns_account_id = """trim(get_json_object(a.request_data,'$.returns_account_id')) as returns_account_id"""
    filter_line = """length(regexp_extract(a.request_data,'(\\\\"\\\\}## \\\\{\\\\".*)',1)) > 0"""
    specified_pars_str = etl_ods_field_diff[3]
    specified_pars_list = etl_ods_field_diff[2]
    null_field_set = list(set(json_tuple_column.split(",")).difference(set(specified_pars_list)))
    null_field_list = []
    for null_field in null_field_set:
        null_field_list.append(",cast( null as String) as `%s`" % (null_field))
    null_field_str = ''.join(null_field_list)
    null_field_str = null_field_str + ",'%s' as `extract_system_time`" % (system_time)

    print("Json待解析字段：" + specified_pars_str)
    if specified_pars_str is not None and len(specified_pars_str) > 0:
        pars_str_list = []
        for pars_field in specified_pars_str.split(","):
            as_str = pars_field.split(".")[-1]
            pars_str_list.append("get_json_object(data_num_colums,'$.%s') as `%s`" % (pars_field, as_str))
        pars_str = ','.join(pars_str_list)
        sql = """
            add file hdfs:///tmp/airflow/get_arrary.py;
            drop table if exists %s.%s_tmp;
            create table %s.%s_tmp stored as parquet as 
            select %s
            from (select returns_colums,%s %s,returns_account_id,request_type
                  from(select split(split(data_colums,'@@####@@')[0],'##&&##')[0] as returns_colums
                              ,split(data_colums,'@@####@@')[1] as data_colums
                              ,split(split(data_colums,'@@####@@')[0],'##&&##')[1] as returns_account_id
                              ,split(split(data_colums,'@@####@@')[0],'##&&##')[2] as request_type
                       from(select transform(concat_ws('##@@',concat_ws('##&&##',returns_colums,returns_account_id,request_type),data_colums)) USING 'python get_arrary.py' as (data_colums)
                            from(select %s
                                        ,%s
                                        ,%s
                                        ,'request_type' as request_type
                                 from %s.%s a
                                 where a.etl_date = '%s'
                                ) a
                            where data_colums is not null
                            ) b
                       ) c
                       lateral view explode(split(data_colums, '##@@')) num_line as data_num_colums
                  ) a
                  ;
            """ % ("etl_mid", TargetTable, "etl_mid", TargetTable, columns, pars_str, null_field_str, return_regexp_extract,
                   regexp_extract, returns_account_id, SourceDB, SourceTable, ExecDate)
    ok = BeelineSession.execute_sql(sql)
    if ok is False:
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (TargetDB, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="ods入库-tmp失败！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)
    sql = """
            insert overwrite table %s.%s
            partition(etl_date = '%s')
            select %s from(
            select %s,row_number()over(partition by %s order by 1) as rn_row_number
            from %s.%s_tmp
            ) tmp where rn_row_number = 1
                   ;
            drop table if exists %s.%s_tmp;
            """ % (TargetDB, TargetTable, ExecDate, columns, columns, row_number_columns, "etl_mid", TargetTable, "etl_mid",TargetTable)
    ok = BeelineSession.execute_sql(sql)
    if ok is False:
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (TargetDB, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="ods入库失败！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)


def get_service_page(DataRows="",LocalDir="",DataFile="",PageFileData="",TaskFlag="",CeleryGetDataStatus="",Page="",PageSize=""):
    for data in DataRows:
        celery_task_id = get_service_page_data_celery.delay(ServiceId=data[0], ServiceCode=data[1],
                                                       Media=data[2], Page=str(Page), PageSize=str(PageSize),
                                                       DataFile=DataFile, PageFileData=PageFileData,
                                                       TaskFlag=TaskFlag
                                                       )
        os.system("""echo "%s %s %s %s ">>%s""" % (celery_task_id, data[0], data[1], data[2], CeleryGetDataStatus))
    # 获取状态
    celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=CeleryGetDataStatus)
    print("正在等待获取页数celery队列执行完成！！！")
    wait_for_celery_status(StatusList=celery_task_id)
    print("获取页数celery队列执行完成！！！")
    print("end %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    # 保存MySQL
    columns = """page_num,account_id,service_code,remark,data,request_filter,flag,media_type"""
    load_data_mysql(AsyncAccountFile=LocalDir, DataFile=PageFileData, DbName="metadb",
                    TableName="oe_sync_page_interface", Columns=columns)

def get_service_info(AirflowDag="",AirflowTask="",TaskInfo="",ExecDate=""):
  task_flag = "%s.%s"%(AirflowDag,AirflowTask)
  local_time = time.strftime("%Y-%m-%d_%H_%M_%S", time.localtime())
  local_dir = """/home/ecsage_data/oceanengine/sync/%s/%s/%s"""%(ExecDate,AirflowDag,AirflowTask)
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
  if datas is not None and len(datas) > 0:
     for dt in datas:
        page_number = int(dt[3])
        for page in range(page_number):
         if page > 0:
           pages = page + 1
           celery_task_id = get_service_data_celery.delay(ServiceId=dt[0], ServiceCode=dt[2],
                                                          Media=dt[1], Page=str(pages), PageSize=str(1000),
                                                          DataFile=data_file, PageFileData=page_task_file,
                                                          TaskFlag=task_flag,TaskExceptionFile=task_exception_file
                                                        )
           os.system("""echo "%s %s %s %s ">>%s""" % (celery_task_id, dt[0], dt[1], dt[2], celery_get_data_status))
     # 获取状态
     print("正在等待celery队列执行完成！！！")
     celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_get_data_status)
     wait_for_celery_status(StatusList=celery_task_id)
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

#广告创意
def get_creative_detail_data(BeelineSession="",AirflowDag="",AirflowTask="",TaskInfo="",ExecDate=""):
  start_date_name = TaskInfo[7]
  end_date_name = TaskInfo[8]
  filter_db_name = TaskInfo[21]
  filter_table_name = TaskInfo[22]
  filter_column_name = TaskInfo[23]
  filter_config = TaskInfo[24]
  target_db = TaskInfo[14]
  target_table = TaskInfo[15]
  interface_flag = "%s.%s"%(AirflowDag,AirflowTask)
  local_time = time.strftime("%Y-%m-%d_%H_%M_%S", time.localtime())
  local_dir = """/home/ecsage_data/oceanengine/sync/%s/%s/%s"""%(ExecDate,AirflowDag,AirflowTask)
  celery_get_page_status = """%s/celery_get_page_status.log"""%(local_dir)
  celery_get_data_status = "%s/celery_get_data_status.log"%(local_dir)
  page_task_file = "%s/page_task_file.log"%(local_dir)
  data_task_file = """%s/data_task_file.log"""%(local_dir)
  tmp_data_task_file = """%s/tmp_data_file.log""" % (local_dir)
  task_exception_file = "%s/task_exception_file.log"%(local_dir)
  data_file = data_task_file.split("/")[-1].split(".")[0] + "_1_%s." % (local_time) + data_task_file.split("/")[-1].split(".")[1]
  param_json = ast.literal_eval(json.loads(json.dumps(TaskInfo[5])))
  #设置查询日期
  if start_date_name is not None and len(start_date_name) > 0 and start_date_name != "":
     param_json["%s"%start_date_name] = ExecDate
     param_json["%s"%end_date_name] = ExecDate
  url_path = TaskInfo[4]
  os.system("""mkdir -p %s"""%(local_dir))
  os.system("""rm -f %s/*"""%(local_dir))
  is_filter = False
  #判断是否从列表过滤
  if filter_db_name is not None and len(filter_db_name) > 0:
      filter_sql = """
      select concat_ws(' ',%s,'%s') from %s.%s where etl_date='%s' %s group by %s
      """%(filter_column_name,interface_flag,filter_db_name,filter_table_name,ExecDate,filter_config,filter_column_name)
      print("获取筛选sql："+filter_sql)
      os.system("""spark-sql -S -e"%s"> %s"""%(filter_sql,tmp_data_task_file))
      etl_md.execute_sql("delete from metadb.oe_sync_filter_info where flag = '%s' "%(interface_flag))
      columns = """advertiser_id,filter_id,flag"""
      load_data_mysql(AsyncAccountFile=local_dir, DataFile=tmp_data_task_file, DbName="metadb", TableName="oe_sync_filter_info",Columns=columns)
      sql = """
            select a.account_id, a.media_type, a.service_code,b.filter_id as id,b.flag
            from metadb.oe_account_interface a
            inner join metadb.oe_sync_filter_info b
            on a.account_id = b.advertiser_id
            where a.exec_date = '%s'
              and b.flag = '%s'
            --  and a.account_id = '1679044314152973'
            --  and b.filter_id = '1685568526811261'
            group by a.account_id, a.media_type, a.service_code,b.filter_id,b.flag
       """%(ExecDate,interface_flag)
      is_filter = True
  else:
      sql = """
            select a.account_id, a.media_type, a.service_code,'' as id,'%s'
            from metadb.oe_account_interface a
            where a.exec_date = '%s'
            group by a.account_id, a.media_type, a.service_code
       """%(interface_flag,ExecDate)
  ok,db_data = etl_md.get_all_rows(sql)
  if db_data is not None and len(db_data) > 0:
    for data in db_data:
      account_id = int(data[0])
      ad_id = int(data[3])
      service_code = str(data[2])
      param_json["advertiser_id"] = account_id
      param_json["ad_id"] = ad_id
      param_json["service_code"] = service_code
      celery_task_id = get_creative_detail_data_celery.delay(ParamJson=str(param_json), UrlPath=url_path,
                                                             TaskExceptionFile=task_exception_file,DataFileDir=local_dir,
                                                             DataFile=data_file,InterfaceFlag=interface_flag
                                                             )
      os.system("""echo "%s %s %s %s ">>%s""" % (celery_task_id, account_id,ad_id,service_code, celery_get_data_status))
    # 获取状态
    print("正在等待celery队列执行完成！！！")
    celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_get_data_status)
    wait_for_celery_status(StatusList=celery_task_id)
    print("celery队列执行完成！！！%s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    # 重试异常
    print("正在等待获取广告创意重试异常执行完成！！！")
    rerun_exception_tasks(UrlPath=url_path,AsyncAccountDir=local_dir, ExceptionFile=task_exception_file,
                          DataFile=data_file, CeleryTaskDataFile=celery_get_data_status,
                          InterfaceFlag=interface_flag, ExecDate=ExecDate,Columns="""account_id,service_code,interface_flag,filter_id""",
                          IsfilterID="Y",ParamJson=param_json)
    print("获取广告创意重试异常执行完成！！！")
    #获取数据文件
    target_file = os.listdir(local_dir)
    data_task_file_list = []
    for files in target_file:
        if str(data_task_file.split("/")[-1]).split(".")[0] in files and '.lock' not in files:
            data_task_file_list.append("%s/%s"%(local_dir, files))
    #数据落地至etl_mid
    load_data_2_etl_mid(BeelineSession=BeelineSession, LocalFileList=data_task_file_list, TargetDB=target_db,
                        TargetTable=target_table, ExecDate=ExecDate)


#广告主
def get_advertisers_info(AirflowDag="", AirflowTask="",BeelineSession="",TargetDB="",TargetTable="",TaskInfo="", ExecDate=""):
    interface_flag = """%s.%s""" % (AirflowDag, AirflowTask)
    local_time = time.strftime("%Y-%m-%d_%H_%M_%S", time.localtime())
    local_dir = """/home/ecsage_data/oceanengine/sync/%s/%s/%s""" % (ExecDate, AirflowDag, AirflowTask)
    celery_get_data_status = "%s/celery_get_data_status.log" % (local_dir)
    data_task_file = """%s/data_task_file.log""" % (local_dir)
    task_exception_file = "%s/task_exception_file.log" % (local_dir)
    data_file = data_task_file.split("/")[-1].split(".")[0] + "_1_%s." % (local_time) + data_task_file.split("/")[-1].split(".")[1]
    os.system("""mkdir -p %s""" % (local_dir))
    os.system("""rm -f %s/*""" % (local_dir))
    ok,datas = etl_md.get_all_rows("""select account_id,service_code from metadb.oe_service_account group by account_id,service_code""")
    for data in datas:
       celery_task_id = get_advertisers_data_celery.delay(AccountIdList=[int(data[0])],ServiceCode=data[1],
                                                          DataFileDir=local_dir,DataFile=data_file,
                                                          TaskExceptionFile=task_exception_file,
                                                          InterfaceFlag=interface_flag
                                                          )
       os.system("""echo "%s %s %s">>%s""" % (celery_task_id, data[0], data[1], celery_get_data_status))
    # 获取状态
    celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_get_data_status)
    print("正在等待获取广告主celery队列执行完成！！！")
    wait_for_celery_status(StatusList=celery_task_id)
    print("获取广告主celery队列执行完成！！！")
    print("end %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    #重试异常
    print("正在等待获取广告主重试异常执行完成！！！")
    rerun_exception_tasks(AsyncAccountDir=local_dir, ExceptionFile=task_exception_file,
                          DataFile=data_file, CeleryTaskDataFile=celery_get_data_status,
                          InterfaceFlag=interface_flag, ExecDate=ExecDate, Columns="""account_id,service_code,interface_flag""",
                          IsfilterID="N"
                          )
    print("获取广告主重试异常执行完成！！！")
    #上传本地文件至etl_mid
    set_data_2_etl_mid(BeelineSession=BeelineSession, TargetDB=TargetDB, TargetTable=TargetTable, ExecDate=ExecDate, LocalDir=local_dir, DataFile=data_file)

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
    load_data_mysql(AsyncAccountFile=SyncDir, DataFile=PageTaskFile, DbName="metadb", TableName="oe_sync_page_interface",Columns=columns)

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
  if TaskInfo[6] is not None and len(TaskInfo[6]) > 0 and TaskInfo[6] != "":
     param_json["start_date"] = ExecDate
     param_json["end_date"] = ExecDate
  url_path = TaskInfo[4]
  filter_db_name = TaskInfo[21]
  filter_table_name = TaskInfo[22]
  filter_column_name = TaskInfo[23]
  filter_config = TaskInfo[24]
  os.system("""mkdir -p %s"""%(local_dir))
  os.system("""rm -f %s/*"""%(local_dir))
  is_filter = False
  #判断是否从列表过滤
  if filter_db_name is not None and len(filter_db_name) > 0:
      filter_sql = """
      select concat_ws(' ',%s,'%s.%s') from %s.%s where etl_date='%s' %s group by %s
      """%(filter_column_name,AirflowDag,AirflowTask,filter_db_name,filter_table_name,ExecDate,filter_config,filter_column_name)
      os.system("""spark-sql -S -e"%s"> %s"""%(filter_sql,tmp_data_task_file))
      etl_md.execute_sql("delete from metadb.oe_sync_filter_info where flag = '%s.%s' "%(AirflowDag,AirflowTask))
      columns = """advertiser_id,filter_id,flag"""
      load_data_mysql(AsyncAccountFile=local_dir, DataFile=tmp_data_task_file, DbName="metadb", TableName="oe_sync_filter_info",Columns=columns)
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
         if str(data_task_file.split("/")[-1]).split(".")[0] in files and '.lock' not in files:
             data_task_file_list.append("%s/%s"%(local_dir, files))
     #数据落地至etl_mid
     load_data_2_etl_mid(BeelineSession=BeelineSession, LocalFileList=data_task_file_list, TargetDB=TargetDB,
                         TargetTable=TargetTable, ExecDate=ExecDate)

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
        set_exit(LevelStatu="red", MSG=msg)
    mid_sql = """
        create table if not exists %s.%s
        (
         request_data string
        )partitioned by(etl_date string,request_type string)
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
        if DataFile.split("/")[-1] in files:
            print(files, "###############################################")
            # 记录子账户
            insert_sql = """
                  load data local infile '%s' into table %s.%s fields terminated by ' ' lines terminated by '\\n' (%s)
               """ % (AsyncAccountFile + "/" + files,DbName,TableName,Columns)
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

#重试代理商
def rerun_service_exception_tasks(AsyncAccountDir="",ExceptionFile="",DataFile="",CeleryTaskDataFile="",InterfaceFlag="",ExecDate="",IsfilterID="",Columns=""):
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
           print("开始第%s次重试异常，时间：%s"%(i+1,time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
           for data in datas:
               status_id = get_service_data_celery.delay(ServiceId=data[0], ServiceCode=data[1],
                                                         Media=data[3], Page=str(data[4]), PageSize=str(data[5]),
                                                         DataFile=DataFile, PageFileData="",
                                                         TaskFlag=InterfaceFlag, TaskExceptionFile=ExceptionFile
                                                        )
               os.system("""echo "%s %s">>%s""" % (status_id, data[0], celery_task_data_file+".%s"%(i)))
           celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_data_file + ".%s"%i)
           wait_for_celery_status(StatusList=celery_task_id)
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

def rerun_exception_tasks(UrlPath="",AsyncAccountDir="",ExceptionFile="",DataFile="",CeleryTaskDataFile="",InterfaceFlag="",ExecDate="",IsfilterID="",Columns="",ParamJson=""):
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
           print("开始第%s次重试异常，时间：%s"%(i+1,time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
           for data in datas:
               if IsfilterID == "Y":
                   account_id = int(data[0])
                   ad_id = int(data[3])
                   service_code = str(data[1])
                   ParamJson["advertiser_id"] = account_id
                   ParamJson["ad_id"] = ad_id
                   ParamJson["service_code"] = service_code
                   status_id = get_creative_detail_data_celery.delay(ParamJson=str(ParamJson), UrlPath=UrlPath,
                                                                     TaskExceptionFile=ExceptionFile,
                                                                     DataFileDir=AsyncAccountDir,
                                                                     DataFile=DataFile,
                                                                     InterfaceFlag=InterfaceFlag
                                                                    )
                   os.system("""echo "%s %s %s %s ">>%s""" % (status_id, account_id, ad_id, service_code, celery_task_data_file+".%s"%(i)))
               else:
                  status_id = get_advertisers_data_celery.delay(AccountIdList=[int(data[0])], ServiceCode=data[1],
                                                             DataFileDir=AsyncAccountDir,DataFile=DataFile,
                                                             TaskExceptionFile=ExceptionFile,InterfaceFlag=InterfaceFlag
                                                             )
                  os.system("""echo "%s %s">>%s""" % (status_id, data[0], celery_task_data_file+".%s"%(i)))
           celery_task_id, status_wait = get_celery_status_list(CeleryTaskStatusFile=celery_task_data_file + ".%s"%i)
           wait_for_celery_status(StatusList=celery_task_id)
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
           os.system("""rm -f %s/%s"""%(file[0],file[1]))

def set_data_2_etl_mid(BeelineSession="",TargetDB="",TargetTable="",ExecDate="",LocalDir="",DataFile=""):
    target_file = os.listdir(LocalDir)
    data_task_file_list = []
    for files in target_file:
        if str(DataFile.split("/")[-1]).split(".")[0] in files and '.lock' not in files:
            data_task_file_list.append("%s/%s" % (LocalDir, files))
    # 数据落地至etl_mid
    load_data_2_etl_mid(BeelineSession=BeelineSession, LocalFileList=data_task_file_list, TargetDB=TargetDB,
                        TargetTable=TargetTable, ExecDate=ExecDate)

def is_key_columns(SourceDB="",SourceTable="",TargetDB="",TargetTable="",ExecDate="",KeyColumns=""):
    if KeyColumns is None or len(KeyColumns) == 0:
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % (SourceDB, SourceTable),
                               TargetTable="%s.%s" % (TargetDB, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="请确认配置表指定主键字段是否正确！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)