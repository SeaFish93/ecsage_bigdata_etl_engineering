# -*- coding: utf-8 -*-

from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_alert_info_d
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.common.base.airflow_instance import Airflow
from ecsage_bigdata_etl_engineering.common.base.sync_method import get_create_mysql_table_columns
from ecsage_bigdata_etl_engineering.common.base.get_config import Conf

import os
import time
import json
import ast
import socket
conf = Conf().conf
etl_data_dir = conf.get("Etl", "hive_mysql_data_home")

#入口方法
def main(TaskInfo,**kwargs):
    global airflow
    global developer
    global regexp_extract_column
    airflow = Airflow(kwargs)
    print(TaskInfo, "####################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    hive_session = set_db_session(SessionType="hive", SessionHandler="hive")
    beeline_session = set_db_session(SessionType="beeline", SessionHandler="beeline")
    exec_date = airflow.execution_date_utc8_str[0:10]
    target_handle = TaskInfo[7]
    target_db = TaskInfo[8]
    target_table = TaskInfo[10]
    export_hive_datafile(BeelineSession=beeline_session, TargetDB=target_db, TargetTable=target_table,
                       AirflowDag=airflow.dag, AirflowTask=airflow.task,
                       TaskInfo=TaskInfo, ExecDate=exec_date
                       )


def export_hive_datafile(BeelineSession="",TargetDB="",TargetTable="",AirflowDag="",AirflowTask="",TaskInfo="",ExecDate=""):
  task_flag = "%s.%s"%(AirflowDag,AirflowTask)
  local_time = time.strftime("%Y-%m-%d_%H_%M_%S", time.localtime())
  hostname = socket.gethostname()
  local_dir = """%s/%s/%s/%s/%s"""%(etl_data_dir,hostname,ExecDate,AirflowDag,AirflowTask)
  data_task_file = """%s/data_%s.log"""%(local_dir,AirflowTask)
  tmp_data_task_file = """%s/tmp_data_file.log""" % (local_dir)
  data_file = data_task_file.split("/")[-1].split(".")[0] + "_1_%s." % (local_time) + data_task_file.split("/")[-1].split(".")[1]
  source_db = TaskInfo[5]
  source_table = TaskInfo[6]
  target_handle = TaskInfo[7]
  target_db = TaskInfo[8]
  target_table = TaskInfo[10]
  export_mode  = TaskInfo[11]
  increment_mode = TaskInfo[12]
  increment_columns = TaskInfo[13]
  filter_condition = TaskInfo[14]
  column_identical = TaskInfo[15]
  filter_sql = ""
  os.system("""mkdir -p %s"""%(local_dir))
  os.system("""chmod -R 777 %s""" % (local_dir))
  os.system("""rm -f %s/*"""%(local_dir))
  mysql_session = set_db_session(SessionType="mysql", SessionHandler=target_handle)
  if int(column_identical) == 1:
      export_columns = get_create_mysql_table_columns(MysqlSession=mysql_session, DB=target_db, Table=target_table)
  elif int(column_identical) == 0:
      export_columns = TaskInfo[16]

  if int(increment_mode) == 0:
      increment_date = airflow.execution_date_utc8_str[0:4]
  elif int(increment_mode) == 1:
      increment_date = airflow.execution_date_utc8_str[0:7]
  elif int(increment_mode) == 2:
      increment_date = airflow.execution_date_utc8_str[0:10]
  if int(export_mode) == 0:
      delete_sql = """
            delete from %s.%s where %s = %s""" %(target_db, target_table, increment_date, increment_date)
      filter_sql = """
            select  %s
            from %s.%s 
            where 1 = 1 and %s = %s
            %s
           -- limit 1 
            """ % (export_columns, source_db, source_table,increment_columns, increment_date, filter_condition)
      print("delete_sqlsql：%s" % (delete_sql))
      print("过滤sql：%s" % (filter_sql))
  elif int(export_mode) == 1:
      delete_sql = """
                   truncate %s.%s""" %(target_db, target_table)
      filter_sql = """
                  select  %s
                  from %s.%s 
                  where 1 = 1 
                  %s
                 -- limit 1   
                  """ % (export_columns, source_db, source_table, filter_condition)
      print("delete_sqlsql：%s" % (delete_sql))
      print("过滤sql：%s" % (filter_sql))
  ok = BeelineSession.execute_sql_result_2_local_file(sql=filter_sql,file_name=tmp_data_task_file)
  if ok is False:
     msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                            SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                             TargetTable="%s.%s" % (TargetDB, TargetTable),
                            BeginExecDate=ExecDate,
                            EndExecDate=ExecDate,
                            Status="Error",
                            Log="hive导出文件出现异常！！！",
                            Developer="developer")
     set_exit(LevelStatu="red", MSG=msg)

  load_data_mysql(AsyncAccountFile=local_dir, DataFile=tmp_data_task_file, DbName=target_db, TableName=target_table,
                  Columns=export_columns, DeleteSql=delete_sql,ExecDate=ExecDate, MysqlSession=mysql_session)

def load_data_mysql(AsyncAccountFile="",DataFile="",DbName="",TableName="",Columns="",DeleteSql="",ExecDate="",MysqlSession=""):
    target_file = os.listdir(AsyncAccountFile)
    ok = MysqlSession.local_file_to_mysql(sql=DeleteSql)
    if ok is False:
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (DbName, TableName),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="删除mysql数据出现异常！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)
    for files in target_file:
        n = 0
        set_run = True
        if DataFile.split("/")[-1] in files:
            print(files, "###############################################")
            insert_sql = """
                  load data local infile '%s' into table %s.%s fields terminated by '\\t' lines terminated by '\\n' (%s)
               """ % (AsyncAccountFile + "/" + files,DbName,TableName,Columns)
            while set_run:
              ok = MysqlSession.local_file_to_mysql(sql=insert_sql)
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




