# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: mysql_2_hive.py
# @Software: PyCharm
# function info：用于同步mysql库数据到hive ods\snap\backtrace表


from yk_bigdata_etl_engineering.common.base.get_config import Conf
from yk_bigdata_etl_engineering.common.base.airflow_instance import Airflow
from yk_bigdata_etl_engineering.common.base.sync_method import get_mysql_hive_table_column
from yk_bigdata_etl_engineering.common.base.sync_method import get_create_mysql_table_columns
from yk_bigdata_etl_engineering.common.base.sync_method import get_table_columns_info
from yk_bigdata_etl_engineering.common.base.sync_method import set_sync_rows
from yk_bigdata_etl_engineering.common.base.sync_method import get_mysql_table_index
from yk_bigdata_etl_engineering.common.base.set_process_exit import set_exit
from yk_bigdata_etl_engineering.common.alert.alert_info import get_alert_info_d
from yk_bigdata_etl_engineering.common.session.db_session import set_db_session
from yk_bigdata_etl_engineering.config.column_type import MYSQL_2_HIVE
from yk_bigdata_etl_engineering.common.base.etl_thread import EtlThread
from yk_bigdata_etl_engineering.common.operator.mysql.conn_mysql_metadb import EtlMetadata

import datetime
import math
import os

conf = Conf().conf
etl_md = EtlMetadata()

#入口方法
def main(TaskInfo, Level,**kwargs):
    global airflow
    global developer
    airflow = Airflow(kwargs)
    #mysql_handler
    mysql_handler = TaskInfo[6]
    #hive handler
    hive_handler = TaskInfo[19]
    #source db
    source_db = TaskInfo[2]
    #来源表
    source_table = TaskInfo[3]
    #目标库
    target_db = TaskInfo[4]
    #目标表
    target_table = TaskInfo[5]
    #临时库
    tmp_db = "etl_mid"
    #临时表
    tmp_table = """%s_tmp"""%(target_table)
    #主键
    key_column = TaskInfo[14]
    #增量/全量标识
    granularity = TaskInfo[8]
    #增量字段
    inc_column = TaskInfo[10]
    #开发者
    developer = TaskInfo[18]
    #源端平台
    source_platform = TaskInfo[20]
    # airflow运行北京时间
    exec_date = airflow.execution_date_utc8_str[0:10].replace("-", "")
    #创建连接session
    hive_session = set_db_session(SessionType="hive", SessionHandler=hive_handler)
    beeline_session = set_db_session(SessionType="beeline", SessionHandler=hive_handler)
    if source_platform == "mysql":
      mysql_session = set_db_session(SessionType="mysql", SessionHandler=mysql_handler)
    try:
        # 删除重跑重复记录
        etl_md.execute_sql(sqlName="delete_etl_job_rows_sql",
                           Parameter={"source_db":source_db, "source_table":source_table,
                                      "exec_date":exec_date},
                           IsReturnData="N")
        if Level in ["snap","sensitive"]:
          set_snap_sensitive_table(HiveSession=hive_session, BeelineSession=beeline_session,SourceDB=source_db, SourceTable=source_table, TargetDB=target_db,
                                   TargetTable=target_table,Granularity=granularity,KeyColumn=key_column,ExecDate=exec_date)

        elif Level == "ods":
          #定义数据文件路径及文件命名
          data_home = conf.get("Airflow_New", "data_home") + "/" + airflow.ds_nodash_utc8
          if not os.path.exists(data_home):
              # 如果目录不存在，则创建
              os.makedirs(data_home)
          data_file = """%s/%s""" % (data_home, target_table + ".file")
          # hdfs存储临时数据文件目录
          hdfs_dir = "/tmp/datafolder_new"
          #保存MySQL主键，提供下游表使用
          set_mysql_key_column(MysqlSession=mysql_session, SourceDB=source_db, SourceTable=source_table,TargetDB=target_db,TargetTable=target_table,ExecDate=exec_date)
          #执行数据文件入仓至临时表
          set_load_file_to_hive(HiveSession=hive_session, MysqlSession=mysql_session, SourceDB=source_db, SourceTable=source_table,ExecDate=exec_date,
                                TargetDB=target_db,TargetTable=target_table, TmpDB=tmp_db, TmpTable=tmp_table,KeyColumn=key_column,Granularity=granularity,
                                IncColumn=inc_column,DataFile=data_file,HDFSDir=hdfs_dir)
          #执行临时表数据至ods分区表
          set_ods_partition_table(HiveSession=hive_session, MysqlSession=mysql_session, SourceDB=source_db, SourceTable=source_table,
                                  TargetDB=target_db,TargetTable=target_table, TmpDB=tmp_db, TmpTable=tmp_table,IsTargetPartition="Y",ExecDate=exec_date)
          # 删除临时表
          sql = """drop table if exists %s.%s""" % (tmp_db, tmp_table)
          hive_session.execute_sql(sql)

        elif Level == "history":
          set_backtrace_table(HiveSession=hive_session, SourceDB=source_db, SourceTable=source_table, TargetDB=target_db,
                              TargetTable=target_table,IsTargetPartition="Y",ExecDate=exec_date)

        else:
          msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                                  SourceTable="%s.%s" % (source_db, source_table),
                                  TargetTable="%s.%s" % (target_db, target_table),
                                  BeginExecDate="",
                                  EndExecDate="",
                                  Status="Error",
                                  Log="未找到对应执行模块！！！",
                                  Developer=developer)
          set_exit(LevelStatu="red", MSG=msg)
        # 检测本次抽取数据是否一致
        check_sync_row(SourceDB=source_db, SourceTable=source_table, TargetDB=target_db, TargetTable=target_table,ExecDate=exec_date, Level=Level)
    except Exception as e:
        print("Exception Error：%s"%(e))
        msg = "Exception_Exist"
        set_exit(LevelStatu="red", MSG=msg)

# 处理ods快照分区表
def set_ods_partition_table(HiveSession="",MysqlSession="",SourceDB="",SourceTable="",TargetDB="", TargetTable="",
                            TmpDB="",TmpTable="",IsTargetPartition="",ExecDate=""):

    begin_system_time = (datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    #若是第一次同步或源表有新字段增加，则修复与源表字段一致及返回ods快照分区表查询信息
    get_select_info = get_mysql_hive_table_column(HiveSession=HiveSession,MysqlSession=MysqlSession,ETLMysqlSession=etl_md,SourceDB=SourceDB,SourceTable=SourceTable,
                                                  TargetDB=TargetDB, TargetTable=TargetTable,IsTargetPartition=IsTargetPartition)
    sql = """
     insert overwrite table %s.%s
     partition(sys_date = '%s')
     select %s
     from %s.%s a
    """%(TargetDB,TargetTable,ExecDate,get_select_info[3],TmpDB,TmpTable)
    #写入ods目标表
    ok = HiveSession.execute_sql(sql)
    end_system_time = (datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    get_rows = get_sync_rows(DB=SourceDB, Table=SourceTable, ExecDate=ExecDate, SyncType="sync_inc")
    sql = """desc formatted %s.%s partition(sys_date='%s')"""%(TargetDB,TargetTable,ExecDate)
    desc_formatted = HiveSession.get_all_rows(sql)
    target_rows = 0
    for desc_info in desc_formatted[1]:
      if "numRows             " in desc_info:
         target_rows = int(str(desc_info[2]).strip())
         break;
    if target_rows <= 0 and int(get_rows[0]) > 0:
      sql = """select count(1) from %s.%s where sys_date='%s'"""%(TargetDB,TargetTable,ExecDate)
      target_rows = int(HiveSession.get_all_rows(sql)[1][0][0])
    # 记录数据日志
    set_sync_rows(MysqlSession=etl_md, SourceDB=SourceDB, SourceTable=SourceTable, TargetDB=TargetDB,
                  TargetTable=TargetTable,Module="sync_inc", ExecDate=ExecDate, BeginExecTime=ExecDate, EndExecTime=ExecDate,
                  BeginSystemTime=begin_system_time,EndSystemTime=end_system_time, SourceRows=int(get_rows[0]), TargetRows=target_rows)
    if ok is False:
      #删除临时表
      sql = """drop table if exists %s.%s""" % (TmpDB, TmpTable)
      HiveSession.execute_sql(sql)
      msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                             SourceTable="%s.%s" % (SourceDB, SourceTable),
                             TargetTable="%s.%s" % (TargetDB, TargetTable),
                             BeginExecDate="",
                             EndExecDate="",
                             Status="Error",
                             Log="insert目标表出现异常！！！",
                             Developer=developer)
      set_exit(LevelStatu="red", MSG=msg)

# 处理snap\sensitive层业务逻辑
def set_snap_sensitive_table(HiveSession="",BeelineSession="",SourceDB="",SourceTable="",TargetDB="",TargetTable="",KeyColumn="",Granularity="",ExecDate=""):

    begin_system_time = (datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    #获取查询字段
    get_select_columns = get_table_columns_info(HiveSession=HiveSession, SourceDB=SourceDB,SourceTable=SourceTable,
                                                TargetDB=TargetDB, TargetTable=TargetTable,IsTargetPartition="N")
    source_table_columns = get_select_columns[2]
    assign_table_columns = get_select_columns[1]
    #获取主键及表关联
    get_key_column,get_table_join = get_important_column_info(KeyColumn=KeyColumn,DB=SourceDB,Table=SourceTable)
    ok, count = HiveSession.get_all_rows("""show partitions %s.%s partition(sys_date='%s')""" % (SourceDB, SourceTable, airflow.yesterday_ds_nodash_utc8))
    if get_key_column == "" or Granularity == "F" or (Granularity == 'D' and ok and len(count) == 0):
        is_condition = "N"
        if Granularity == "F" or (Granularity == 'D' and ok and len(count) == 0):
            sql = """
                    insert overwrite table {target_db}.{target_table}
                    select {source_select_column}
                    from {source_db}.{source_table}
                    where sys_date = '{exec_date}'
                    """.format(target_db=TargetDB, target_table=TargetTable, source_db=SourceDB,
                               source_table=SourceTable,source_select_column=source_table_columns,exec_date=ExecDate
                               )
        else:
            sql = """
            insert overwrite table {target_db}.{target_table}
            select {source_select_column}
            from {source_db}.{source_table}
            where sys_date = '{exec_date}'
                union all
            select {target_select_column}
            from {target_db}.{target_table} a
            where a.sys_date != '{exec_date}'
            """.format(target_db=TargetDB,target_table=TargetTable,source_db=SourceDB,source_table=SourceTable,
                       source_select_column=source_table_columns,target_select_column = assign_table_columns,exec_date=ExecDate
                       )
    else:
        is_condition = "Y"
        sql = """
           set hive.auto.convert.join = false;
           insert overwrite table {target_db}.{target_table}
           select {source_select_column}
           from {source_db}.{source_table}
           where sys_date = '{exec_date}'
              union all
           select {target_select_column}
           from {target_db}.{target_table} a
           left join {source_db}.{source_table} b
           {table_join}
           and b.sys_date = '{exec_date}'
           where b.{key_column} is null
             and a.sys_date != '{exec_date}'
        """.format(source_db=SourceDB,source_table=SourceTable,source_select_column=source_table_columns,target_db=TargetDB,
                   target_table=TargetTable,target_select_column=assign_table_columns,table_join=get_table_join,
                   key_column=get_key_column.split(",")[0],exec_date=ExecDate)
    ok = BeelineSession.execute_sql(sql)
    end_system_time = (datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    source_rows = get_table_or_partition_rows(HiveSession=HiveSession, DB=SourceDB, Table=SourceTable, IsPartition="Y",IsCondition="N", ExecDate=ExecDate)
    target_rows = get_table_or_partition_rows(HiveSession=HiveSession, DB=TargetDB, Table=TargetTable, IsPartition="N",IsCondition=is_condition, ExecDate=ExecDate)
    # # 记录增量数据
    set_sync_rows(MysqlSession=etl_md, SourceDB=SourceDB, SourceTable=SourceTable, TargetDB=TargetDB,
                  TargetTable=TargetTable, Module="sync_inc", ExecDate=ExecDate, BeginExecTime=ExecDate,
                  EndExecTime=ExecDate, BeginSystemTime=begin_system_time, EndSystemTime=end_system_time,
                  SourceRows=source_rows, TargetRows=target_rows)
    #全量条数
    full_target_rows = get_table_or_partition_rows(HiveSession=HiveSession, DB=TargetDB, Table=TargetTable, IsPartition="N",IsCondition="N",ExecDate=ExecDate)
    # 记录全量条数日志
    end_system_time = (datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    set_sync_rows(MysqlSession=etl_md, SourceDB=SourceDB, SourceTable=SourceTable, TargetDB=TargetDB,
                  TargetTable=TargetTable, Module="sync_full", ExecDate=ExecDate, BeginExecTime=ExecDate,
                  EndExecTime=ExecDate, BeginSystemTime=begin_system_time, EndSystemTime=end_system_time,
                  SourceRows=full_target_rows, TargetRows=full_target_rows)
    if ok is False:
       msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                             SourceTable="%s.%s" % (SourceDB, SourceTable),
                             TargetTable="%s.%s" % (TargetDB, TargetTable),
                             BeginExecDate=ExecDate,
                             EndExecDate=ExecDate,
                             Status="Error",
                             Log="insert目标表出现异常！！！",
                             Developer=developer)
       set_exit(LevelStatu="red", MSG=msg)

#处理backtrace层的业务逻辑
def set_backtrace_table(HiveSession="",SourceDB="",SourceTable="",TargetDB="",TargetTable="",IsTargetPartition="",ExecDate=""):

    begin_system_time = (datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    # 获取查询字段
    get_select_columns = get_table_columns_info(HiveSession=HiveSession, SourceDB=SourceDB, SourceTable=SourceTable,
                                                TargetDB=TargetDB, TargetTable=TargetTable, IsTargetPartition=IsTargetPartition)
    assign_table_columns = get_select_columns[3]
    sql = """
    insert overwrite table %s.%s
    partition(sys_date='%s')
    select %s
    from %s.%s a
    """%(TargetDB,TargetTable,ExecDate,assign_table_columns,SourceDB,SourceTable)
    ok = HiveSession.execute_sql(sql)
    # 记录数据日志
    end_system_time = (datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    #source data
    source_rows = get_table_or_partition_rows(HiveSession=HiveSession,DB=SourceDB,Table=SourceTable,IsPartition="N",IsCondition="N",ExecDate=ExecDate)
    #target data
    target_rows = get_table_or_partition_rows(HiveSession=HiveSession, DB=TargetDB, Table=TargetTable, IsPartition="Y",IsCondition="N",ExecDate=ExecDate)
    # 记录数据日志
    set_sync_rows(MysqlSession=etl_md, SourceDB=SourceDB, SourceTable=SourceTable, TargetDB=TargetDB,
                  TargetTable=TargetTable, Module="sync_inc", ExecDate=ExecDate, BeginExecTime=ExecDate,
                  EndExecTime=ExecDate, BeginSystemTime=begin_system_time, EndSystemTime=end_system_time,
                  SourceRows=source_rows, TargetRows=target_rows)
    if ok is False:
       msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                             SourceTable="%s.%s" % (SourceDB, SourceTable),
                             TargetTable="%s.%s" % (TargetDB, TargetTable),
                             BeginExecDate=ExecDate,
                             EndExecDate=ExecDate,
                             Status="Error",
                             Log="insert目标表出现异常！！！",
                             Developer=developer)
       set_exit(LevelStatu="red", MSG=msg)

#获取表或分区的条数
def get_table_or_partition_rows(HiveSession="",DB="",Table="",IsPartition="",IsCondition="",ExecDate=""):
    if IsPartition == "N":
       if IsCondition == "N":
          sql = """desc formatted %s.%s""" % (DB, Table)
          sql_count = """select count(1) from %s.%s""" % (DB, Table)
       else:
          sql = """select 1"""
          sql_count = """select count(1) from %s.%s where sys_date='%s'""" % (DB, Table, ExecDate)
    else:
       sql = """desc formatted %s.%s partition(sys_date='%s')""" % (DB, Table,ExecDate)
       sql_count = """select count(1) from %s.%s where sys_date='%s'""" % (DB, Table,ExecDate)
    desc_formatted = HiveSession.get_all_rows(sql)
    rows = 0
    for desc_info in desc_formatted[1]:
        if "numRows             " in desc_info:
            rows = int(str(desc_info[2]).strip())
            break;
    if rows <= 0:
        rows = int(HiveSession.get_all_rows(sql_count)[1][0][0])
    return rows

# 获取重要字段相关信息
def get_important_column_info(KeyColumn="",DB="",Table=""):
    key_columns = ""
    table_join_sql = ""
    key_num = 0
    if KeyColumn is None or len(KeyColumn) == 0:
      key_column_list = etl_md.execute_sql(sqlName="get_table_kcolumn_sql", Parameter={"hive_db": DB, "hive_table":Table}, IsReturnData="Y")
      if key_column_list[1] is None or len(key_column_list[1]) == 0:
         KeyColumn = ""
      else:
         KeyColumn = key_column_list[1][0][0]
    if KeyColumn is None or len(KeyColumn) == 0:
       pass
    else:
       for key_column in KeyColumn.split(","):
           key_columns = key_columns + ",`" + key_column.strip() + "`"
           if key_num == 0:
             table_join_sql = "on " + "a.`" + key_column.strip() + "`" + " = " + "b.`" + key_column.strip() + "`"
           else:
             table_join_sql = table_join_sql + " and " + "a.`" + key_column.strip()+"`" + " = " + "b.`" + key_column.strip()+"`"
           key_num = key_num + 1
    key_columns = key_columns.replace(",","",1)
    return key_columns,table_join_sql

#处理数据文件入仓，写入临时表
def set_load_file_to_hive(HiveSession="",MysqlSession="",SourceDB="",SourceTable="",TargetDB="", TargetTable="",TmpDB="",
                          TmpTable="",KeyColumn="",Granularity="",IncColumn="",DataFile="",HDFSDir="",ExecDate=""):

    # 抽取MySQL数据写入文本
    set_mysql_data_to_file(MysqlSession=MysqlSession, HiveSession=HiveSession, SourceDB=SourceDB,
                           SourceTable=SourceTable, TargetDB=TargetDB,
                           TargetTable=TargetTable, KeyColumn=KeyColumn, Granularity=Granularity,
                           IncColumn=IncColumn,ExecDate=ExecDate,DataFile=DataFile)
    # 创建临时表
    create_source_table_tmp(MysqlSession=MysqlSession, HiveSession=HiveSession, SourceDB=SourceDB,
                            SourceTable=SourceTable,
                            TargetDB=TmpDB, TargetTable=TmpTable)
    #上传本地数据文件至HDFS
    print("""hdfs dfs -moveFromLocal -f %s %s"""% (DataFile,HDFSDir),"************************************")
    ok = os.system("hdfs dfs -moveFromLocal -f %s %s" % (DataFile,HDFSDir))
    if ok != 0:
        # 删除数据文件
        os.system("rm -f %s" % DataFile)
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % (SourceDB, SourceTable),
                               TargetTable="%s.%s" % (TargetDB, TargetTable),
                               BeginExecDate="",
                               EndExecDate="",
                               Status="Error",
                               Log="上传本地数据文件至HDFS出现异常！！！",
                               Developer=developer)
        set_exit(LevelStatu="red", MSG=msg)
    load_table_sql = """
        load data inpath '{hdfs_dir}/{file_name}' OVERWRITE  INTO TABLE {db_name}.{table_name}
    """.format(hdfs_dir=HDFSDir,file_name=DataFile.split("/")[-1], db_name=TmpDB, table_name=TmpTable)
    ok = HiveSession.execute_sql(load_table_sql)
    if ok is False:
       #删除临时表
       sql = """drop table if exists %s.%s"""%(TmpDB,TmpTable)
       HiveSession.execute_sql(sql)
       msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                              SourceTable="%s.%s" % (SourceDB, SourceTable),
                              TargetTable="%s.%s" % (TargetDB, TargetTable),
                              BeginExecDate="",
                              EndExecDate="",
                              Status="Error",
                              Log="HDFS数据文件load入仓临时表出现异常！！！",
                              Developer=developer)
       set_exit(LevelStatu="red", MSG=msg)

# 抽取MySQL数据写入文本
def set_mysql_data_to_file(MysqlSession="", HiveSession="", SourceDB="",SourceTable="",TargetDB="",TargetTable="", KeyColumn="",
                           Granularity="", IncColumn="", DataFile="",ExecDate=""):

    begin_system_time = (datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    #获取抽取MySQL查询sql
    get_select_sql_list,source_cnt = get_source_data_sql(MysqlSession=MysqlSession, HiveSession=HiveSession, SourceDB=SourceDB,
                                                         SourceTable=SourceTable,TargetDB=TargetDB, TargetTable=TargetTable,
                                                         KeyColumn=KeyColumn,Granularity=Granularity, IncColumn=IncColumn)
    if type(source_cnt) is tuple:
        source_cnt = source_cnt[0]
    source_file_cnt = 0
    #开启多线程抽取MySQL当期数据
    if len(get_select_sql_list) > 1:
        i = 0
        th = []
        for sql in get_select_sql_list:
            i = i + 1
            etl_thread = EtlThread(thread_id=i, thread_name="Thread_%s.%s_%d" % (airflow.dag,airflow.task, i),
                                   my_run=MysqlSession.select_data_to_local_file, sql=sql, filename=DataFile + "_" + str(i))
            etl_thread.start()
            th.append(etl_thread)
        for etl_th in th:
            etl_th.join()
        while i > 0:
            cnt_str = os.popen('wc -l %s' % DataFile + "_" + str(i))
            cnt_list = cnt_str.read().split()
            source_file_cnt = source_file_cnt + int(cnt_list[0])
            i = i - 1
        os.system("cat %s_* > %s" % (DataFile, DataFile))
        os.system("rm -f %s_* " % DataFile)
        ok = True
    else:
        ok = MysqlSession.select_data_to_local_file(sql=get_select_sql_list[0], filename=DataFile)
        cnt_str = os.popen('wc -l %s' % DataFile)
        cnt_list = cnt_str.read().split()
        source_file_cnt = int(cnt_list[0])
    end_system_time = (datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    #记录数据日志
    set_sync_rows(MysqlSession=etl_md, SourceDB=SourceDB, SourceTable=SourceTable, TargetDB="file", TargetTable=DataFile.split("/")[-1],
                  Module="sync_inc", ExecDate=ExecDate,BeginExecTime=ExecDate,EndExecTime=ExecDate, BeginSystemTime=begin_system_time,
                  EndSystemTime=end_system_time, SourceRows=int(source_cnt), TargetRows=source_file_cnt)
    if ok is False:
      msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                           SourceTable="%s.%s" % (SourceDB, SourceTable),
                           TargetTable="%s.%s" % (TargetDB, TargetTable),
                           BeginExecDate="",
                           EndExecDate="",
                           Status="Error",
                           Log="执行抽取Mysql出现异常！！！",
                           Developer=developer)
      set_exit(LevelStatu="red", MSG=msg)

#处理文本入库创建临时表
def create_source_table_tmp(MysqlSession="",HiveSession="",SourceDB="",SourceTable="",TargetDB="",TargetTable=""):
    # 以来源表结构为主，创建目标表
    get_table_columns = get_create_mysql_table_columns(MysqlSession=MysqlSession, DB=SourceDB, Table=SourceTable)
    source_columns_type = get_table_columns[1]
    create_table_sql = """
                create table %s.%s(
                     %s
                )row format delimited fields terminated by '\\001'
                stored as textfile
             """ % (TargetDB, TargetTable, source_columns_type.replace(",", "", 1))
    HiveSession.execute_sql("""drop table if exists %s.%s"""%(TargetDB,TargetTable))
    ok = HiveSession.execute_sql(create_table_sql)
    return ok

#处理抽取MySQL查询sql
def get_source_data_sql(MysqlSession="",HiveSession="",SourceDB="",SourceTable="",TargetDB="",TargetTable="",KeyColumn="",Granularity="",IncColumn=""):
    source_data_sql = ""
    source_table_column_list =get_create_mysql_table_columns(MysqlSession=MysqlSession,DB=SourceDB,Table=SourceTable)
    sql_list = []
    for column in source_table_column_list[3]:
      if "BIT" in str(column[2]).upper():
          source_data_sql = source_data_sql + """,ifnull(cast(%s as decimal(34,0)),'')""" % (column[1])
      elif (not str(column[2]).upper() in MYSQL_2_HIVE and "INT" not in str(column[2]).upper()) or column[0]:
          source_data_sql = source_data_sql + """,REPLACE( REPLACE( REPLACE( ifnull(%s,''), '|', '|'), '\\n', ' '), '\\r', ' ')""" % (column[1])
      else:
          source_data_sql = source_data_sql + """,ifnull(%s,'')""" % (column[1])
    source_data_sql = """select CONCAT_WS('%s', %s)"""%("\001",source_data_sql.replace(",","",1))
    source_data_sql = source_data_sql + " from %s.%s t" % (SourceDB, SourceTable)
    #查看分区表是否存在
    ok_01,table_count = HiveSession.get_all_rows("""show tables in %s like '%s'"""%(TargetDB,TargetTable))
    #查询分区表是否有前一天分区，若是没有则认为该表为第一次同步，即全量同步
    if ok_01 and len(table_count) > 0:
      ok, count = HiveSession.get_all_rows("""show partitions %s.%s partition(sys_date='%s')"""%(TargetDB,TargetTable,airflow.yesterday_ds_nodash_utc8))
    else:
      ok = False
      count = 0
    if Granularity == 'D' and ok and len(count) > 0:
      #增量抽取
      where_sql = get_sync_date_format(MysqlSession=MysqlSession,SourceDB=SourceDB,SourceTable=SourceTable,TargetDB=TargetDB,TargetTable=TargetTable,
                                       Granularity=Granularity,IncColumn=IncColumn)
      source_data_sql = source_data_sql + " where " + where_sql
      sql_list.clear()
      sql_list.append(source_data_sql)
      count_sql = "select count(1) fcnt from %s.%s t where %s" % (SourceDB, SourceTable, where_sql)
      ok, source_cnt = MysqlSession.get_one_row(count_sql)
    else:
      sql_index = "show index from %s.%s" % (SourceDB, SourceTable)
      # columns为已排序好的tuple（元组）
      ok, columns = MysqlSession.get_all_rows(sql_index)
      get_table_index = get_mysql_table_index(Columns=columns, ColumnsInfo=source_table_column_list[3],IsSelect=True)
      if get_table_index is None:
        #如果没拿到索引字段，就取任务信息里的unique_key字段内容
        unique_key_list = str(KeyColumn).strip().split(",")
        get_table_index = get_mysql_table_index(Columns=unique_key_list, ColumnsInfo=source_table_column_list[3], IsSelect=False)
      if get_table_index:
        find_split_range_sql = "select count(1) fcnt, min(%s) fmin, max(%s) fmax from %s.%s" % (get_table_index, get_table_index, SourceDB, SourceTable)
        ok, find_split_range = MysqlSession.get_one_row(find_split_range_sql)
        fcnt = find_split_range[0]
        fmin = find_split_range[1]
        fmax = find_split_range[2]
        source_cnt = fcnt
        print("min=%s, max=%s, count=%s" % (str(fmin), str(fmax), str(fcnt)))
        if fcnt < 2000 * 10000:
          # 2000万以下的数据量不用分批跑
          sql_list.clear()
          sql_list.append(source_data_sql)
        else:
          sql_list.clear()
          num_proc = int(fmax) - int(fmin)
          if num_proc > 2:
            # 最多2个进程同时获取数据
            num_proc = 2
          # 每一个进程查询量的增量
          d = math.ceil((int(fmax) - int(fmin) + 1) / num_proc)
          i = 0
          while i < num_proc:
            s_ind = int(fmin) + i * d
            e_ind = s_ind + d
            if i == num_proc - 1:
               e_ind = int(fmax) + 1
            sql = source_data_sql + " where " + get_table_index + ">=" + str(s_ind) + " and " + get_table_index + "<" + str(e_ind)
            sql_list.append(sql)
            i = i + 1
      else:
          count_sql = "select count(1) fcnt from %s.%s t" % (SourceDB, SourceTable)
          ok, source_cnt = MysqlSession.get_one_row(count_sql)
          sql_list.clear()
          sql_list.append(source_data_sql)
    return sql_list,source_cnt

#获取抽取日期格式
def get_sync_date_format(MysqlSession="",SourceDB="",SourceTable="",TargetDB="",TargetTable="",Granularity="",IncColumn=""):
    begin_exec_timestamp = airflow.execution_date_start_timestamp
    end_exec_timestamp = airflow.execution_date_end_timestamp
    begin_exec_datetime = airflow.execution_date_utc8_str
    end_exec_datetime = airflow.execution_date_utc8_str
    begin_exec_date = airflow.execution_date_utc8.to_date_string()
    end_exec_date = airflow.execution_date_utc8.to_date_string()
    sql = """
       select column_name,lower(t.data_type),column_type
       from information_schema.columns t
       where t.table_schema = '%s'
       and t.table_name = '%s'
       and t.column_name = '%s'
       and lower(t.data_type) in ('bigint','datetime')
    """ % (SourceDB, SourceTable,IncColumn)
    ok, date_format = MysqlSession.get_all_rows(sql)
    sql = """select %s from %s.%s limit 1""" % (IncColumn, SourceDB, SourceTable)
    ok, data = MysqlSession.get_all_rows(sql)
    if date_format is not None and len(date_format) > 0:
       if date_format[0][1] in ["bigint","int"]:
           # 增量抽取时间类型
           inc_date_type = "TimeStamp"
           # 增量抽取时间格式
           inc_date_format = "0"
       elif date_format[0][1] in ["date","datetime"]:
           inc_date_type,inc_date_format = get_date_type(DateFormat=data[0][0], DateType=date_format[0][1])
       elif date_format[0][1] in ["varchar"]:
           if (("-" not in str(data[0][0])) or ("/" not in str(data[0][0])) or (":" not in str(data[0][0]))) and len(str(data[0][0])) == 13:
               inc_date_type = "TimeStamp"
               inc_date_format = "0"
           else:
               inc_date_type = ""
               inc_date_format = ""
       else:
           inc_date_type = ""
           inc_date_format = ""
    else:
      # 增量抽取时间类型
      inc_date_type = ""
      # 增量抽取时间格式
      inc_date_format = ""
    begin_time = ""
    end_time = ""
    if Granularity == "D":
      if inc_date_type == "TimeStamp":
          begin_time = begin_exec_timestamp
          end_time = end_exec_timestamp
      elif inc_date_type == "DateTime":
        if inc_date_format == "0":
            begin_time = """%s 00:00:00"""%(begin_exec_datetime.split(" ")[0].replace("-",""))
            end_time = """%s 23:59:59"""%(end_exec_datetime.split(" ")[0].replace("-",""))
        elif inc_date_format == "1":
            begin_time = """%s 00:00:00""" % (begin_exec_datetime.split(" ")[0])
            end_time = """%s 23:59:59""" % (end_exec_datetime.split(" ")[0])
        else:
            begin_time = """%s 00:00:00""" % (begin_exec_datetime.split(" ")[0].replace("-", "/"))
            end_time = """%s 23:59:59""" % (end_exec_datetime.split(" ")[0].replace("-", "/"))
      elif inc_date_type == "Date":
        if inc_date_format == "0":
            begin_time = """%s""" % (begin_exec_date.replace("-", ""))
            end_time = """%s""" % (end_exec_date.replace("-", ""))
        elif inc_date_format == "1":
            begin_time = """%s""" % (begin_exec_date)
            end_time = """%s""" % (end_exec_date)
        else:
            begin_time = """%s""" % (begin_exec_date.replace("-", "/"))
            end_time = """%s""" % (end_exec_date.replace("-", "/"))
      else:
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % (SourceDB, SourceTable),
                               TargetTable="%s.%s" % (TargetDB, TargetTable),
                               BeginExecDate=begin_time,
                               EndExecDate=end_time,
                               Status="Error",
                               Log="获取增量日期格式出现异常！！！",
                               Developer=developer)
        set_exit(LevelStatu="red", MSG=msg)
    else:
      return None
    sync_condition = """t.%s >= '%s' and t.%s <= '%s'"""%(IncColumn,begin_time,IncColumn,end_time)
    return sync_condition

#获取日期类型
def get_date_type(DateFormat="",DateType=""):
    if DateType in ["date"]:
        inc_date_type = "Date"
    else:
        inc_date_type = "DateTime"
    if "-" in str(DateFormat):
         inc_date_format = "1"
    elif "/" in str(DateFormat):
          inc_date_format = "2"
    else:
          inc_date_format = "0"
    return inc_date_type,inc_date_format

#保存MySQL表唯一主键
def set_mysql_key_column(MysqlSession="", SourceDB="", SourceTable="",TargetDB="",TargetTable="",ExecDate=""):
    unique_keys = ""
    unique_keys_list = MysqlSession.get_unique_keys(SourceDB, SourceTable)
    ind = 0
    len_list = len(unique_keys_list)
    for key in unique_keys_list:
        if ind < len_list - 1:
            unique_keys = unique_keys + """%s""" % (key) + ","
        else:
            unique_keys = unique_keys + """%s""" % (key)
        ind = ind + 1
    etl_md.execute_sql(sqlName="insert_table_kcolumn_sql",
                       Parameter={"hive_db": TargetDB,"hive_table":TargetTable,"key_column":unique_keys},
                       IsReturnData="N")
    etl_md.execute_sql(sqlName="delete_table_kcolumn_sql",
                       Parameter={"hive_db": TargetDB, "hive_table": TargetTable, "create_time": ExecDate},
                       IsReturnData="N")

#保存数据记录
def set_data_row(HiveSession="",SourceDB="",SourceTable="",TargetDB="",TargetTable="",SourceTableIsCondition="",TargetTableIsCondition="",
                 ExecDate="",BeginSystemTime="",EndSystemTime=""):
    # 增量条数
    if SourceTableIsCondition == "Y":
       sql = """select count(1) from %s.%s where sys_date='%s'""" % (SourceDB, SourceTable, ExecDate)
    else:
       sql = """select count(1) from %s.%s""" % (SourceDB, SourceTable)
    source_rows = int(HiveSession.get_all_rows(sql)[1][0][0])
    if TargetTableIsCondition == "Y":
       sql = """select count(1) from %s.%s where sys_date='%s'""" % (TargetDB, TargetTable, ExecDate)
    else:
       sql = """select count(1) from %s.%s""" % (TargetDB, TargetTable, ExecDate)
    target_rows = int(HiveSession.get_all_rows(sql)[1][0][0])
    # 记录数据日志
    set_sync_rows(MysqlSession=etl_md, SourceDB=SourceDB, SourceTable=SourceTable, TargetDB=TargetDB,
                  TargetTable=TargetTable, Module="sync_inc", ExecDate=ExecDate, BeginExecTime=ExecDate,
                  EndExecTime=ExecDate, BeginSystemTime=BeginSystemTime, EndSystemTime=EndSystemTime,
                  SourceRows=source_rows, TargetRows=target_rows)

#获取来源、目标抽取条数
def get_sync_rows(DB="",Table="",ExecDate="",SyncType=""):
    get_rows = etl_md.execute_sql(sqlName="get_source_target_data_sql",
                                  Parameter={"source_db": DB,"source_table":Table,"exec_date":ExecDate,"module":SyncType},
                                  IsReturnData="Y")
    get_source_rows = 0
    get_target_rows = 0
    if len(get_rows[1]) > 0:
        get_source_rows = int(get_rows[1][0][0])
        get_target_rows = int(get_rows[1][0][1])
    return get_source_rows,get_target_rows

#检测抽取条数
def check_sync_row(SourceDB="",SourceTable="",TargetDB="",TargetTable="",ExecDate="",Level=""):
    msg_list = []
    # 检测本次抽取是否一致
    source_rows, target_rows = get_sync_rows(DB=SourceDB, Table=SourceTable, ExecDate=ExecDate, SyncType="sync_inc")
    # 获取落地文本条数
    get_file_rows = etl_md.execute_sql(sqlName="get_file_rows_sql",
                                       Parameter={"exec_date": ExecDate,"source_db":SourceDB,"source_table":SourceTable},
                                       IsReturnData="Y")
    get_file_row = 0
    get_target_rows = 0
    if Level == "ods":
        get_file_row = int(get_file_rows[1][0][0])
        get_target_rows = int(target_rows)
    if (source_rows != target_rows and Level != "ods") or (get_file_row != get_target_rows) or (int(target_rows) == 0 and int(source_rows) > 0 and Level == "ods") or (target_rows > 0 and abs(source_rows - target_rows) / target_rows > 0.001 and Level == "ods"):
       msg = """数据条数不一致！！！"""
       msg_list.append(msg)
    if target_rows == 0 and Level == "ods":
       msg = """本次抽取导入为0！！！"""
       msg_list.append(msg)
    #历史全量比较
    if Level == "snap":
       #上次
       sql = """SELECT DATE_FORMAT(DATE_SUB('%s',INTERVAL 1 DAY),'##Y##m##d')"""%(ExecDate)
       get_last_data = (etl_md.get_rows_info(sql.replace("##","%")))[1][0][0]
       source_rows_01, target_rows_01 = get_sync_rows(DB=SourceDB, Table=SourceTable, ExecDate=str(get_last_data).replace("-",""), SyncType="sync_full")
       #本次
       source_rows_02, target_rows_02 = get_sync_rows(DB=SourceDB, Table=SourceTable, ExecDate=ExecDate,SyncType="sync_full")
       if target_rows_01 - target_rows_02 > 100000:
          msg = """历史数据降低大于十万条！！！"""
          msg_list.append(msg)
    for msg in msg_list:
       print(msg)
       if msg == """本次抽取导入为0！！！""" or msg == """历史数据降低大于十万条！！！""":
           status = "Warning"
       else:
           status = "Error"
       msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                              SourceTable="%s.%s" % (SourceDB, SourceTable),
                              TargetTable="%s.%s" % (TargetDB, TargetTable),
                              BeginExecDate=ExecDate,
                              EndExecDate=ExecDate,
                              Status=status,
                              Log=msg,
                              Developer=developer)
       set_exit(LevelStatu="red", MSG=msg)