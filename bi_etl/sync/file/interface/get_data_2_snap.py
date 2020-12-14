# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: get_data_2_snap.py
# @Software: PyCharm
# function info：用于同步接口数据到hive ods\snap\backtrace表

from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_alert_info_d
from ecsage_bigdata_etl_engineering.common.base.sync_method import get_table_columns_info


#落地至snap
def exec_snap_hive_table(AirflowDagId="",AirflowTaskId="",HiveSession="",BeelineSession="",SourceDB="",SourceTable="",
                        TargetDB="", TargetTable="",IsReport="",KeyColumns="",ExecDate=""):
   get_select_columns = get_table_columns_info(HiveSession=HiveSession, SourceDB=SourceDB, SourceTable=SourceTable,
                                               TargetDB=TargetDB, TargetTable=TargetTable, IsTargetPartition="N")
   source_table_columns = get_select_columns[2]
   assign_table_columns = get_select_columns[1]
   print(source_table_columns,"################################")
   print(assign_table_columns,"==============================")
   #设置snap查询字段
   snap_columns = ""
   if IsReport == 0:
       is_key_columns(SourceDB=SourceDB, SourceTable=SourceTable, TargetDB=TargetDB,
                      TargetTable=TargetTable, ExecDate=ExecDate, KeyColumns=KeyColumns)
       key_column_list = KeyColumns.split(",")
       is_null_col = "`"+key_column_list[0]+"`"
       key_columns_joins = ""
       num = 0
       for key in key_column_list:
           if num == 0:
              key_columns_join = "on a.`%s` = b.`%s`"%(key,key)
           else:
               key_columns_join = "and a.`%s` = b.`%s`" % (key, key)
           key_columns_joins = key_columns_joins + " " + key_columns_join
           num = num + 1
       #获取ods表字段
       ok,ods_table_columns = HiveSession.get_column_info(SourceDB,SourceTable)
       ods_columns = ""
       for column in ods_table_columns:
           create_col = """,`%s`  %s comment'%s' \n"""%(column[0],column[1],column[2])
           ods_columns = ods_columns + create_col
           if column[0] == "etl_date":
               break;
       ods_columns = ods_columns.replace(",", "", 1)
       create_snap_sql = """
       create table if not exists %s.%s(
         %s
       )
       row format delimited fields terminated by '\\001' 
       stored as parquet
       """%(TargetDB,TargetTable,ods_columns)
       HiveSession.execute_sql(create_snap_sql)
       #获取snap表字段
       ok, snap_table_columns = HiveSession.get_column_info(TargetDB, TargetTable)
       for column in snap_table_columns:
           snap_columns = snap_columns + "," + "a.`%s`"%(column[0])
       snap_columns = snap_columns.replace(",", "", 1)
       sql = """
           insert overwrite table %s.%s
           select %s
           from %s.%s a
           left join %s.%s b
           %s
           and b.etl_date = '%s'
           where b.%s is null
              union all
           select %s from %s.%s a where etl_date = '%s'
       """%(TargetDB,TargetTable,snap_columns,TargetDB,TargetTable,SourceDB,SourceTable,
            key_columns_joins,ExecDate,is_null_col,snap_columns,SourceDB, SourceTable,ExecDate
            )
   else:
       # 获取ods表字段
       ok, ods_table_columns = HiveSession.get_column_info(SourceDB, SourceTable)
       ods_columns = ""
       for column in ods_table_columns:
           create_col = """,`%s`  %s comment'%s' \n""" % (column[0], column[1], column[2])
           ods_columns = ods_columns + create_col
           if column[0] == "etl_date":
               break;
       ods_columns = ods_columns.replace(",", "", 1)
       create_snap_sql = """
              create table if not exists %s.%s(
                %s
              )
              row format delimited fields terminated by '\\001' 
              stored as parquet
              """ % (TargetDB, TargetTable, ods_columns)
       HiveSession.execute_sql(create_snap_sql)
       # 获取snap表字段
       ok, snap_table_columns = HiveSession.get_column_info(TargetDB, TargetTable)
       for column in snap_table_columns:
           snap_columns = snap_columns + "," + "a.`%s`" % (column[0])
       snap_columns = snap_columns.replace(",", "", 1)
       sql = """
        insert overwrite table %s.%s
        select %s
        from %s.%s a
        where etl_date != '%s'
           union all
        select %s
        from %s.%s a where etl_date = '%s' 
       """%(TargetDB,TargetTable,snap_columns,TargetDB,TargetTable,ExecDate,snap_columns,SourceDB,SourceTable,ExecDate)
   ok = BeelineSession.execute_sql(sql)
   if ok is False:
       msg = get_alert_info_d(DagId=AirflowDagId, TaskId=AirflowTaskId,
                              SourceTable="%s.%s" % (SourceDB, SourceTable),
                              TargetTable="%s.%s" % (TargetDB, TargetTable),
                              BeginExecDate=ExecDate,
                              EndExecDate=ExecDate,
                              Status="Error",
                              Log="snap入库失败！！！",
                              Developer="developer")
       set_exit(LevelStatu="red", MSG=msg)
   sql_check = """
               select a.source_cnt,b.target_cnt
               from(select count(1) as source_cnt
                    from %s.%s where etl_date = '%s'
                   ) a
               inner join (
                    select count(1) as target_cnt
                    from %s.%s where etl_date = '%s'
               ) b
               on 1 = 1
               where a.source_cnt <> b.target_cnt
               """%(SourceDB,SourceTable,ExecDate,TargetDB,TargetTable,ExecDate)
   #ok, data = HiveSession.get_all_rows(sql_check)
   data = []
   ok = True
   print("snap入库数据：" + str(data))
   if ok is False or len(data) > 0:
       msg = get_alert_info_d(DagId=AirflowDagId, TaskId=AirflowTaskId,
                              SourceTable="%s.%s" % (SourceDB, SourceTable),
                              TargetTable="%s.%s" % (TargetDB, TargetTable),
                              BeginExecDate=ExecDate,
                              EndExecDate=ExecDate,
                              Status="Error",
                              Log="snap入库数据对比不上！！！",
                              Developer="developer")
       set_exit(LevelStatu="red", MSG=msg)

def is_key_columns(AirflowDagId="",AirflowTaskId="",SourceDB="",SourceTable="",TargetDB="",TargetTable="",ExecDate="",KeyColumns=""):
    if KeyColumns is None or len(KeyColumns) == 0:
        msg = get_alert_info_d(DagId=AirflowDagId, TaskId=AirflowTaskId,
                               SourceTable="%s.%s" % (SourceDB, SourceTable),
                               TargetTable="%s.%s" % (TargetDB, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="请确认配置表指定主键字段是否正确！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)