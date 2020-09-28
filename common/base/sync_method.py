# -*- coding: utf-8 -*-
# @Time    : 2019/11/19 17:05
# @Author  : wangsong
# @FileName: sync_method.py
# @Software: PyCharm
#function info：数据采集方法

from ecsage_bigdata_etl_engineering.common.base.sensitive_column import sensitive_column
from ecsage_bigdata_etl_engineering.config.column_type import get_column_hive_type
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit

# 处理mysql2ods表
def get_mysql_hive_table_column(HiveSession="",MysqlSession="",ETLMysqlSession="",SourceDB="",SourceTable="",TargetDB="", TargetTable="",IsTargetPartition=""):
    #记录同步MySQL表字段信息
    ok, column_count = ETLMysqlSession.execute_sql(sqlName="get_data_sync_tasks_sql",
                                                   Parameter={"source_db":SourceDB,"source_table":SourceTable
                                                              ,"target_db":TargetDB,"target_table":TargetTable},
                                                   IsReturnData="Y")
    if column_count is None or len(column_count) == 0:
        sql = """select * from information_schema.columns t where t.table_schema = '%s' and t.table_name = '%s'""" % (SourceDB, SourceTable)
        ok, get_column = MysqlSession.get_all_rows(sql)
        for insert_column in get_column:
            ETLMysqlSession.execute_sql(sqlName="insert_sync_columns_info_sql",
                                        Parameter={"source_db": SourceDB,"source_table": SourceTable,"target_db": TargetDB,"target_table": TargetTable,
                                                   "column_name": insert_column[3],"column_type": insert_column[15],"column_comment": insert_column[19],
                                                   "column_key": insert_column[16],"is_alter": 0,"status": 1},
                                        IsReturnData="N")
    #判断目标表是否存在
    sql = """ show tables in %s like '%s' """ % (TargetDB, TargetTable)
    ok, get_target_table = HiveSession.get_all_rows(sql)
    #获取来源表信息
    sql = """
      select lower(column_name),data_type ,column_name
      from information_schema.columns t 
      where t.table_schema = '%s'
        and t.table_name = '%s'
    """%(SourceDB,SourceTable)
    ok, source_table_columns_type = MysqlSession.get_all_rows(sql)
    source_table_columns = []
    source_table_columns_source = []
    for source_table_column in source_table_columns_type:
        source_table_columns.append(source_table_column[0])
        source_table_columns_source.append(source_table_column[2])
    if ok:
      if get_target_table:
         #判断是否与MySQL字段一致
         target_table_columns_list = get_create_hive_table_columns(HiveSession=HiveSession, DB=TargetDB, Table=TargetTable)
         target_table_columns = target_table_columns_list[2]
         # 找出源表在目标表不一致的字段
         diff_source_target_columns = set(source_table_columns).difference(set(target_table_columns))
         # 若是源表含有在目标表没有的字段，则目标表需添加不一致的字段
         if diff_source_target_columns:
           msg = "表：【%s.%s】新增字段【%s】，请对应表采集人员检测所新增字段是否含有敏感信息！！！"%(SourceDB,SourceTable,diff_source_target_columns)
           #push_qywx_msg(msg)
           for diff_source_target_column in diff_source_target_columns:
             sql = """
                 select column_name,data_type,character_maximum_length,column_type,column_comment,column_key
                 from information_schema.columns t 
                 where t.table_schema = '%s'
                   and t.table_name = '%s'
                   and t.column_name = '%s'
             """ % (SourceDB, SourceTable,diff_source_target_column)
             ok, get_diff_column = MysqlSession.get_all_rows(sql)
             #记录新增字段
             ETLMysqlSession.execute_sql(sqlName="insert_sync_columns_info_sql",
                                         Parameter={"source_db": SourceDB, "source_table": SourceTable,
                                                    "target_db": TargetDB, "target_table": TargetTable,
                                                    "column_name": get_diff_column[0][0], "column_type": get_diff_column[0][3],
                                                    "column_comment": get_diff_column[0][4],
                                                    "column_key": get_diff_column[0][5], "is_alter": 1, "status": 1},
                                         IsReturnData="N")
             ok, col, sensitive_col = sensitive_column("mysql", SourceDB, SourceTable, get_diff_column[0][0])
             col_type = get_column_hive_type(get_diff_column[0])
             if ok:
               col_type = "string"
             if IsTargetPartition == "Y":
                alter_table_sql = """alter table %s.%s add columns(`%s` %s) CASCADE""" % (TargetDB, TargetTable, get_diff_column[0][0], col_type)
             else:
                alter_table_sql = """alter table %s.%s add columns(`%s` %s)""" % (TargetDB, TargetTable, get_diff_column[0][0], col_type)
             HiveSession.execute_sql(alter_table_sql)
      else:
         #以来源表结构为主，创建目标表
         get_table_columns = get_create_mysql_table_columns(MysqlSession=MysqlSession,DB=SourceDB,Table=SourceTable)
         source_columns_type = get_table_columns[1]
         create_table_sql = """
            create table %s.%s(
                 %s
                 ,extract_system_time  string
            ) partitioned by(etl_date string)
            row format delimited fields terminated by '\\001' 
            stored as parquet
         """ % (TargetDB, TargetTable, source_columns_type.replace(",", "", 1))
         HiveSession.execute_sql(create_table_sql)
    else:
      msg = "获取比较源表%s.%s与目标表字段%s.%s是否一致，发生异常！！！" % (SourceDB, SourceTable, TargetDB, TargetTable)
      print(msg)
      set_exit(LevelStatu="red", MSG=msg)

    return get_select_column_info(HiveSession=HiveSession, TargetDB=TargetDB, TargetTable=TargetTable,SourceTableColumn=source_table_columns,IsTargetPartition=IsTargetPartition)

#获取mysql创建表字段
def get_create_mysql_table_columns(MysqlSession="",DB="",Table=""):
    columns_list = []
    columns_type_list = []
    md5_column_type_list = []
    create_columns = ""
    sql = """
      select column_name,data_type,character_maximum_length,column_type
      from information_schema.columns t 
      where t.table_schema = '%s'
        and t.table_name = '%s'
    """ % (DB, Table)
    ok, get_columns = MysqlSession.get_all_rows(sql)
    if ok is False:
       msg = "获取创建表%s.%s的执行sql失败！！！"% (DB, Table)
       print(msg)
       set_exit(LevelStatu="red", MSG=msg)
    for column in get_columns:
       ok, col, sensitive_col = sensitive_column("mysql", DB, Table, column[0])
       # 该字段是否需要加密处理
       if ok:
         # md5加密处理后的字段类型都是string
         target_table_column_type = ("""%s"""%column[0],"""string""")
         #加密、字段名、字段类型
         md5_column_type = (True,sensitive_col,"string",(column[0],column[1]))
       else:
         #转换MySQL字段类型为hive字段类型
         target_table_column_type = ("""%s"""%column[0],get_column_hive_type(column))
         md5_column_type = (False,sensitive_col, column[1],(column[0],column[1]))
       columns_list.append("""%s"""%column[0])
       columns_type_list.append(target_table_column_type)
       md5_column_type_list.append(md5_column_type)
       create_columns = create_columns + """,`%s`  %s""" % (target_table_column_type[0], target_table_column_type[1]) + "\n"
    return columns_type_list,create_columns,columns_list,md5_column_type_list

#获取源表或目标表字段信息
def get_table_columns_info(HiveSession="",SourceDB="",SourceTable="",TargetDB="",TargetTable="",IsTargetPartition=""):
    #获取源表字段相关信息
    source_table_columns_list = get_create_hive_table_columns(HiveSession=HiveSession, DB=SourceDB, Table=SourceTable)
    source_table_columns = source_table_columns_list[2]
    source_table_columns_type = source_table_columns_list[0]
    #判断目标表是否存在:
    # 1、若是存在，则判断源表与目标表字段是否一致，若是不一致，则保存一致
    # 2、若是不存在，则以源表为主，创建目标表
    sql = """ show tables in %s like '%s' """%(TargetDB,TargetTable)
    ok, get_target_table = HiveSession.get_all_rows(sql)
    if ok:
      if get_target_table:
         target_table_columns_list = get_create_hive_table_columns(HiveSession=HiveSession, DB=TargetDB, Table=TargetTable)
         target_table_columns = target_table_columns_list[2]
         target_table_columns_type = target_table_columns_list[0]
         #找出源表在目标表不一致的字段
         diff_source_target_columns_type = set(source_table_columns_type).difference(set(target_table_columns_type))
         #若是源表含有在目标表没有的字段，则目标表需添加不一致的字段
         if diff_source_target_columns_type:
            for diff_source_target_column_type in diff_source_target_columns_type:
              if diff_source_target_column_type[0] not in target_table_columns:
                 if IsTargetPartition == "Y":
                    alter_table_sql = """alter table %s.%s add columns(`%s` %s) CASCADE""" % (TargetDB, TargetTable, diff_source_target_column_type[0], diff_source_target_column_type[1])
                 else:
                    alter_table_sql = """alter table %s.%s add columns(`%s` %s)""" % (TargetDB, TargetTable, diff_source_target_column_type[0], diff_source_target_column_type[1])
                 HiveSession.execute_sql(alter_table_sql)
      else:
        if IsTargetPartition == "Y":
            get_table_columns = get_create_hive_table_columns(HiveSession=HiveSession, DB=SourceDB, Table=SourceTable)
            source_columns_type = get_table_columns[1]
            etl_time_column = ",extract_system_time  string"
            if """`extract_system_time`""" in source_columns_type:
                etl_time_column = ""
            create_table_sql = """
                 create table %s.%s(
                   %s
                   %s
                 ) partitioned by(etl_date string)
                 row format delimited fields terminated by '\\001' 
                 stored as parquet
            """ % (TargetDB, TargetTable, source_columns_type.replace(",", "", 1).replace(""",`etl_date`  string""",""),etl_time_column)
            HiveSession.execute_sql(create_table_sql)
        else:
            get_table_columns = get_create_hive_table_columns(HiveSession=HiveSession, DB=SourceDB, Table=SourceTable)
            source_columns_type = get_table_columns[1]
            create_table_sql = """
                create table %s.%s(
                  %s
                ) row format delimited fields terminated by '\\001' stored as parquet
            """%(TargetDB,TargetTable,source_columns_type.replace(",","",1))
            HiveSession.execute_sql(create_table_sql)
    else:
      msg = "获取比较源表%s.%s与目标表字段%s.%s是否一致，发生异常！！！"%(SourceDB,SourceTable,TargetDB,TargetTable)
      print(msg)
      set_exit(LevelStatu="red", MSG=msg)

    return get_select_column_info(HiveSession=HiveSession,TargetDB=TargetDB,TargetTable=TargetTable,SourceTableColumn=source_table_columns,IsTargetPartition=IsTargetPartition)

#保存抽取记录
def set_sync_rows(MysqlSession="",SourceDB="",SourceTable="",TargetDB="",TargetTable="",Module="",ExecDate="",BeginExecTime="",
                  EndExecTime="",BeginSystemTime="",EndSystemTime="",SourceRows="",TargetRows=""):
    # 记录条数
    MysqlSession.execute_sql(sqlName="insert_etl_job_rows_sql",
                             Parameter={"source_db": SourceDB,"source_table":SourceTable,"target_db":TargetDB,"target_table":TargetTable
                                        ,"module":Module,"exec_date":ExecDate,"begin_exec_time":BeginExecTime,"end_exec_time":EndExecTime
                                        ,"begin_system_time":BeginSystemTime,"end_system_time":EndSystemTime,"source_rows":int(SourceRows),"target_rows":int(TargetRows)
                                        },
                             IsReturnData="N")

#查找MySQL索引字段，且字段类型为int、bigint
def get_mysql_table_index(Columns=[],ColumnsInfo=(()),IsSelect=True):
    index_column = None
    for ind in Columns:
      # 拿到第一个int、bigint类型的索引字段
      for comm in ColumnsInfo:
        if IsSelect:
          if comm[3][0] == ind[4] and "int" in comm[3][1]:
             index_column = ind[4]
             break
        else:
          if comm[3][0] == ind and "int" in comm[3][1]:
             index_column = ind
             break
        if index_column:
            break
    return index_column

#获取hive创建表字段
def get_create_hive_table_columns(HiveSession="",DB="",Table="",IsPartition=""):
    columns_list = []
    columns_type_list = []
    create_columns = ""
    sql = """ desc %s.%s """ % (DB, Table)
    ok, get_columns = HiveSession.get_all_rows(sql)
    if ok is False:
       msg = "获取创建表%s.%s的执行sql失败！！！"% (DB, Table)
       print(msg)
       set_exit(LevelStatu="red", MSG=msg)
    for column in get_columns:
        if column[1] is None:
            break;
        columns_list.append(column[0])
        columns_type_list.append(column)
        if IsPartition == "Y" and str(column[0]).strip() not in ["extract_system_time","etl_date"]:
            create_columns = create_columns + """,`%s`  %s""" % (column[0], column[1]) + "\n"
        else:
            create_columns = create_columns + """,`%s`  %s""" % (column[0], column[1]) + "\n"
    return columns_type_list,create_columns,columns_list

#获取查询字段
def get_select_column_info(HiveSession="",TargetDB="",TargetTable="",SourceTableColumn="",IsTargetPartition=""):
    # 重新获取目标表字段
    target_table_columns_list = get_create_hive_table_columns(HiveSession=HiveSession, DB=TargetDB, Table=TargetTable)
    target_table_columns = target_table_columns_list[2]
    # 找出目标表在源表不一致的字段
    diff_target_source_column = set(target_table_columns).difference(set(SourceTableColumn))
    select_target_columns = ""
    assign_target_columns = ""
    select_source_columns = ""
    assign_source_columns = ""
    for target_table_column in target_table_columns:
        if target_table_column in diff_target_source_column and target_table_column == "extract_system_time":
            select_target_columns = select_target_columns + """,%s as extract_system_time""" % ("FROM_UNIXTIME(UNIX_TIMESTAMP())")
            assign_target_columns = assign_target_columns + """,%s as extract_system_time""" % ("FROM_UNIXTIME(UNIX_TIMESTAMP())")
            select_source_columns = select_source_columns + """,%s as extract_system_time""" % ("FROM_UNIXTIME(UNIX_TIMESTAMP())")
            assign_source_columns = assign_source_columns + """,%s as extract_system_time""" % ("FROM_UNIXTIME(UNIX_TIMESTAMP())")
        elif IsTargetPartition == "Y" and target_table_column == "etl_date":
            pass
        elif target_table_column in diff_target_source_column:
            select_target_columns = select_target_columns + """,`%s`""" % (target_table_column)
            assign_target_columns = assign_target_columns + """,a.`%s`""" % (target_table_column)
            select_source_columns = select_source_columns + """,null as %s""" % (target_table_column)
            assign_source_columns = assign_source_columns + """,null as %s""" % (target_table_column)
        else:
            select_target_columns = select_target_columns + """,`%s`""" % (target_table_column)
            assign_target_columns = assign_target_columns + """,a.`%s`""" % (target_table_column)
            select_source_columns = select_source_columns + """,`%s`""" % (target_table_column)
            assign_source_columns = assign_source_columns + """,a.`%s`""" % (target_table_column)
    select_target_columns = select_target_columns.replace(",", "", 1)
    assign_target_columns = assign_target_columns.replace(",", "", 1)
    select_source_columns = select_source_columns.replace(",", "", 1)
    assign_source_columns = assign_source_columns.replace(",", "", 1)
    return select_target_columns, assign_target_columns,select_source_columns, assign_source_columns

def get_interface_2_hive_table_sql(DB="",Table="",InterfaceAcountType=""):
    if InterfaceAcountType is not None:
      sql = """
         drop table if exists %s.%s;
         create table %s.%s(data string)
         partitioned by(etl_date string,type string)
      """%(DB,Table,DB,Table)
    else:
      sql = """
         drop table if exists %s.%s;
         create table %s.%s(data string)
         partitioned by(etl_date string)
      """%(DB,Table,DB,Table)
    return sql