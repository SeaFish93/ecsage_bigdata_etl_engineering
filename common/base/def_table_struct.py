# -*- coding: utf-8 -*-
# @Time    : 2020/12/07 17:05
# @Author  :
# @FileName: sync_method.py
# @Software: PyCharm
#function info：Ods表结构自动更新

from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit
from ecsage_bigdata_etl_engineering.common.base.sync_method import get_create_hive_table_columns
import json
import ast
#初期表结构字段需要配置表配置，
# 如果desc没有表结构，通过配置字段建表，然后解析etl_mid文件字段，比较 ？desc 比较表结构不同就，alter :看是否可以调用已有的函数
#
#

def def_ods_structure(HiveSession="",BeelineSession="",SourceTable="",TargetDB="",TargetTable="",IsTargetPartition="Y",ExecDate="",ArrayFlag="",IsReplace="",ExPartField=""):
    etlmid_table_columns = []
    etlmid_table_columns_str = analysis_etlmid_cloumns(HiveSession=HiveSession, SourceTable=SourceTable,
                                                       TargetTable=TargetTable
                                                       , ExecDate=ExecDate, ArrayFlag=ArrayFlag,IsReplace=IsReplace)
    for etlmid_table_column in etlmid_table_columns_str.split(','):
        etlmid_table_columns.append(etlmid_table_column.split(".")[-1])

    default_table_columns = "returns_account_id,returns_colums,request_type,extract_system_time"
    default_table_columns = default_table_columns + ",%s"%(ExPartField[0]) if len(ExPartField) > 0 else default_table_columns
    exclude_fields = set(default_table_columns.split(","))
    exclude_fields.update({'etl_date'})

    sql = """ show tables in %s like '%s' """ % (TargetDB, TargetTable)
    ok, get_target_table = HiveSession.get_all_rows(sql)
    if ok:
      if get_target_table:
         target_table_columns_list = get_create_hive_table_columns(HiveSession=HiveSession, DB=TargetDB, Table=TargetTable)
         target_table_columns = target_table_columns_list[2]
         #找出源表在目标表不一致的字段
         diff_source_target_columns = set(etlmid_table_columns).difference(set(target_table_columns))
         #若是源表含有在目标表没有的字段，则目标表需添加不一致的字段
         if diff_source_target_columns:
            for diff_source_target_column in diff_source_target_columns:
              if diff_source_target_column not in target_table_columns:
                 if IsTargetPartition == "Y":
                    alter_table_sql = """alter table %s.%s add columns(`%s` %s) CASCADE""" % (TargetDB, TargetTable, diff_source_target_column, "String")
                 else:
                    alter_table_sql = """alter table %s.%s add columns(`%s` %s)""" % (TargetDB, TargetTable, diff_source_target_column, "String")
                 HiveSession.execute_sql(alter_table_sql)
      else:
        if IsTargetPartition == "Y":
            # 默认配置表结构
            str = ','.join(etlmid_table_columns)

            # 直接依赖配置的字段信息，类型以String为主
            field_list = []
            strs = str + ',' + default_table_columns
            for field_str in strs.split(","):
                field_list.append("`%s`"%(field_str) + " String\n")
            create_table_colums = ','.join(field_list)

            create_table_sql = """
               create table if not exists  %s.%s(
                     %s
               ) partitioned by(etl_date string)
               row format delimited fields terminated by '\\001' 
               stored as parquet
            """ % (TargetDB, TargetTable, create_table_colums)
            HiveSession.execute_sql(create_table_sql)
    else:
      msg = "获取比较源表%s.%s与目标表字段%s.%s是否一致，发生异常！！！"%(TargetDB,TargetTable)
      print(msg)
      set_exit(LevelStatu="red", MSG=msg)
    target_table_columns_list = get_create_hive_table_columns(HiveSession=HiveSession, DB=TargetDB, Table=TargetTable)
    target_table_columns = target_table_columns_list[2]
    # 找出ODS在etl_mid表不一致的字段
    diff_source_target_columns = set(target_table_columns).difference(set(etlmid_table_columns))

    diff_source_target_columns = list(diff_source_target_columns - exclude_fields)
    #1.ODS-ETL_MID,2,ODS,3.ETL_MID
    return list(diff_source_target_columns),target_table_columns,etlmid_table_columns,etlmid_table_columns_str

#解析etl_mid文档
def analysis_etlmid_cloumns(HiveSession="",BeelineSession="",SourceTable="", TargetTable="",ExecDate="",ArrayFlag="",IsReplace="Y"):
    spec_pars = """dimensions,metrics"""
    spec_pars_list = list(spec_pars.split(","))
    all_pars_list = []
    #filter_line = """ where etl_date = '%s'""" % (ExecDate)
    #get_field_sql = """select request_data from %s.%s %s""" % ("etl_mid",SourceTable,filter_line)
    get_field_sql = """select request_data
                              from %s.%s 
                              where etl_date = '%s' and request_data like '%%len_flag": "Y%%' limit 1
                    """ % ("etl_mid",SourceTable,ExecDate)

    ok, data = HiveSession.get_all_rows(get_field_sql)
    if len(data)>0:
        split_flag = """## {"""
        return_Str= data[0][0]
        #print(return_Str)
        #print("获取etl_mid的样本数据" + data[0][0])
        if IsReplace == "N":
            print("------------------------------")
            data_str = return_Str
            data_str2 = json.loads((json.loads(json.dumps(data_str))))
            data = data_str2['data']
            if isinstance(data,list):
                data_str2=data[0]
                print("+===================++")
            else:
                data_str2=data_str2['data']
                print("+++++++++++++++++++++++")
        else:
            data_str = return_Str[return_Str.find(split_flag) + 3:]
            data_str2 = json.loads(data_str)
            data_str2 = data_str2['data']
            print(data_str)
        if ArrayFlag is not None and len(ArrayFlag) > 0:
            data_str3 = data_str2[ArrayFlag][0]
        else:
            data_str3 = data_str2
        for keys in data_str3:
            if keys in spec_pars_list and isinstance(data_str3[keys], dict):
                for key in data_str3[keys]:
                    all_pars_list.append(keys + "." + key)
            else:
                all_pars_list.append(keys)
        print(all_pars_list)
    else:
        msg = "【etl_mid库】中，%s的%s接入数据可能存在异常" % (ExecDate,TargetTable)
        print(msg)
        set_exit(LevelStatu="red", MSG=msg)
    return ','.join(all_pars_list)

def adj_snap_structure(HiveSession="",BeelineSession="",SourceDB="",SourceTable="",
                        TargetDB="", TargetTable="",CustomSetParameter="",IsReport=""):
    # 获取ods表字段
    ok, src_table_structure = HiveSession.get_column_info(SourceDB, SourceTable)
    tgt_tb_create = [] #收集目标表结构语句
    src_tb_columns= [] #收集源表表结构字段
    IsTargetPartition = "Y" if IsReport==1 else "N"
    for columns in src_table_structure:
        create_col = """,`%s`  %s comment'%s' \n""" % (columns[0], columns[1], columns[2])
        if columns[0] == "etl_date":
            if IsTargetPartition=="N" :#默认非日报=不分区
                tgt_tb_create.append(create_col)
                src_tb_columns.append(columns[0])
            break;
        else:
            tgt_tb_create.append(create_col)
            src_tb_columns.append(columns[0])
    tgt_tb_create_str = ''.join(tgt_tb_create).replace(",", "", 1)
    #print(tgt_tb_create_str)


    # 明确Snap存在与否
    sql = """ show tables in %s like '%s' """ % (TargetDB, TargetTable)
    ok, get_target_table = HiveSession.get_all_rows(sql)

    if ok:
        if get_target_table :
            target_table_columns_list = get_create_hive_table_columns(HiveSession=HiveSession, DB=TargetDB,Table=TargetTable)
            tgt_tb_columns = target_table_columns_list[2]
            # 找出源表在目标表不一致的字段
            diff_src_tgt_columns = set(src_tb_columns).difference(set(tgt_tb_columns))
            print(diff_src_tgt_columns)
            # 若是源表含有在目标表没有的字段，则目标表需添加不一致的字段
            if diff_src_tgt_columns:
                for diff_src_tgt_column in diff_src_tgt_columns:
                    if diff_src_tgt_column not in tgt_tb_columns:
                        if IsTargetPartition == "Y":
                            alter_table_sql = """alter table %s.%s add columns(`%s` %s) CASCADE""" % (TargetDB, TargetTable, diff_src_tgt_column, "String")
                        else:
                            alter_table_sql = """alter table %s.%s add columns(`%s` %s)""" % (TargetDB, TargetTable, diff_src_tgt_column, "String")
                        HiveSession.execute_sql(alter_table_sql)
        else:
            if IsTargetPartition=="N":
                create_snap_sql = """
                       create table if not exists %s.%s(
                         %s
                       )
                       row format delimited fields terminated by '\\001' 
                       stored as parquet
                       """ % (TargetDB, TargetTable, tgt_tb_create_str)
            else:
                create_snap_sql = """
                                       create table if not exists %s.%s(
                                         %s
                                       )partitioned by(etl_date string)
                                       row format delimited fields terminated by '\\001' 
                                       stored as parquet
                                       """ % (TargetDB, TargetTable, tgt_tb_create_str)
            HiveSession.execute_sql(create_snap_sql)
    else:
        msg = "语句：查询Snap表存在与否出现问题！！" % (TargetDB, TargetTable)
        print(msg)
        set_exit(LevelStatu="red", MSG=msg)
