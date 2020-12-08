# -*- coding: utf-8 -*-
# @Time    : 2020/12/07 17:05
# @Author  :
# @FileName: sync_method.py
# @Software: PyCharm
#function info：Ods表结构自动更新

from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit
from ecsage_bigdata_etl_engineering.common.base.sync_method import get_create_hive_table_columns
import json
#初期表结构字段需要配置表配置，
# 如果desc没有表结构，通过配置字段建表，然后解析etl_mid文件字段，比较 ？desc 比较表结构不同就，alter :看是否可以调用已有的函数
#
#

def def_ods_structure(HiveSession="",BeelineSession="",SourceTable="",TargetDB="",TargetTable="",IsTargetPartition="Y",ExecDate="",Array_Flag="",Specified_Pars_Str=""):
    etlmid_table_columns = []
    etlmid_table_columns_str = analysis_etlmid_cloumns(HiveSession=HiveSession, SourceTable=SourceTable,
                                                       TargetTable=TargetTable
                                                       , ExecDate=ExecDate, Array_Flag=Array_Flag)
    for etlmid_table_column in etlmid_table_columns_str.split(','):
        etlmid_table_columns.append(etlmid_table_column.split(".")[-1])

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
            default_table_columns = "returns_account_id,returns_colums,request_type,extract_system_time"
            # 直接依赖配置的字段信息，类型以String为主
            field_list = []
            strs = str + ',' + default_table_columns
            for field_str in strs.split(","):
                field_list.append(field_str + " String\n")
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

#解析etl_mid文档
def analysis_etlmid_cloumns(HiveSession="",BeelineSession="",SourceTable="", TargetTable="",ExecDate="",Array_Flag=""):
    filter_line = """ where etl_date = '%s' and length(request_data) > 1000 limit 1 """%(ExecDate)
    spec_pars = """dimensions,metrics"""
    spec_pars_list = list(spec_pars.split(","))
    all_pars_list = []
    get_field_sql = """select request_data from %s.%s %s""" % ("etl_mid",SourceTable,filter_line)
    ok, data = HiveSession.get_all_rows(get_field_sql)
    if len(data)>0:
        split_flag = """## {"""
        return_Str= data[0][0]
        #print("获取etl_mid的样本数据" + data[0][0])
        data_str = return_Str[return_Str.find(split_flag) + 3:]
        #print(data_str)
        data_str2 = json.loads(data_str)
        data_str2 = data_str2['data']
        if Array_Flag is not None and len(Array_Flag) > 0:
            data_str3 = data_str2[Array_Flag][0]
        else:
            data_str3 = data_str2
        for keys in data_str3:
            if keys in spec_pars_list and isinstance(data_str3[keys], dict):
                for key in data_str3[keys]:
                    all_pars_list.append(keys + "." + key)
            else:
                all_pars_list.append(keys)
    else:
        msg = "【etl_mid库】中，%s的%s接入数据可能存在异常" % (ExecDate,TargetTable)
        print(msg)
        set_exit(LevelStatu="red", MSG=msg)

    return ','.join(all_pars_list)