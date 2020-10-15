# -*- coding: utf-8 -*-
# @Time    : 2019/1/23 19:30
# @Author  : wangsong
# @FileName: exec_script_sql.py
# @Software: PyCharm
import time
import datetime
import importlib
import pendulum


from ecsage_bigdata_etl_engineering.common.base.airflow_instance import Airflow
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit
from ecsage_bigdata_etl_engineering.common.operator.mysql.conn_mysql_metadb import EtlMetadata
import re

def run(jd, **kwargs):
    # 开始处理从hive加工数据到dws层
    global airflow
    etl_md = EtlMetadata()
    airflow = Airflow(kwargs)
    # 打印airflow task信息到日志
    engine_type = "hive" #jd[11]
    target_db= jd[5]
    target_table = jd[6]
    business = jd[2]
    dw_level = jd[3]
    dw_granularity = str(jd[4]).lower()
    if engine_type == "beeline":
        session = set_db_session(SessionType="beeline", SessionHandler="hive",AppName=airflow.dag + "." + airflow.task)
    elif engine_type == "hive":
        session = set_db_session(SessionType="hive", SessionHandler="hive",AppName=airflow.dag + "." + airflow.task)
    elif engine_type == "spark":
        session = set_db_session(SessionType="spark", SessionHandler="spark",AppName=airflow.dag + "." + airflow.task)
    #执行sql
    task_module = get_task_module(Business=business,DWLevel=dw_level, DB=target_db, Table=target_table)
    sql_list = task_module.SQL_LIST
    ok = True
    task_list = []
    for sql in sql_list:
        sql_str = "EXPLAIN DEPENDENCY " + re.sub(";$","",re.sub("set .*","",replace_placeholder(sql["sql"])))
        ok,data = session.get_all_rows(sql_str) #.execute_sql(sql=sql_str)
        get_depend_table = data[0][0]
        for table in eval(get_depend_table)["input_tables"]:
            if table["tabletype"] == "MANAGED_TABLE":
               print(table["tablename"])
               table_name = str(table["tablename"]).split("@")[1]
               db_name = str(table["tablename"]).split("@")[0]
               if db_name in ["ods","snap","history","sensitive"]:
                   dep_task_id = db_name+"_" + table_name
               else:
                   dep_task_id = db_name + "_" + table_name
               ok,get_data = etl_md.execute_sql(sqlName="get_depend_sql",Parameter={"task_id": target_db+"_"+target_table, "dep_task_id": dep_task_id},IsReturnData="Y")
               ok, get_is_task_data = etl_md.execute_sql(sqlName="get_is_task_sql",Parameter={"task_id": target_db + "_" + target_table}, IsReturnData="Y")
               if len(get_is_task_data) > 0:
                   task_list.append(dep_task_id)
               if get_data[0][0] == 0 and len(get_is_task_data) > 0 and target_db+"_"+target_table != dep_task_id:
                  etl_md.execute_sql(sqlName="insert_depend_sql", Parameter={"task_id": target_db+"_"+target_table,"dep_task_id":dep_task_id}, IsReturnData="N")
        if ok is False:
            set_exit(LevelStatu="red", MSG="")
    if len(task_list) > 0:
       #任务所有的依赖
       ok, get_list_depend_data = etl_md.execute_sql(sqlName="get_list_depend_sql", Parameter={"task_id": target_db + "_" + target_table},IsReturnData="Y")
       if len(get_list_depend_data) > 0:
         get_list_depends = []
         for get_list_depend in get_list_depend_data:
           get_list_depends.append(get_list_depend[0])
         for difference_data in set(get_list_depends).difference(set(task_list)):
             #删除多余的依赖
             etl_md.execute_sql(sqlName="get_delete_depend_sql",Parameter={"task_id": target_db + "_" + target_table,"dep_task_id": difference_data}, IsReturnData="N")

def replace_placeholder(txt):
    trx_dt = airflow.ds_nodash_utc8
    trx_yr = airflow.dt.year
    if airflow.dt.month > 9:
        trx_month = str(airflow.dt.month)
    else:
        trx_month = "0"+str(str(airflow.dt.month))
    if airflow.dt.day > 9:
        trx_day = str(airflow.dt.day)
    else:
        trx_day = "0"+str(str(airflow.dt.day))
    dt = airflow.dt
    trx_first_day_of_month = pendulum.datetime(dt.year, dt.month, 1, 0, 0, 0, 0, "Asia/Shanghai").to_date_string().replace("-", "")
    trx_last_day_of_month = pendulum.datetime(dt.year, dt.month, 1, 0, 0, 0, 0, "Asia/Shanghai") \
        .add(months=1).subtract(days=1).to_date_string().replace("-", "")
    trx_last_date_of_month = pendulum.datetime(dt.year, dt.month, 1, 0, 0, 0, 0, "Asia/Shanghai") \
        .add(months=1).subtract(days=1).to_date_string()
    yesterday = airflow.yesterday_ds_nodash_utc8
    trx_yesterday_date = airflow.dt.subtract(days=1).to_date_string()
    trx_next_date = airflow.dt.subtract(days=-1).to_date_string()
    trx_next_dt = airflow.dt.subtract(days=-1).to_date_string().replace("-", "")
    trx_date = airflow.dt.to_date_string()
    trx_datetime = airflow.dt.to_datetime_string()
    trx_yesterday_yyyy_mm_dd = airflow.dt.subtract(days=1).to_date_string().replace("-", "_")
    return txt.replace("${trx_dt}", str(trx_dt)) \
        .replace("${trx_yr}", str(trx_yr)) \
        .replace("${trx_month}", str(trx_month)) \
        .replace("${trx_day}", str(trx_day)) \
        .replace("${trx_first_day_of_month}", trx_first_day_of_month) \
        .replace("${trx_last_day_of_month}", trx_last_day_of_month) \
        .replace("${trx_last_date_of_month}", trx_last_date_of_month) \
        .replace("${trx_yesterday}", str(yesterday)) \
        .replace("${trx_date}", str(trx_date)) \
        .replace("${trx_datetime}", str(trx_datetime)) \
        .replace("${trx_yesterday_date}", str(trx_yesterday_date))\
        .replace("${trx_next_date}", str(trx_next_date))\
        .replace("${trx_next_dt}", str(trx_next_dt)) \
        .replace("${trx_yesterday_yyyy_mm_dd}", str(trx_yesterday_yyyy_mm_dd))
def get_task_module(Business="",DWLevel="",DB="",Table=""):
#Business：所属项目名称,DWLevel：所属项目的包名,DB：目标数据库,Table：目标表名
    try:
        pkg = ".%s.%s.%s" % (DWLevel, DB, Table)
        module = importlib.import_module(pkg, package=Business)
        return module
    except Exception as e:
        msg = "|**** Error: 获取SQL文件失败 %s" % e
        #set_error_msg(msg)
        raise Exception("获取SQL文件失败!")