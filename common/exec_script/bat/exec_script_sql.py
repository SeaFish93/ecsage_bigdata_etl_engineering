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
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_alert_info_d
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit


def run(jd,no_run_date, **kwargs):
    # 开始处理从hive加工数据到dws层
    global airflow
    airflow = Airflow(kwargs)
    # 打印airflow task信息到日志
    engine_type = jd[11]
    business = jd[2]
    dw_level = jd[3]
    target_db= jd[5]
    target_table = jd[6]
    #if engine_type == "beeline":
    #    session = set_db_session(SessionType="beeline", SessionHandler="hive",AppName=airflow.dag + "." + airflow.task)
    #elif engine_type == "hive":
    #    session = set_db_session(SessionType="hive", SessionHandler="hive",AppName=airflow.dag + "." + airflow.task)
    #elif engine_type == "spark":
    #    session = set_db_session(SessionType="spark", SessionHandler="spark",AppName=airflow.dag + "." + airflow.task)
    session = set_db_session(SessionType=engine_type, SessionHandler=engine_type, AppName=airflow.dag + "." + airflow.task)
    #执行sql
    task_module = get_task_module(Business=business,DWLevel=dw_level, DB=target_db, Table=target_table)
    sql_list = task_module.SQL_LIST
    ok = True
    for sql in sql_list:
        sql_str = replace_placeholder(sql["sql"])
        print(sql_str)
        ok = session.execute_sql(sql=sql_str)
        if ok is False:
            msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                                   SourceTable="",
                                   TargetTable="%s.%s" % (jd[5], jd[6]),
                                   BeginExecDate="",
                                   EndExecDate="",
                                   Status="Error",
                                   Log="执行sql失败！！！",
                                   Developer="test")
            set_exit(LevelStatu="red", MSG=msg)

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