# -*- coding: utf-8 -*-
# @Time    : 2019/1/23 19:30
# @Author  : wangsong
# @FileName: exec_script_sql.py
# @Software: PyCharm
import time
import datetime
import importlib
import pendulum
import random


from etl_main.common.airflow_instance import Airflow
from etl_main.common.hive_operator import HiveNoSqlDB
#from etl_main.common.spark_operator import SparkNoSqlDB
#from etl_main.common.presto_operator import PrestoDB
#from etl_main.common.beeline_operator import BeelineNoSqlDB
from etl_main.common.check_result import check_table_data_count
#from akulaku_etl.common.send_msg import (push_qywx_msg, set_error_msg)
#from akulaku_etl.common.get_table_dependent import get_down_table

global warning
global airflow
global mysql_db
global hive_db
global beeline_db
global presto
global job_detail
global task_module
warning = ""
airflow = None
mysql_db = None
hive_db = None
presto = None
beeline_db = None
job_detail = None
task_module = None


def finally_del():
    pass


def get_sql_list():
    try:
        pkg = ".bi_etl.%s.%s" % (job_detail[3], job_detail[4])
        module = importlib.import_module(pkg, package="akulaku_etl")
        return module.SQL_LIST
    except Exception as e:
        msg = "|**** Error: 获取SQL文件失败 %s" % e
        #set_error_msg(msg)
        finally_del()
        exit(1)


def get_task_module():
    try:
        pkg = ".bi_etl.%s.%s" % (job_detail[1], job_detail[4])
        module = importlib.import_module(pkg, package="akulaku_etl")
        return module
    except Exception as e:
        msg = "|**** Error: 获取SQL文件失败 %s" % e
        #set_error_msg(msg)
        raise Exception("获取SQL文件失败!")
    return None


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


def submit_excute_job():
    sql_list = task_module.SQL_LIST
    # worker = job_detail[17]
    ok = True
    for sql in sql_list:
        sql_str = replace_placeholder(sql["sql"])
        if sql["excute_engine"] == "hive":
            ok = hive_db.execute_sql(sql_str)
        elif sql["excute_engine"] == "presto":
            ok = presto.execute_sql(sql_str)
        elif sql["excute_engine"] == "beeline":
            tb = "%s_%s" % (job_detail[3], job_detail[4])
            ok = beeline_db.execute_sql(sql=sql_str, task_name=tb)
        else:
            # 默认spark引擎处理
          #  spark_db = SparkNoSqlDB(port=conf.get(SOURCE_TYPE["spark"]["config"], "port"),
          #                          host=conf.get(SOURCE_TYPE["spark"]["config"], "host"),
          #                          user=conf.get(SOURCE_TYPE["spark"]["config"], "user"),
          #                          metastore_uris=conf.get(SOURCE_TYPE["spark"]["config"], "hive.metastore.uris"),
          #                          app_name=airflow.dag + "." + airflow.task)
          #  ok = spark_db.execute_sql(sql_str)
          #  spark_db.spark_conn_close()
          pass
        if not ok:
            return ok
    return ok


def delete_old_partition():
    old_partition = airflow.dt.subtract(days=3).to_date_string().replace("-", "")
    del_table_partition = "%s.%s"
    # 约定所有日分区字段名都为dw_snsh_dt
    drop_sql = "alter table %s.%s drop if exists partition (%s=%s)" % (job_detail[3], job_detail[4], "dw_snsh_dt", old_partition)
    return hive_db.execute_sql(drop_sql)


def run(jd,no_run_date, **kwargs):
    # 开始处理从hive加工数据到dws层
    global airflow
    global job_detail
    global warning
    global task_module
    global hive_db
    global beeline_db
    global presto
    global db_name
    global table_name
    global dag_id
    global task_id
    global exec_date
    global sys_time
    exit(0)
    airflow = Airflow(kwargs)
    job_detail = jd
    warning = ""
    db_name = job_detail[1]
    table_name = job_detail[4]
    dag_id = airflow.dag
    task_id = airflow.task
    exec_date = airflow.ds_nodash_utc8
    sys_time = (datetime.datetime.now() + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    # 打印airflow task信息到日志
    start_time = time.time()
    print(kwargs)
    exit(0)
    hive_db = HiveNoSqlDB(port=conf.get(SOURCE_TYPE["hive"]["config"], "port"),
                          host=conf.get(SOURCE_TYPE["hive"]["config"], "host"),
                          user=conf.get(SOURCE_TYPE["hive"]["config"], "user"),
                          password=conf.get(SOURCE_TYPE["hive"]["config"], "password"),
                          default_db=conf.get(SOURCE_TYPE["hive"]["config"], "defaultDB"))
    beeline_db = BeelineNoSqlDB(port=conf.get(SOURCE_TYPE["hive"]["config"], "port"),
                                host=conf.get(SOURCE_TYPE["hive"]["config"], "host"),
                                user=conf.get(SOURCE_TYPE["hive"]["config"], "user"),
                                password=conf.get(SOURCE_TYPE["hive"]["config"], "password"),
                                metastore_uris=conf.get(SOURCE_TYPE["hive"]["config"], "beeline.metastore.uris"))

    presto = PrestoDB(port=conf.get(SOURCE_TYPE["presto"]["config"], "port"),
                      host=conf.get(SOURCE_TYPE["presto"]["config"], "host"),
                      user=conf.get(SOURCE_TYPE["presto"]["config"], "username"),
                      password=conf.get(SOURCE_TYPE["presto"]["config"], "password"),
                      timeout=conf.get(SOURCE_TYPE["presto"]["config"], "timeout"))

    task_module = get_task_module()
    ok = submit_excute_job()
    if not ok:
        msg = "|%s.%s任务处理失败" % (airflow.dag, airflow.task)
        #set_error_msg(msg)
        raise Exception("任务处理失败！")
    task_module.run()

    partition = ""
    if job_detail[13] == "D":
        # 目前只支持日分区
        partition = " where dw_snsh_dt = %d" % int(airflow.ds_nodash_utc8)
        ok = delete_old_partition()
    elif job_detail[13] == "PD":
        # 日分区永久保存   需求特殊(催收使用的用户观测点数据需要每日用永久保存)
        partition = " where dw_snsh_dt = %d" % int(airflow.ds_nodash_utc8)

    ok, cnt = check_table_data_count(hive_db, job_detail[3], job_detail[4] + " " + partition)
    if not ok or cnt == 0: # alter by wangsong，添加当分区条数为0时，程序直接退出
        msg = "|%s脚本sql处理失败" % (job_detail[4])
        raise Exception(msg)
        #set_error_msg(msg)
    '''    
    if ok and cnt == 0:
        warning = warning + "|%s.%s PARTITION(%d)导入数据为0" % (job_detail[3], job_detail[4], int(airflow.ds_nodash_utc8))
    '''
    end_time = time.time()
    run_seconds = int(end_time - start_time)
    par = "PARTITION(%d)" % int(airflow.ds_nodash_utc8) if job_detail[13] == "D" or job_detail[13] == "PD" else ""
    succ_msg = "[SUCC] Dag:" + airflow.dag + "Task:" + airflow.task + " run [" + str(run_seconds) + "s]|%s.%s %s[%d]"% (job_detail[3], job_detail[4], par, cnt)
    print(succ_msg)
    if not warning == "":
        warning = "[Warning]" + warning + "|run[" + str(run_seconds) + "s]"
        #push_qywx_msg(warning)