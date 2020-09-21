# -*- coding: utf-8 -*-
# @Time    : 2019/1/23 19:30
# @Author  : luoyh
# @FileName: exec_script_sql_hour.py
# @Software: PyCharm
#20190917 修改日期分区日期格式 by wangsong
#20190918 修改添加保存成功日志 by wangsong
#20190918 修改添加对写入日志表捕获异常 by wangsong
#20190919 修改写入日志中的msg，对其含有英文引号进行替换为中文 by wangsong
#20190919 修改获取模块文件传参 by wangsong

import time
import datetime
import importlib
import pendulum
from akulaku_etl.common.get_config import Conf
from akulaku_etl.config.source_type import SOURCE_TYPE
from akulaku_etl.common.airflow_instance import Airflow
from akulaku_etl.common.hive_operator import HiveNoSqlDB
from akulaku_etl.common.spark_operator import SparkNoSqlDB
from akulaku_etl.common.beeline_operator import BeelineNoSqlDB
from akulaku_etl.common.presto_operator import PrestoDB
from akulaku_etl.common.check_result import check_table_data_count
from akulaku_etl.common.send_msg import (push_qywx_msg, set_error_msg)
from akulaku_etl.common.get_table_dependent import get_down_table

conf = Conf().conf

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
        set_error_msg(msg)
        finally_del()
        exit(1)


def get_task_module():
    try:
        pkg = ".bi_etl.%s.%s" % (job_detail[1], job_detail[4])
        module = importlib.import_module(pkg, package="akulaku_etl")
        return module
    except Exception as e:
        msg = "|**** Error: 获取SQL文件失败 %s" % e
        set_error_msg(msg)
        try:
          # 保存落地日志
          dep_tables, dep_id_info, dep_developer, developer = get_down_table(DagId=dag_id, TaskId=task_id)
          sql = """
              insert into etl_mid.etl_warning_result_detail
              partition(task_instance='%s',error_type='%s',stats_dt='%s')
              select '%s' as db_name,'%s' as table_name,'%s' as is_sync,null as sync_source_rows,null as sync_target_rows,
                      '%s' as down_table,'%s' as developer,'%s' as down_developer,'%s' as msg,'%s' as create_time
          """ % ("%s.%s" % (dag_id, task_id), "error", exec_date, db_name, table_name, "N", dep_tables, developer, dep_developer,msg.replace("""'""","""‘"""), sys_time)
          state = hive_db.execute_sql(sql=sql)
          if state is False:
              raise Exception("exec sql is not succ.")
        except Exception as e:
          exception_msg = """【Error】<TaskInstance：%s.%s>运行周期北京时间：%s，保存落地【error】日志失败！！！"""%(dag_id,task_id,exec_date)
          print(exception_msg)
          print(e)
          push_qywx_msg(exception_msg,"jaywang")
        finally_del()
        exit(1)
    return None


def replace_placeholder(txt):
    tz = "Asia/Shanghai"
    dt = airflow.dt
    trx_yr = dt.year
    trx_month = "%s" % (str(dt.month) if dt.month >= 10 else "0%d" % dt.month)
    trx_day = "%s" % (str(dt.day) if dt.day >= 10 else "0%d" % dt.day)

    # yyyymmdd
    trx_dt = airflow.ds_nodash_utc8
    trx_first_day_of_month = pendulum.datetime(dt.year, dt.month, 1, 0, 0, 0, 0, tz).to_date_string().replace("-", "")
    trx_last_day_of_month = pendulum.datetime(dt.year, dt.month, 1, 0, 0, 0, 0, tz) \
        .add(months=1).subtract(days=1).to_date_string().replace("-", "")

    # yyyy-mm-dd
    yesterday = airflow.yesterday_ds_nodash_utc8
    trx_yesterday_date = airflow.dt.subtract(days=1).to_date_string()
    trx_date = airflow.dt.to_date_string()

    # timestamp
    trx_hour = pendulum.datetime(dt.year, dt.month, dt.day, dt.hour, 0, 0, 0, tz)
    # trx_before_hour = trx_hour.add(hours=-1)
    trx_after_hour = trx_hour.add(hours=1)
    # trx_star_timestamp = int(trx_before_hour.timestamp() * 1000)
    # trx_end_timestamp = int(trx_hour.timestamp() * 1000)
    trx_star_timestamp = int(trx_hour.timestamp() * 1000)
    trx_end_timestamp = int(trx_after_hour.timestamp() * 1000)
    if trx_after_hour.hour == 0:
        # 凌晨0点，需要跑昨天一整天的数据
        dt = trx_after_hour.add(days=-1)

        trx_yr = dt.year
        trx_month = "%s" % (str(dt.month) if dt.month >= 10 else "0%d" % dt.month)
        trx_day = "%s" % (str(dt.day) if dt.day >= 10 else "0%d" % dt.day)

        trx_star_timestamp = int(dt.timestamp() * 1000)
        trx_end_timestamp = int(trx_after_hour.timestamp() * 1000)

    return txt.replace("${trx_dt}", str(trx_dt)) \
        .replace("${trx_yr}", str(trx_yr)) \
        .replace("${trx_month}", str(trx_month)) \
        .replace("${trx_day}", str(trx_day)) \
        .replace("${trx_star_timestamp}", str(trx_star_timestamp)) \
        .replace("${trx_end_timestamp}", str(trx_end_timestamp)) \
        .replace("${trx_date}", str(trx_date))

def submit_excute_job():
    global hive_db
    global beeline_db
    global presto
    sql_list = task_module.SQL_LIST
    ok = True
    # worker = job_detail[17]
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
            spark_db = SparkNoSqlDB(port=conf.get(SOURCE_TYPE["spark"]["config"], "port"),
                                    host=conf.get(SOURCE_TYPE["spark"]["config"], "host"),
                                    user=conf.get(SOURCE_TYPE["spark"]["config"], "user"),
                                    metastore_uris=conf.get(SOURCE_TYPE["spark"]["config"], "hive.metastore.uris"),
                                    app_name=airflow.dag + "." + airflow.task)
            ok = spark_db.execute_sql(sql_str)
        if not ok:
            return ok
    return ok


def delete_old_partition():
    old_partition = airflow.dt.subtract(days=3).to_date_string().replace("-", "")
    del_table_partition = "%s.%s"
    # 约定所有日分区字段名都为dw_snsh_dt
    drop_sql = "alter table %s.%s drop if exists partition (%s=%s)" % (job_detail[3], job_detail[4], "dw_snsh_dt", old_partition)
    return hive_db.execute_sql(drop_sql)


def run_hour(jd, **kwargs):
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
    airflow = Airflow(kwargs)
    job_detail = jd
    db_name = job_detail[1]
    table_name = job_detail[4]
    dag_id = airflow.dag
    task_id = airflow.task
    exec_date = exec_date = airflow.ds_nodash_utc8
    sys_time = (datetime.datetime.now() + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    warning = ""

    # 打印airflow task信息到日志
    start_time = time.time()
    execution_date = airflow.execution_date_utc8_str
    print(kwargs)
    print("execution_date=%s" % execution_date)

    hive_db = HiveNoSqlDB(port=conf.get(SOURCE_TYPE["hive"]["config"], "port"),
                          host=conf.get(SOURCE_TYPE["hive"]["config"], "host"),
                          user=conf.get(SOURCE_TYPE["hive"]["config"], "user"),
                          password=conf.get(SOURCE_TYPE["hive"]["config"], "password"),
                          default_db=conf.get(SOURCE_TYPE["hive"]["config"], "defaultDB"))
    presto = PrestoDB(port=conf.get(SOURCE_TYPE["presto"]["config"], "port"),
                      host=conf.get(SOURCE_TYPE["presto"]["config"], "host"),
                      user=conf.get(SOURCE_TYPE["presto"]["config"], "username"),
                      password=conf.get(SOURCE_TYPE["presto"]["config"], "password"),
                      timeout=conf.get(SOURCE_TYPE["presto"]["config"], "timeout"))
    beeline_db = BeelineNoSqlDB(port=conf.get(SOURCE_TYPE["hive"]["config"], "port"),
                                host=conf.get(SOURCE_TYPE["hive"]["config"], "host"),
                                user=conf.get(SOURCE_TYPE["hive"]["config"], "user"),
                                password=conf.get(SOURCE_TYPE["hive"]["config"], "password"),
                                metastore_uris=conf.get(SOURCE_TYPE["hive"]["config"], "beeline.metastore.uris"))
    task_module = get_task_module()
    ok = submit_excute_job()
    if not ok:
        msg = "|%s脚本sql处理失败" % (job_detail[4])
        set_error_msg(msg)
        try:
           # 保存落地日志
           dep_tables, dep_id_info, dep_developer, developer = get_down_table(DagId=dag_id, TaskId=task_id)
           sql = """
                insert into etl_mid.etl_warning_result_detail
                partition(task_instance='%s',error_type='%s',stats_dt='%s')
                select '%s' as db_name,'%s' as table_name,'%s' as is_sync,null as sync_source_rows,null as sync_target_rows,
                        '%s' as down_table,'%s' as developer,'%s' as down_developer,'%s' as msg,'%s' as create_time
           """ % ("%s.%s" % (dag_id, task_id), "error", exec_date, db_name, table_name, "N", dep_tables, developer, dep_developer,msg.replace("""'""","""‘"""), sys_time)
           state = hive_db.execute_sql(sql=sql)
           if state is False:
               raise Exception("exec sql is not succ.")
        except Exception as e:
          exception_msg = """【Error】<TaskInstance：%s.%s>运行周期北京时间：%s，保存落地【error】日志失败！！！"""%(dag_id,task_id,exec_date)
          print(exception_msg)
          print(e)
          push_qywx_msg(exception_msg,"jaywang")
        finally_del()
        exit(1)
    task_module.run()

    partition = ""
    if job_detail[13] == "D":
        # 目前只支持日分区
        partition = " where dw_snsh_dt = %d" % int(airflow.ds_nodash_utc8)
        ok = delete_old_partition()

    ok, cnt = check_table_data_count(hive_db, job_detail[3], job_detail[4] + " " + partition)
    if not ok:
        msg = "|%s脚本sql处理失败" % (job_detail[4])
        set_error_msg(msg)
        try:
          # 保存落地日志
          dep_tables, dep_id_info, dep_developer, developer = get_down_table(DagId=dag_id, TaskId=task_id)
          sql = """
              insert into etl_mid.etl_warning_result_detail
              partition(task_instance='%s',error_type='%s',stats_dt='%s')
              select '%s' as db_name,'%s' as table_name,'%s' as is_sync,null as sync_source_rows,null as sync_target_rows,
                      '%s' as down_table,'%s' as developer,'%s' as down_developer,'%s' as msg,'%s' as create_time
          """ % ("%s.%s" % (dag_id, task_id), "error", exec_date, db_name, table_name, "N", dep_tables, developer, dep_developer,msg.replace("""'""","""‘"""), sys_time)
          state = hive_db.execute_sql(sql=sql)
          if state is False:
              raise Exception("exec sql is not succ.")
        except Exception as e:
          exception_msg = """【Error】<TaskInstance：%s.%s>运行周期北京时间：%s，保存落地【error】日志失败！！！"""%(dag_id,task_id,exec_date)
          print(exception_msg)
          print(e)
          push_qywx_msg(exception_msg,"jaywang")
        finally_del()
        exit(1)
    if ok and cnt == 0:
        warning = warning + "|%s.%s PARTITION(%d)导入数据为0" % (job_detail[3], job_detail[4], int(airflow.ds_nodash_utc8))

    end_time = time.time()
    run_seconds = int(end_time - start_time)
    par = "PARTITION(%d)" % int(airflow.ds_nodash_utc8) if job_detail[13] == "D" else ""
    succ_msg = "[SUCC] Dag:" + airflow.dag + "Task:" + airflow.task + " run [" + str(run_seconds) + "s]|%s.%s %s[%d]"% (job_detail[3], job_detail[4], par, cnt)
    print(succ_msg)
    if not warning == "":
        warning = "[Warning]" + warning + "|run[" + str(run_seconds) + "s]"
        push_qywx_msg(warning)
        try:
          # 保存落地日志
          dep_tables, dep_id_info, dep_developer, developer = get_down_table(DagId=dag_id, TaskId=task_id)
          sql = """
               insert into etl_mid.etl_warning_result_detail
               partition(task_instance='%s',error_type='%s',stats_dt='%s')
               select '%s' as db_name,'%s' as table_name,'%s' as is_sync,null as sync_source_rows,null as sync_target_rows,
                       '%s' as down_table,'%s' as developer,'%s' as down_developer,'%s' as msg,'%s' as create_time
          """ % ("%s.%s" % (dag_id, task_id), "warning", exec_date, db_name, table_name, "N", dep_tables, developer, dep_developer,warning.replace("""'""","""‘"""), sys_time)
          state = hive_db.execute_sql(sql=sql)
          if state is False:
              raise Exception("exec sql is not succ.")
        except Exception as e:
          exception_msg = """【Error】<TaskInstance：%s.%s>运行周期北京时间：%s，保存落地【warning】日志失败！！！"""%(dag_id,task_id,exec_date)
          print(exception_msg)
          print(e)
          push_qywx_msg(exception_msg,"jaywang")
    else:
        try:
          # 保存落地日志
          dep_tables, dep_id_info, dep_developer, developer = get_down_table(DagId=dag_id, TaskId=task_id)
          sql = """
                insert into etl_mid.etl_warning_result_detail
                partition(task_instance='%s',error_type='%s',stats_dt='%s')
                select '%s' as db_name,'%s' as table_name,'%s' as is_sync,null as sync_source_rows,null as sync_target_rows,
                        '%s' as down_table,'%s' as developer,'%s' as down_developer,'%s' as msg,'%s' as create_time
          """ % ("%s.%s" % (dag_id, task_id), "succ", exec_date, db_name, table_name, "N", dep_tables, developer,dep_developer, succ_msg.replace("""'""","""‘"""), sys_time)
          state = hive_db.execute_sql(sql=sql)
          if state is False:
              raise Exception("exec sql is not succ.")
        except Exception as e:
          exception_msg = """【Error】<TaskInstance：%s.%s>运行周期北京时间：%s，保存落地【succ】日志失败！！！"""%(dag_id,task_id,exec_date)
          print(exception_msg)
          print(e)
          push_qywx_msg(exception_msg,"jaywang")
