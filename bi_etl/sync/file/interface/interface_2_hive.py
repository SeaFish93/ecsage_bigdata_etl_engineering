# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: interface_2_hive.py
# @Software: PyCharm
# function info：用于同步接口数据到hive ods\snap\backtrace表


from ecsage_bigdata_etl_engineering.common.base.get_config import Conf
from ecsage_bigdata_etl_engineering.common.base.airflow_instance import Airflow
from ecsage_bigdata_etl_engineering.common.base.sync_method import get_mysql_hive_table_column
from ecsage_bigdata_etl_engineering.common.base.sync_method import get_create_mysql_table_columns
from ecsage_bigdata_etl_engineering.common.base.sync_method import get_table_columns_info
from ecsage_bigdata_etl_engineering.common.base.sync_method import set_sync_rows
from ecsage_bigdata_etl_engineering.common.base.sync_method import get_mysql_table_index
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_alert_info_d
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.config.column_type import MYSQL_2_HIVE
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread
from ecsage_bigdata_etl_engineering.common.operator.mysql.conn_mysql_metadb import EtlMetadata

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
    print(TaskInfo,"####################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    interface_url = TaskInfo[2]
    interface_level = TaskInfo[3]
    interface_time_line = TaskInfo[4]
    group_by = TaskInfo[5]
    is_run_date = TaskInfo[6]
    start_date = airflow.execution_date_utc8_str[0:10]
    end_date = airflow.execution_date_utc8_str[0:10]

    #分支执行
    if interface_level is None:
      print("!@@@@@@@@@@@@@@@@@@@@@@@@@@###########################################")

def get_level_time_line_date_group(InterfaceUrl="",InterfaceLevel="",InterfaceTimeLine=""):
    pass

