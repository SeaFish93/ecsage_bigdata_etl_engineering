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
    shell_name = TaskInfo[2]
    shell_path = TaskInfo[3]
    params = TaskInfo[4]
    start_date = airflow.execution_date_utc8_str[0:10]
    end_date = airflow.execution_date_utc8_str[0:10]
    print(params,"=============================")
    interval = int(params.split(',')[0])
    action = int(params.split(',')[1])

    ok = os.system("sh  %s/%s.sh %s %s %s %s") % (shell_path, shell_name,start_date,end_date,interval,action)
    if ok != 0:
      pass
