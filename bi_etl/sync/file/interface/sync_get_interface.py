# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: interface_2_hive.py
# @Software: PyCharm
# function info：用于同步接口数据到hive ods\snap\backtrace表


from ecsage_bigdata_etl_engineering.common.base.get_config import Conf
from ecsage_bigdata_etl_engineering.common.base.airflow_instance import Airflow
from ecsage_bigdata_etl_engineering.common.operator.mysql.conn_mysql_metadb import EtlMetadata
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_create_dag_alert
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit

import subprocess
import os
from airflow.models import Variable

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
    start_date = Variable.get("%s_start_date"%(airflow.dag), default_var=airflow.tomorrow_ds_nodash_utc8)
    end_date = Variable.get("%s_end_date"%(airflow.dag), default_var=airflow.tomorrow_ds_nodash_utc8)
    interval = int(str(params.split(',')[0]))
    action = int(str(params.split(',')[1]))
    print(Variable.get("baz", default_var=None),"==============================================================")
    print(start_date,end_date,"----------------------------")
    print(interval, "=============================")
    #ok = os.system("sh  %s/%s %s %s %s %s" % (shell_path, shell_name+".sh",start_date,end_date,interval,action))
    #(ok, output) = subprocess.getstatusoutput("sh  %s/%s %s %s %s %s" % (shell_path, shell_name+".sh",start_date,end_date,interval,action))
    #print("日志打印：",output)
    #if ok != 0:
    #    msg = get_create_dag_alert(FileName="%s" % (os.path.basename(__file__)), Log="执行接口出现异常！！！",
    #                               Developer="蒋杰")
    #    set_exit(LevelStatu="red", MSG=msg)
