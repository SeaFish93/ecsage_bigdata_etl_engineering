# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: interface_2_hive.py
# @Software: PyCharm
# function info：用于同步接口数据到hive ods\snap\backtrace表


from ecsage_bigdata_etl_engineering.common.base.get_config import Conf
from ecsage_bigdata_etl_engineering.common.base.airflow_instance import Airflow
from ecsage_bigdata_etl_engineering.common.base.curl import exec_interface_data_curl
from ecsage_bigdata_etl_engineering.common.operator.mysql.conn_mysql_metadb import EtlMetadata
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit

import datetime
import math
import os
import time
import subprocess

conf = Conf().conf
etl_md = EtlMetadata()

#入口方法
def main(TaskInfo, Level,**kwargs):
    time.sleep(2)
    global airflow
    global developer
    airflow = Airflow(kwargs)
    print(TaskInfo,"####################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    interface_acount_type = TaskInfo[2]
    interface_url = TaskInfo[3]
    interface_level = TaskInfo[4]
    interface_time_line = TaskInfo[5]
    group_by = TaskInfo[6]
    is_run_date = TaskInfo[7]
    start_date = airflow.execution_date_utc8_str[0:10]
    end_date = airflow.execution_date_utc8_str[0:10]

    #分支执行
    if interface_acount_type is not None and interface_level is not None and interface_time_line is not None and group_by is not None and is_run_date == 1:
      get_level_time_line_date_group(StartDate=start_date,EndDate=end_date,InterfaceAcountType=interface_acount_type,
                                     InterfaceUrl=interface_url,InterfaceLevel=interface_level
                                     ,InterfaceTimeLine=interface_time_line)

#含有level、time_line、date、group接口
def get_level_time_line_date_group(StartDate="",EndDate="",InterfaceAcountType="",InterfaceUrl="",InterfaceLevel="",
                                   InterfaceTimeLine="",Group_Column=""):
    now_time = time.strftime("%H_%M_%S", time.localtime())
    data_dir = conf.get("Interface", "interface_data_home")
    file_name = "%s"%(data_dir) + "/" + airflow.ds_nodash_utc8 + "/%s/%s_%s_%s_%s"%(airflow.dag,airflow.task,InterfaceAcountType,EndDate,now_time)
    print(file_name,"===========================================")
    data = {"ec_fn":file_name,
            "mt":InterfaceAcountType,
            "level":["%s"%(InterfaceLevel)],
            "start_date":"%s"%(StartDate),
            "end_date":"%s"%(EndDate),
            "group_by":Group_Column.split(","),
            "time_line":"%s"%(InterfaceTimeLine)
           }
    exec_interface_data_curl(URL=InterfaceUrl,Data=data)
    #判断文件是否已生成
    sshpasswdy_home = conf.get("Interface", "sshpasswdy_home")
    check_script_home = conf.get("Interface", "check_script_home")
    ssh_host = conf.get("Interface", "ssh_host")
    sshpass_shell = """
     sshpass -f %s  ssh %s "sudo %s %s"
    """%(sshpasswdy_home,ssh_host,check_script_home,file_name)
    (ok, output) = subprocess.getstatusoutput(sshpass_shell)
    print("日志打印：", output)
    if ok != 0:
        set_exit(LevelStatu="red", MSG="接口检测文件出现异常！！！")
    #转换为json文件
    check_script_home = conf.get("Interface", "json_python_home")
    json_shell = """
      sshpass -f %s  ssh %s "sudo python %s %s"
    """%(sshpasswdy_home,ssh_host,check_script_home,file_name)
    (ok, output) = subprocess.getstatusoutput(json_shell)
    print("日志打印：", output)
    if ok != 0:
        set_exit(LevelStatu="red", MSG="接口转换为json文件出现异常！！！")
    #落地hdfs
    check_script_home = conf.get("Interface", "hdfs_client_home")
    hdfs_shell = """
     sshpass -f %s  ssh %s "sudo python %s %s.txt"
    """%(sshpasswdy_home,ssh_host,check_script_home,file_name)
    (ok, output) = subprocess.getstatusoutput(hdfs_shell)
    print("日志打印：", output)
    if ok != 0:
        set_exit(LevelStatu="red", MSG="接口转换为json文件出现异常！！！")
    #落地hive临时表
    #创建临时表
    #数据落地hive

