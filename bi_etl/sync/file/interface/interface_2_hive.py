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
from ecsage_bigdata_etl_engineering.common.base.sync_method import get_interface_2_hive_table_sql
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session

import datetime
import math
import os
import json
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
    interface_url = TaskInfo[2]
    source_db = TaskInfo[18]
    source_table = TaskInfo[19]
    target_db = TaskInfo[21]
    target_table = TaskInfo[22]
    hive_handler = TaskInfo[20]
    start_date_name = TaskInfo[11]
    end_date_name = TaskInfo[12]
    data_json = TaskInfo[3]
    data_json = json.dumps(data_json)
    partition_01 = TaskInfo[4]
    partition_02 = TaskInfo[5]
    partition_03 = TaskInfo[6]
    partition_04 = TaskInfo[7]
    partition_05 = TaskInfo[8]
    partition_06 = TaskInfo[9]
    partition_07 = TaskInfo[10]
    is_init_data = TaskInfo[15]
    file_dir_name = TaskInfo[24]
    print(file_dir_name,"==================")
    interface_module = TaskInfo[25]
    start_date = airflow.execution_date_utc8_str[0:10]
    end_date = airflow.execution_date_utc8_str[0:10]
    if is_init_data == 0:
      if start_date_name is not None and len(start_date_name)>0 and end_date_name is not None and len(end_date_name)>0:
         data_json["%s"%(start_date_name)] = start_date
         data_json["%s" % (end_date_name)] = end_date
    else:
        if start_date_name is not None and len(start_date_name) > 0 and end_date_name is not None and len(end_date_name) > 0:
           start_date = TaskInfo[11]
           end_date = TaskInfo[12]
           data_json["%s" % (start_date_name)] = start_date
           data_json["%s" % (end_date_name)] = end_date
    beeline_session = "" #set_db_session(SessionType="beeline", SessionHandler=hive_handler)
    hive_session = "" #set_db_session(SessionType="hive", SessionHandler=hive_handler)
    if Level == "file":
      #数据文件落地至临时表
      get_level_time_line_date_group(StartDate=start_date,EndDate=end_date,
                                      InterfaceUrl=interface_url,DataJson=data_json
                                      ,FileDirName = file_dir_name
                                      ,InterfaceModule = interface_module
                                      ,DB=target_db, Table=target_table
                                    )
    elif Level == "ods":
      exec_ods_hive_table(HiveSession=hive_session,BeelineSession=beeline_session,SourceDB=source_db,SourceTable=source_table,
                          TargetDB=target_db, TargetTable=target_table,ExecDate=end_date)
    elif Level == "snap":
      exec_snap_hive_table(HiveSession="", BeelineSession="", SourceDB="", SourceTable="",
                          TargetDB="", TargetTable="", ExecDate="")

#含有level、time_line、date、group接口
def get_level_time_line_date_group(StartDate="",EndDate="",
                                   InterfaceUrl="",DataJson=""
                                   ,FileDirName = ""
                                   ,InterfaceModule = ""
                                   ,DB="", Table=""
                                   ):
    data_json = json.dumps(DataJson)
    now_time = time.strftime("%H_%M_%S", time.localtime())
    data_dir = conf.get("Interface", InterfaceModule)
    file_name = "/%s_%s_%s_%s"%(airflow.dag,airflow.task,EndDate,now_time)
    file_dir_name = "%s"%(data_dir) + "/" + airflow.ds_nodash_utc8 + "/%s/%s"%(airflow.dag,file_name)
    data_json["%s"%(FileDirName)] = file_dir_name
    print("接口url："+InterfaceUrl)
    print("接口参数："+data_json)
    print("接口落地文件：" + file_dir_name)
    exec_interface_data_curl(URL=InterfaceUrl,Data=data_json)
    #处理落地文件及上传hdfs
    #exec_file(FileName=file_dir_name, params="accountId")
    #落地hive临时表
    #exec_file_2_hive_table(BeelineSession=BeelineSession, DB=DB, Table=Table,
    #                       FileName=file_name, InterfaceAcountType=InterfaceAcountType,
    #                       ExecDate=EndDate,ISDelete=ISDelete)

def exec_file(FileName="",params=""):
    # 判断文件是否已生成
    sshpasswdy_home = conf.get("Interface", "sshpasswdy_home")
    check_script_home = conf.get("Interface", "check_script_home")
    ssh_host = conf.get("Interface", "ssh_host")
    sshpass_shell = """
         sshpass -f %s  ssh %s "sudo %s %s"
        """ % (sshpasswdy_home, ssh_host, check_script_home, FileName)
    exec_shell(ShellCommand=sshpass_shell, MSG="接口检测文件出现异常！！！")
    # 转换为json文件
    if params is not None:
      check_script_home = conf.get("Interface", "json_params_python_home")
      json_shell = """
            sshpass -f %s  ssh %s "sudo python %s %s %s"
          """ % (sshpasswdy_home, ssh_host, check_script_home, FileName,params)
    else:
      check_script_home = conf.get("Interface", "json_python_home")
      json_shell = """                                                    
              sshpass -f %s  ssh %s "sudo python %s %s"           
         """ % (sshpasswdy_home, ssh_host, check_script_home, FileName)
    exec_shell(ShellCommand=json_shell, MSG="接口转换为json文件出现异常！！！")
    # 落地hdfs
    check_script_home = conf.get("Interface", "hdfs_client_home")
    hdfs_shell = """
         sshpass -f %s  ssh %s "sudo python %s %s.txt"
        """ % (sshpasswdy_home, ssh_host, check_script_home, FileName)
    exec_shell(ShellCommand=hdfs_shell, MSG="接口上传hdfs文件出现异常！！！")

def exec_shell(ShellCommand="",MSG=""):
    (ok, output) = subprocess.getstatusoutput(ShellCommand)
    print("日志打印：", output)
    if ok != 0:
        set_exit(LevelStatu="red", MSG=MSG)

#落地至临时表
def exec_file_2_hive_table(BeelineSession="",DB="",Table="",FileName="",InterfaceAcountType="",ExecDate="",ISDelete=""):
    inpath = "%s%s.txt" % ("/tmp/sync", FileName)
    sql,load_sql = get_interface_2_hive_table_sql(DB=DB,Table=Table,InterfaceAcountType=InterfaceAcountType,ISDelete=ISDelete,HDFSDir=inpath,ExecDate=ExecDate)
    BeelineSession.execute_sql(sql)
    print(inpath,"========================@@@@@@@@@@@@@@@@@@@@@")
    ok = BeelineSession.execute_sql(load_sql)
    if ok is False:
        set_exit(LevelStatu="red", MSG="接口写入hive表【%s.%s】出现异常"%(DB,Table))

#落地至ods
def exec_ods_hive_table(HiveSession="",BeelineSession="",SourceDB="",SourceTable="",
                        TargetDB="", TargetTable="",ExecDate=""):
   get_ods_column = HiveSession.get_column_info(TargetDB,TargetTable)
   print(get_ods_column)

#落地至snap
def exec_snap_hive_table(HiveSession="",BeelineSession="",SourceDB="",SourceTable="",
                        TargetDB="", TargetTable="",ExecDate=""):
   get_ods_column = HiveSession.get_column_info(TargetDB,TargetTable)
   print(get_ods_column,"@@###########################################")

