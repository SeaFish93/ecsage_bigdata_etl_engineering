# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: interface_2_hive.py
# @Software: PyChar
# function info：用于同步接口数据到hive ods\snap\backtrace表


from ecsage_bigdata_etl_engineering.common.base.get_config import Conf
from ecsage_bigdata_etl_engineering.common.base.airflow_instance import Airflow
from ecsage_bigdata_etl_engineering.common.base.curl import exec_interface_data_curl
from ecsage_bigdata_etl_engineering.common.operator.mysql.conn_mysql_metadb import EtlMetadata
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_create_dag_alert
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_local_hdfs_thread
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_data_2_ods
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_data_2_snap
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_alert_info_d
from ecsage_bigdata_etl_engineering.common.base.def_table_struct import def_ods_structure
from ecsage_bigdata_etl_engineering.common.base.def_table_struct import adj_snap_structure
from ecsage_bigdata_etl_engineering.common.base.def_table_struct import analysis_etlmid_cloumns

import datetime
import math
import os
import json
import ast
import time
import subprocess

conf = Conf().conf
etl_md = EtlMetadata()

#入口方法
def main(TaskInfo, Level,**kwargs):
    time.sleep(2)
    global airflow
    global developer
    global regexp_extract_column
    airflow = Airflow(kwargs)
    print(TaskInfo,"####################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    interface_url = TaskInfo[2]
    source_db = TaskInfo[18]
    source_table = TaskInfo[19]
    target_db = TaskInfo[21]
    target_table = TaskInfo[22]
    hive_handler = TaskInfo[20]
    beeline_handler = "beeline"
    start_date_name = TaskInfo[11]
    end_date_name = TaskInfo[12]
    data_json = TaskInfo[3]
    data_json_request = data_json
    data_json = json.dumps(data_json)
    file_dir_name = TaskInfo[24]
    interface_module = TaskInfo[25]
    filter_modify_time_name = TaskInfo[26]
    select_exclude_columns = TaskInfo[27]
    is_report = TaskInfo[28]
    key_columns = TaskInfo[29]
    commit_num = TaskInfo[31]
    exclude_account_id = TaskInfo[34]
    array_flag = TaskInfo[35]
    custom_set_parameter =TaskInfo[36]

    #regexp_extract_column = TaskInfo[30]
    #if regexp_extract_column is None or len(regexp_extract_column) == 0:
    #    regexp_extract_column = """get_json_object(get_json_object(regexp_extract(a.request_data,'(\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\".*)',1),'$.data'),'$.list')"""
    exec_date = airflow.execution_date_utc8_str[0:10]
    #exec_date="2020-11-02"
    hive_session = set_db_session(SessionType="hive", SessionHandler=hive_handler)
    beeline_session = set_db_session(SessionType="beeline", SessionHandler=beeline_handler)
    if Level == "file":
      data_json = ast.literal_eval(json.loads(data_json))
      if filter_modify_time_name is not None and len(filter_modify_time_name) > 0:
          data_json["filtering"]["%s" % (filter_modify_time_name)] = exec_date
      if start_date_name is not None and len(start_date_name) > 0 and end_date_name is not None and len(end_date_name) > 0:
          data_json["%s" % (start_date_name)] = exec_date
          data_json["%s" % (end_date_name)] = exec_date
      #数据文件落地至临时表
      get_file_2_hive(HiveSession=hive_session,BeelineSession=beeline_session,InterfaceUrl=interface_url,DataJson=data_json
                      ,FileDirName = file_dir_name,DataJsonRequest=data_json_request
                      ,InterfaceModule = interface_module,CommitNum=commit_num
                      ,DB=target_db, Table=target_table,ExecDate=exec_date,IsReport=is_report
                      ,Exclude_Account_id=exclude_account_id
                      ,CustomSetParameter=custom_set_parameter
                     )
    elif Level == "ods":
      get_data_2_ods(HiveSession=hive_session,BeelineSession=beeline_session,SourceDB=source_db,SourceTable=source_table,
                          TargetDB=target_db, TargetTable=target_table,IsReport=is_report,SelectExcludeColumns=select_exclude_columns,KeyColumns=key_columns,ExecDate=exec_date
                          ,ArrayFlag=array_flag,CustomSetParameter=custom_set_parameter
                          ,DagId=airflow.dag,TaskId=airflow.task)
    elif Level == "snap":
      get_data_2_snap(HiveSession=hive_session, BeelineSession=beeline_session, SourceDB=source_db, SourceTable=source_table,
                             TargetDB=target_db, TargetTable=target_table, IsReport=is_report
                            ,KeyColumns=key_columns, ExecDate=exec_date,CustomSetParameter=custom_set_parameter
                            ,DagId=airflow.dag,TaskId=airflow.task)

#含有level、time_line、date、group接口
def get_file_2_hive(HiveSession="",BeelineSession="",InterfaceUrl="",DataJson={},DataJsonRequest=""
                                   ,FileDirName = ""
                                   ,InterfaceModule = "",CommitNum=""
                                   ,DB="", Table="",ExecDate="",IsReport=""
                                   ,Exclude_Account_id=""
                                   ,CustomSetParameter=""
                                   ):
    data_json = DataJson
    etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
    mysql_session = set_db_session(SessionType="mysql", SessionHandler="mysql_media")
    request_type = data_json["mt"]
    if "filtering" in data_json.keys():
       if "status" in data_json["filtering"].keys():
           delete_type = data_json["filtering"]["status"] #"delete"
       else:
           delete_type = "not_delete"
    else:
       delete_type = "not_delete"
    if Exclude_Account_id is not None and len(Exclude_Account_id) > 0:
        exclude_account_id="""a.account_id not in (%s)"""%(Exclude_Account_id)
    else:
        exclude_account_id="""1=1"""
    print("开始执行调用接口")
    data_dir = conf.get("Interface", InterfaceModule)
    file_dir = "%s" % (data_dir) + "/" + airflow.ds_nodash_utc8 + "/%s/%s" % (airflow.dag,request_type)
    ########set_filebeat_yml(DataFile=file_dir, FileName="%s_%s_%s.log" % (airflow.dag, airflow.task,ExecData), ExecDate=ExecData)
    #数据文件绝对路径
    file_dir_name_list = []
    #获取请求域名个数
    host_count_sql = """
         select count(1)
         from metadb.request_hostname_interface a
    """
    ok, host_count = etl_md.get_all_rows(host_count_sql)
    if IsReport == 1:
       #获取每个域名分配的子账户个数
       media_type = """media_type"""
       where = """media_type"""
       table = """
          (select a.account_id, a.media_type, a.service_code 
           from metadb.oe_account_interface a
           where a.exec_date = '%s' and %s
          )
       """%(ExecDate,exclude_account_id)
       group_by = """media_type"""
       select_session = etl_md
    else:
       media_type = """media as media_type"""
       where = """media"""
       table = """big_data_mdg.media_advertiser"""
       group_by = """media"""
       select_session = mysql_session
    account_num_sql = """
           select count(1),min(rn),max(rn) 
           from(select account_id, media_type, service_code,@row_num:=@row_num+1 as rn
                from (select account_id, %s, service_code
                      from %s a
                      where %s = %s
                      group by account_id, %s, service_code
                      ) tmp,(select @row_num:=0) r
                ) tmp1
       """%(media_type,table,where,int(data_json["mt"]),group_by)
    ok, account_num = select_session.get_all_rows(account_num_sql)
    account_avg = account_num[0][0] / host_count[0][0]
    if account_avg > 0:
        account_avg = account_avg + 1
        account_run_num = 1
        host = ""
        for n in range(1, host_count[0][0]+1):
           #获取请求hostname
           host_sql = """
              select hostname
              from(select hostname,@row_num:=@row_num+1 as rn
                   from metadb.request_hostname_interface a,(select @row_num:=0) r
                  ) tmp
              where rn = %s
           """%(n)
           ok, hosts = etl_md.get_all_rows(host_sql)
           host = hosts[0][0]
           if IsReport == 1:
               # 获取每个域名分配的子账户个数
               media_type = """media_type"""
               where = """media_type"""
               table = """
                  (select a.account_id, a.media_type, a.service_code 
                   from metadb.oe_account_interface a
                   where a.exec_date = '%s' and %s
                   )
               """%(ExecDate,exclude_account_id)
               select_session = etl_md
               group_by = """media_type"""
           else:
               media_type = """media as media_type"""
               where = """media"""
               table = """big_data_mdg.media_advertiser"""
               select_session = mysql_session
               group_by = """media"""

           #请求子账户
           account_sql = """
                  select account_id, media_type, service_code
                  from(select account_id, media_type, service_code,@row_num:=@row_num+1 as rn
                       from (select account_id, %s, service_code
                             from %s a
                             where %s = %s
                             group by account_id, %s, service_code
                            ) tmp,(select @row_num:=0) r
                       ) tmp1
                  where rn >= %s and rn <= %s
               """ % (media_type,table,where,int(data_json["mt"]),group_by,int(account_run_num), int(account_avg))
           ok, accounts_list = select_session.get_all_rows(account_sql)
           #data_json["ec_tn"]=airflow.task
           #data_json["ec_src"]="OE" if InterfaceModule == "oceanengine" else "TC"
           #提交请求
           interface_url = """http://%s%s"""%(host,InterfaceUrl)
           file = request_commit_account(AccountData=accounts_list, Num=n, InterfaceUrl=interface_url, ExecDate=ExecDate, FileDir=file_dir,
                                  FileDirName=FileDirName,DataJson=data_json)
           file_dir_name_list.append(file)
           #提交次数
           account_run_num = account_avg + 1
           account_avg = account_run_num + account_avg

    ######### for data in data_list:
    #########    request_params.append(data)
    #########    advertiser_list.append({"serviceCode":str(data[2]),"accountId":str(data[0])})
    #########    if num == CommitNum or nums == len(data_list):
    #########       run_num = run_num + 1
    #########       print("第%s批正在提交！%s"%(run_num,advertiser_list))
    #########       now_time = time.strftime("%H_%M_%S", time.localtime())
    #########       file_name = "%s_%s_%s_%s_%s.log" % (airflow.dag, airflow.task,run_num,ExecDate, now_time)
    #########       file_dir_name = "%s/%s" % (file_dir, file_name)
    #########       if os.path.exists(file_dir) is False:
    #########           os.system("mkdir -p %s" % (file_dir))
    #########       data_json["%s" % (FileDirName)] = file_dir_name
    #########       data_json["advertiser_list"] = advertiser_list
    #########       file_dir_name_list.append((file_dir_name, data_json))
    #########       print("请求接口URL：%s"%(InterfaceUrl))
    #########       print("请求接口参数：%s"%(data_json))
    #########       # 分子账户开启进程
    #########       exec_interface_data_curl(URL=InterfaceUrl, Data=data_json, File=file_dir_name,DataJsonRequest=DataJsonRequest)
    #########       #time.sleep(10)
    #########       num = 0
    #########       advertiser_list = []
    #########    num = num + 1
    #########    nums = nums + 1
    print("结束执行调用接口，进行等待MD5文件生成")
    wait_for_md5(FileDirNameList=file_dir_name_list, DB=DB, Table=Table, ExecDate=ExecDate)
    #### md5_file_false = []
    #### set_md5_file_true = True
    #### sleep_num = 1
    #### print(file_dir_name_list,"========================================================")
    #### # 判断是否没有md5的文件
    #### while set_md5_file_true:
    ####   for file in file_dir_name_list:
    ####     #判断md5文件是否存在
    ####     is_md5_file = os.path.exists(file[0]+".md5")
    ####     if is_md5_file is False:
    ####        md5_file_false.append(file[0])
    ####     else:
    ####        pass
    ####   if len(md5_file_false) > 0:
    ####       wait_mins = 600
    ####       if sleep_num <= wait_mins:
    ####           min = 60
    ####           print("等待第%s次%s秒"%(sleep_num,min))
    ####           time.sleep(min)
    ####       else:
    ####           msg = "等待md5生成超时！！！\n%s" % (md5_file_false)
    ####           msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
    ####                                  SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
    ####                                  TargetTable="%s.%s" % (DB, Table),
    ####                                  BeginExecDate=ExecData,
    ####                                  EndExecDate=ExecData,
    ####                                  Status="Error",
    ####                                  Log=msg,
    ####                                  Developer="developer")
    ####           set_exit(LevelStatu="red", MSG=msg)
    ####   else:
    ####       set_md5_file_true = False
    ####   md5_file_false.clear()
    ####   sleep_num = sleep_num + 1
    #落地临时表
    exec_file_2_hive(HiveSession=HiveSession,BeelineSession=BeelineSession,LocalFileName=file_dir_name_list,RequestType=request_type,DeleteType=delete_type,DB=DB,Table=Table,ExecDate=ExecDate,CustomSetParameter=CustomSetParameter,EtlMdSession=etl_md)

def exec_file_2_hive(HiveSession="",BeelineSession="",LocalFileName="",RequestType="",DeleteType="",DB="",Table="",ExecDate="",CustomSetParameter="",EtlMdSession=""):
    mid_table = """%s.%s""" % (DB, Table)
    # 创建data临时表
    mid_sql = """
              create table if not exists %s
              (
               request_data string
              )partitioned by(etl_date string,request_type string,delete_type string)
              row format delimited fields terminated by '\\001' 
            """%(mid_table)
    HiveSession.execute_sql(mid_sql)
    load_num = 0
    hdfs_dir = conf.get("Airflow_New", "hdfs_home") #"/tmp/datafolder_new"
    load_table_sqls = ""
    load_table_sql_0 = ""
    load_table_sql = ""
    for data in LocalFileName:
        local_file = """%s""" % (data)
       # 落地mid表
        if load_num == 0:
           load_table_sql_0 = """
                load data inpath '{hdfs_dir}/{file_name}' OVERWRITE  INTO TABLE {table_name}
                partition(etl_date='{exec_date}',request_type='{request_type}',delete_type='{delete_type}')
                ;\n
            """.format(hdfs_dir=hdfs_dir, file_name=local_file.split("/")[-1], table_name=mid_table,exec_date=ExecDate,request_type=RequestType,delete_type=DeleteType)
        else:
           load_table_sql = """
                load data inpath '{hdfs_dir}/{file_name}' INTO TABLE {table_name}
                partition(etl_date='{exec_date}',request_type='{request_type}',delete_type='{delete_type}')
                ;\n
            """.format(hdfs_dir=hdfs_dir, file_name=local_file.split("/")[-1], table_name=mid_table,exec_date=ExecDate, request_type=RequestType,delete_type=DeleteType)
        load_table_sqls = load_table_sql + load_table_sqls
        load_num = load_num + 1
    load_table_sqls = load_table_sql_0 + load_table_sqls
    if len(load_table_sqls) == 0:
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (DB, Table),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="API采集没执行！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)
    #上传hdfs
    get_local_hdfs_thread(TargetDb=DB, TargetTable=Table, ExecDate=ExecDate, DataFileList=LocalFileName,HDFSDir=hdfs_dir,EtlMdSession=EtlMdSession)
    #落地至hive
    ok_data = BeelineSession.execute_sql(load_table_sqls,CustomSetParameter)
    if ok_data is False:
       # 删除临时表
       HiveSession.execute_sql("""drop table if exists %s""" % (mid_table))
       msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                              SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                              TargetTable="%s.%s" % (DB, Table),
                              BeginExecDate=ExecDate,
                              EndExecDate=ExecDate,
                              Status="Error",
                              Log="HDFS数据文件load入仓临时表出现异常！！！",
                              Developer="developer")
       set_exit(LevelStatu="red", MSG=msg)




def is_key_columns(SourceDB="",SourceTable="",TargetDB="",TargetTable="",ExecDate="",KeyColumns=""):
    if KeyColumns is None or len(KeyColumns) == 0:
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % (SourceDB, SourceTable),
                               TargetTable="%s.%s" % (TargetDB, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="请确认配置表指定主键字段是否正确！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)

def set_filebeat_yml(DataFile="",FileName="",ExecDate=""):
    #设置数据采集
    tmplate_yml_file = """/root/bigdata_item_code/ecsage_bigdata_etl_engineering/config/template_filebeat.yml"""
    tmplate_yml_shell = """mkdir -p /tmp/tmplate_yml"""
    yml_file = """/tmp/tmplate_yml/%s.yml"""%(FileName)
    os.system(tmplate_yml_shell)
    cp_yml = """cp /root/bigdata_item_code/ecsage_bigdata_etl_engineering/config/template_filebeat.yml %s"""%(yml_file)
    os.system(cp_yml)
    filebeat_name = "%s_%s*_%s*.log" % (airflow.dag, airflow.task, ExecDate)
    filebeat_name = "%s/%s" % (DataFile, filebeat_name)
    sed_cat = """echo '%s'|sed 's/\//\\\\\//g'"""%(filebeat_name)
    sed_cat = os.popen(sed_cat)
    get_sed_cat = sed_cat.read().split()[0]
    sed_file_dir = """ sed -i "s/##file_dir##/%s/g" %s"""%(get_sed_cat,yml_file)
    os.system(sed_file_dir)
    sed_file_dir = """sed -i "s/##file_name##/testfile/g" %s """%(yml_file)
    os.system(sed_file_dir)
    scp = """ scp %s root@bd127-node:/etc/filebeat/inputs.d/ """%(yml_file)
    os.system(scp)

#等待md5
def wait_for_md5(FileDirNameList="",DB="", Table="",ExecDate=""):
    md5_file_false = []
    set_md5_file_true = True
    sleep_num = 1
    # 判断是否没有md5的文件
    while set_md5_file_true:
      for file in FileDirNameList:
        #判断md5文件是否存在
        is_md5_file = os.path.exists(file+".md5")
        if is_md5_file is False:
           md5_file_false.append(file)
        else:
           file_md5_length = os.popen("cat %s|wc -L"%(file+".md5"))
           file_length = file_md5_length.read().split()[0]
           print(file,".md5",file_length,"==============================================")
           if int(file_length) == 0:
               msg = "生成为空异常md5文件！！！\n%s" % (file+".md5")
               msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                                      SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                                      TargetTable="%s.%s" % (DB, Table),
                                      BeginExecDate=ExecDate,
                                      EndExecDate=ExecDate,
                                      Status="Error",
                                      Log=msg,
                                      Developer="developer")
               set_exit(LevelStatu="red", MSG=msg)
      if len(md5_file_false) > 0:
          wait_mins = 600
          if sleep_num <= wait_mins:
              min = 60
              print("等待第%s次%s秒"%(sleep_num,min))
              time.sleep(min)
          else:
              msg = "等待md5生成超时！！！\n%s" % (md5_file_false)
              msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                                     SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                                     TargetTable="%s.%s" % (DB, Table),
                                     BeginExecDate=ExecDate,
                                     EndExecDate=ExecDate,
                                     Status="Error",
                                     Log=msg,
                                     Developer="developer")
              set_exit(LevelStatu="red", MSG=msg)
      else:
          set_md5_file_true = False
      md5_file_false.clear()
      sleep_num = sleep_num + 1

def request_commit_account(AccountData="",Num="",InterfaceUrl="",ExecDate="",FileDir="",FileDirName="",DataJson=""):
    advertiser_list = []
    if AccountData is not None and len(AccountData)>0:
       for data in AccountData:
           account_id = str(data[0])
           service_code = str(data[2])
           advertiser_list.append({"serviceCode": service_code, "accountId": account_id})
       print("第【%s】批正在提交，子账户个数：【%s】！" % (Num,len(AccountData)))
       now_time = time.strftime("%H_%M_%S", time.localtime())
       file_name = "%s_%s_%s_%s_%s.log" % (airflow.dag, airflow.task, Num, ExecDate, now_time)
       file_dir_name = "%s/%s" % (FileDir, file_name)
       if os.path.exists(FileDir) is False:
          os.system("mkdir -p %s" % (FileDir))
       DataJson["%s" % (FileDirName)] = file_dir_name
       DataJson["advertiser_list"] = advertiser_list
       print("请求接口URL：%s" % (InterfaceUrl))
       print("请求接口参数：%s" % (DataJson))
       # 分子账户开启进程
       exec_interface_data_curl(URL=InterfaceUrl, Data=DataJson, File=file_dir_name, DataJsonRequest="DataJsonRequest")
       return file_dir_name
