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
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_create_dag_alert
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_local_hdfs_thread
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_alert_info_d

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
                     )
    elif Level == "ods":
      exec_ods_hive_table(HiveSession=hive_session,BeelineSession=beeline_session,SourceDB=source_db,SourceTable=source_table,
                          TargetDB=target_db, TargetTable=target_table,IsReport=is_report,SelectExcludeColumns=select_exclude_columns,KeyColumns=key_columns,ExecDate=exec_date)
    elif Level == "snap":
      exec_snap_hive_table(HiveSession=hive_session, BeelineSession=beeline_session, SourceDB=source_db, SourceTable=source_table,
                             TargetDB=target_db, TargetTable=target_table, IsReport=is_report, KeyColumns=key_columns, ExecDate=exec_date)

#含有level、time_line、date、group接口
def get_file_2_hive(HiveSession="",BeelineSession="",InterfaceUrl="",DataJson={},DataJsonRequest=""
                                   ,FileDirName = ""
                                   ,InterfaceModule = "",CommitNum=""
                                   ,DB="", Table="",ExecDate="",IsReport=""
                                   ,Exclude_Account_id=""
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
    exec_file_2_hive(HiveSession=HiveSession,BeelineSession=BeelineSession,LocalFileName=file_dir_name_list,RequestType=request_type,DeleteType=delete_type,DB=DB,Table=Table,ExecDate=ExecDate)

def exec_file_2_hive(HiveSession="",BeelineSession="",LocalFileName="",RequestType="",DeleteType="",DB="",Table="",ExecDate=""):
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
    if len(load_table_sql) == 0:
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
    get_local_hdfs_thread(TargetDb=DB, TargetTable=Table, ExecDate=ExecDate, DataFileList=LocalFileName,HDFSDir=hdfs_dir)
    #落地至hive
    ok_data = BeelineSession.execute_sql(load_table_sqls)
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
    #校验接口数据是否一致
    ####### sql = """
    #######     add file hdfs:///tmp/airflow/get_arrary.py;
    #######     drop table if exists %s_check_request;
    #######     create table %s_check_request as
    #######     select tmp.returns_colums,tmp.`num` ,cast(tmp1.total_number as int) as num_1
    #######     from(select count(request_id) as num,returns_colums
    #######          from (select returns_colums,data__num_colums,request_colums,request_id
    #######                from(select split(split(data_colums,'@@####@@')[0],'##&&##')[0] as returns_colums
    #######                            ,split(data_colums,'@@####@@')[1] as data_colums
    #######                            ,split(split(data_colums,'@@####@@')[0],'##&&##')[1] as request_colums
    #######                            ,split(split(data_colums,'@@####@@')[0],'##&&##')[2] as request_id
    #######                from(select transform(data_col) USING 'python get_arrary.py' as (data_colums)
    #######                     from(select concat_ws('##@@',concat_ws('##&&##',returns_colums,request_param,request_id),data_colums) as data_col
    #######                          from(select regexp_replace(regexp_extract(a.request_data,'(returns :.*\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\")',1),'\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\"','') as returns_colums
    #######                                 ,get_json_object(get_json_object(regexp_extract(a.request_data,'(\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\".*)',1),'$.data'),'$.list') as data_colums
    #######                                 ,b.request_param
    #######                                 ,get_json_object(regexp_extract(a.request_data,'(\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\".*)',1),'$.request_id') as request_id
    #######                               from %s a
    #######                               inner join %s b
    #######                               on a.etl_date = b.etl_date
    #######                               and a.md5_id = b.md5_id
    #######                               where a.etl_date = '%s'
    #######                                 and a.md5_id = '%s'
    #######                          ) a
    #######                     ) b
    #######                     where data_col like '%s'
    #######                     ) b
    #######                 ) c
    #######                 lateral view explode(split(data_colums, '##@@')) num_line as data__num_colums
    #######             ) a
    #######             group by returns_colums
    #######         ) tmp
    #######     inner join(select returns_colums,total_number
    #######                from(select regexp_replace(regexp_extract(a.request_data,'(returns :.*\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\")',1),'\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\"','') as returns_colums
    #######                            ,get_json_object(get_json_object(get_json_object(regexp_extract(a.request_data,'(\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\".*)',1),'$.data'),'$.page_info'),'$.total_number') as total_number
    #######                    from %s a
    #######                    inner join %s b
    #######                    on a.etl_date = b.etl_date
    #######                    and a.md5_id = b.md5_id
    #######                    where a.etl_date = '%s'
    #######                      and a.md5_id = '%s'
    #######                   ) a
    #######                 group by returns_colums,total_number
    #######             ) tmp1
    #######     on tmp.returns_colums = tmp1.returns_colums
    #######     where tmp.`num` <> cast(tmp1.total_number as int)
    ####### """%(mid_table,mid_table,mid_table,param_table,ExecDate,ParamsMD5,"""%##@@%""",mid_table,param_table,ExecDate,ParamsMD5)
    ####### ok = BeelineSession.execute_sql(sql)
    ####### if ok is False:
    #######    sql = """drop table if exists %s_check_request"""%(mid_table)
    #######    HiveSession.execute_sql(sql)
    #######    msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
    #######                            SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
    #######                            TargetTable="%s.%s" % (DB, Table),
    #######                            BeginExecDate=ExecDate,
    #######                            EndExecDate=ExecDate,
    #######                            Status="Error",
    #######                            Log="校验执行失败！！！",
    #######                            Developer="developer")
    #######    set_exit(LevelStatu="red", MSG=msg)
    ####### ok,data = HiveSession.get_all_rows("select * from %s_check_request limit 1"%(mid_table))
    ####### sql = """drop table if exists %s_check_request""" % (mid_table)
    ####### HiveSession.execute_sql(sql)
    ####### data = []
    ####### print("采集接口数据：" + str(data))
    ####### if ok is False or len(data) > 0:
    #######    print("采集接口异常数据："+ str(data))
    #######    msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
    #######                            SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
    #######                            TargetTable="%s.%s" % (DB, Table),
    #######                            BeginExecDate=ExecDate,
    #######                            EndExecDate=ExecDate,
    #######                            Status="Error",
    #######                            Log="采集接口数据出现异常，源与目标文件对不上！！！",
    #######                            Developer="developer")
    #######    set_exit(LevelStatu="red", MSG=msg)

#落地至ods
def exec_ods_hive_table(HiveSession="",BeelineSession="",SourceDB="",SourceTable="",
                        TargetDB="", TargetTable="",IsReport="",SelectExcludeColumns="",KeyColumns="",ExecDate=""):
   ok,get_ods_column = HiveSession.get_column_info(TargetDB,TargetTable)
   system_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
   system_table_columns = "returns_account_id,returns_colums,request_type,extract_system_time,etl_date"
   select_system_table_column = "returns_account_id,returns_colums,request_type,'%s' as extract_system_time"%(system_time)
   is_key_columns(SourceDB=SourceDB, SourceTable=SourceTable, TargetDB=TargetDB,TargetTable=TargetTable, ExecDate=ExecDate, KeyColumns=KeyColumns)
   row_number_columns = ""
   key_column_list = KeyColumns.split(",")
   for key in key_column_list:
       row_number_columns = row_number_columns + "," + "`" + key + "`"
   row_number_columns = row_number_columns.replace(",", "", 1)
   select_exclude_columns = SelectExcludeColumns
   if select_exclude_columns is None or len(select_exclude_columns) == 0:
       select_exclude_columns = "000000"
   columns = ""
   for column in get_ods_column:
      columns = columns + "," + column[0]
      if column[0] == "etl_date":
          break;
   columns = columns.replace(",", "", 1)
   json_tuple_columns = ""
   for get_json_tuple_column in columns.split(","):
       if get_json_tuple_column not in select_exclude_columns.split(",") and get_json_tuple_column not in system_table_columns.split(","):
          json_tuple_columns = json_tuple_columns + "," + "'%s'"%(get_json_tuple_column)
   json_tuple_columns = json_tuple_columns.replace(",", "", 1)
   json_tuple_column = json_tuple_columns.replace("'", "")
   select_json_tuple_column = json_tuple_columns.replace("'", "`")
   regexp_extract = """get_json_object(get_json_object(regexp_replace(regexp_extract(a.request_data,'(\\\\"\\\\}## \\\\{\\\\".*)',1),'\\\\"\\\\}## ',''),'$.data'),'$.list') as data_colums"""
   return_regexp_extract = """regexp_replace(regexp_extract(a.request_data,'(##\\\\{\\\\"accountId\\\\":.*\\\\}##)',1),'##','') as returns_colums"""
   returns_account_id = """trim(get_json_object(regexp_replace(regexp_replace(regexp_extract(a.request_data,'(##\\\\{\\\\"accountId\\\\":.*\\\\}## )',1),'##',''),' ',''),'$.accountId')) as returns_account_id"""
   filter_line = """length(regexp_extract(a.request_data,'(\\\\"\\\\}## \\\\{\\\\".*)',1)) > 0"""
   ######if IsReport == 1:
   ######    regexp_extract = """get_json_object(get_json_object(regexp_replace(regexp_extract(a.request_data,'(\\\\"\\\\}## \\\\{\\\\".*)',1),'\\\\"\\\\}## ',''),'$.data'),'$.list') as data_colums"""
   ######    return_regexp_extract = """regexp_replace(regexp_extract(a.request_data,'(##\\\\{\\\\"accountId\\\\":.*\\\\}##)',1),'##','') as returns_colums"""
   ######    returns_account_id = """trim(get_json_object(regexp_replace(regexp_replace(regexp_extract(a.request_data,'(##\\\\{\\\\"accountId\\\\":.*\\\\}## )',1),'##',''),' ',''),'$.accountId')) as returns_account_id"""
   ######else:
   ######   regexp_extract = """get_json_object(get_json_object(regexp_extract(a.request_data,'(\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\".*)',1),'$.data'),'$.list') as data_colums"""
   ######   return_regexp_extract = """regexp_replace(regexp_extract(a.request_data,'(accountId:.*\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\")',1),'\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\"','') as returns_colums"""
   ######   returns_account_id = """trim(regexp_replace(regexp_replace(regexp_replace(regexp_extract(a.request_data,'(accountId:.*\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\")',1),'\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\"',''),'accountId: ',''),',.*','')) as returns_account_id"""
   sql = """
        add file hdfs:///tmp/airflow/get_arrary.py;
        drop table if exists %s.%s_tmp;
        create table %s.%s_tmp stored as parquet as 
        select %s,%s
        from (select returns_colums,data_num_colums,returns_account_id,request_type
              from(select split(split(data_colums,'@@####@@')[0],'##&&##')[0] as returns_colums
                          ,split(data_colums,'@@####@@')[1] as data_colums
                          ,split(split(data_colums,'@@####@@')[0],'##&&##')[1] as returns_account_id
                          ,split(split(data_colums,'@@####@@')[0],'##&&##')[2] as request_type
                   from(select transform(concat_ws('##@@',concat_ws('##&&##',returns_colums,returns_account_id,request_type),data_colums)) USING 'python get_arrary.py' as (data_colums)
                        from(select %s
                                    ,%s
                                    ,%s
                                    ,request_type
                             from %s.%s a
                             where a.etl_date = '%s'
                               and %s
                            ) a
                        where data_colums is not null
                        ) b
                   ) c
                   lateral view explode(split(data_colums, '##@@')) num_line as data_num_colums
              ) a
              lateral view json_tuple(data_num_colums,%s) b
              as %s
               ;
        """%("etl_mid",TargetTable,"etl_mid",TargetTable,select_json_tuple_column,select_system_table_column,return_regexp_extract,regexp_extract,returns_account_id,SourceDB,SourceTable,ExecDate,filter_line,json_tuple_columns,select_json_tuple_column)
   ok = BeelineSession.execute_sql(sql)
   if ok is False:
       msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                              SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                              TargetTable="%s.%s" % (TargetDB, TargetTable),
                              BeginExecDate=ExecDate,
                              EndExecDate=ExecDate,
                              Status="Error",
                              Log="ods入库-tmp失败！！！",
                              Developer="developer")
       set_exit(LevelStatu="red", MSG=msg)
   sql = """
        insert overwrite table %s.%s
        partition(etl_date = '%s')
        select %s,%s from(
        select %s,%s,row_number()over(partition by %s order by 1) as rn_row_number
        from %s.%s_tmp
        ) tmp where rn_row_number = 1
               ;
        drop table if exists %s.%s_tmp;
        """%(TargetDB,TargetTable,ExecDate,select_json_tuple_column,select_system_table_column,select_json_tuple_column,select_system_table_column,row_number_columns,"etl_mid",TargetTable,"etl_mid",TargetTable)
   ok = BeelineSession.execute_sql(sql)
   if ok is False:
       msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                              SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                              TargetTable="%s.%s" % (TargetDB, TargetTable),
                              BeginExecDate=ExecDate,
                              EndExecDate=ExecDate,
                              Status="Error",
                              Log="ods入库失败！！！",
                              Developer="developer")
       set_exit(LevelStatu="red", MSG=msg)
   ######## #校验ods与临时数据条数是否一致
   ######## sql = """
   ########     select a.total_number as total_number_mid,b.total_number as total_number_ods
   ########     from(
   ########          select sum(cast(tmp1.total_number as int)) as total_number
   ########          from(select returns_colums,total_number
   ########               from(select regexp_replace(regexp_extract(a.request_data,'(returns :.*\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\")',1),'\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\"','') as returns_colums
   ########                           ,get_json_object(get_json_object(get_json_object(regexp_extract(a.request_data,'(\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\".*)',1),'$.data'),'$.page_info'),'$.total_number') as total_number
   ########                    from %s.%s a
   ########                    inner join %s.%s_param b
   ########                    on a.etl_date = b.etl_date
   ########                    and a.md5_id = b.md5_id
   ########                    where a.etl_date = '%s'
   ########                    ) a
   ########               group by returns_colums,total_number
   ########              ) tmp1
   ########          ) a
   ########     inner join (select count(1) as total_number from %s.%s where etl_date = '%s') b
   ########     on 1 = 1
   ########     where a.total_number <> b.total_number
   ######## """%(SourceDB,SourceTable,SourceDB,SourceTable,ExecDate,TargetDB,TargetTable,ExecDate)
   ######## ok,data = HiveSession.get_all_rows(sql)
   ######## data = []
   ######## print("ods入库数据：" + str(data))
   ######## if ok is False or len(data) > 0:
   ########     print("ods入库异常数据：" + str(data))
   ########     msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
   ########                            SourceTable="%s.%s" % (SourceDB, SourceTable),
   ########                            TargetTable="%s.%s" % (TargetDB, TargetTable),
   ########                            BeginExecDate=ExecDate,
   ########                            EndExecDate=ExecDate,
   ########                            Status="Error",
   ########                            Log="ods校验失败！！！",
   ########                            Developer="developer")
   ########     set_exit(LevelStatu="red", MSG=msg)
   #BeelineSession.execute_sql("drop table if exists %s.%s" % (SourceDB, SourceTable))
   #BeelineSession.execute_sql("drop table if exists %s.%s_param" % (SourceDB, SourceTable))

#落地至snap
def exec_snap_hive_table(HiveSession="",BeelineSession="",SourceDB="",SourceTable="",
                        TargetDB="", TargetTable="",IsReport="",KeyColumns="",ExecDate=""):
   #设置snap查询字段
   snap_columns = ""
   if IsReport == 0:
       is_key_columns(SourceDB=SourceDB, SourceTable=SourceTable, TargetDB=TargetDB,
                      TargetTable=TargetTable, ExecDate=ExecDate, KeyColumns=KeyColumns)
       key_column_list = KeyColumns.split(",")
       is_null_col = "`"+key_column_list[0]+"`"
       key_columns_joins = ""
       num = 0
       for key in key_column_list:
           if num == 0:
              key_columns_join = "on a.`%s` = b.`%s`"%(key,key)
           else:
               key_columns_join = "and a.`%s` = b.`%s`" % (key, key)
           key_columns_joins = key_columns_joins + " " + key_columns_join
       #获取ods表字段
       ok,ods_table_columns = HiveSession.get_column_info(SourceDB,SourceTable)
       ods_columns = ""
       for column in ods_table_columns:
           create_col = """,`%s`  %s comment'%s' \n"""%(column[0],column[1],column[2])
           ods_columns = ods_columns + create_col
           if column[0] == "etl_date":
               break;
       ods_columns = ods_columns.replace(",", "", 1)
       create_snap_sql = """
       create table if not exists %s.%s(
         %s
       )
       row format delimited fields terminated by '\\001' 
       stored as parquet
       """%(TargetDB,TargetTable,ods_columns)
       HiveSession.execute_sql(create_snap_sql)
       #获取snap表字段
       ok, snap_table_columns = HiveSession.get_column_info(TargetDB, TargetTable)
       for column in snap_table_columns:
           snap_columns = snap_columns + "," + "a.`%s`"%(column[0])
       snap_columns = snap_columns.replace(",", "", 1)
       sql = """
           insert overwrite table %s.%s
           select %s
           from %s.%s a
           left join %s.%s b
           %s
           and b.etl_date = '%s'
           where b.%s is null
              union all
           select %s from %s.%s a where etl_date = '%s'
       """%(TargetDB,TargetTable,snap_columns,TargetDB,TargetTable,SourceDB,SourceTable,
            key_columns_joins,ExecDate,is_null_col,snap_columns,SourceDB, SourceTable,ExecDate
            )
   else:
       # 获取ods表字段
       ok, ods_table_columns = HiveSession.get_column_info(SourceDB, SourceTable)
       ods_columns = ""
       for column in ods_table_columns:
           create_col = """,`%s`  %s comment'%s' \n""" % (column[0], column[1], column[2])
           ods_columns = ods_columns + create_col
           if column[0] == "etl_date":
               break;
       ods_columns = ods_columns.replace(",", "", 1)
       create_snap_sql = """
              create table if not exists %s.%s(
                %s
              )
              row format delimited fields terminated by '\\001' 
              stored as parquet
              """ % (TargetDB, TargetTable, ods_columns)
       HiveSession.execute_sql(create_snap_sql)
       # 获取snap表字段
       ok, snap_table_columns = HiveSession.get_column_info(TargetDB, TargetTable)
       for column in snap_table_columns:
           snap_columns = snap_columns + "," + "a.`%s`" % (column[0])
       snap_columns = snap_columns.replace(",", "", 1)
       sql = """
        insert overwrite table %s.%s
        select %s
        from %s.%s a
        where etl_date != '%s'
           union all
        select %s
        from %s.%s a where etl_date = '%s' 
       """%(TargetDB,TargetTable,snap_columns,TargetDB,TargetTable,ExecDate,snap_columns,SourceDB,SourceTable,ExecDate)
   ok = BeelineSession.execute_sql(sql)
   if ok is False:
       msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                              SourceTable="%s.%s" % (SourceDB, SourceTable),
                              TargetTable="%s.%s" % (TargetDB, TargetTable),
                              BeginExecDate=ExecDate,
                              EndExecDate=ExecDate,
                              Status="Error",
                              Log="snap入库失败！！！",
                              Developer="developer")
       set_exit(LevelStatu="red", MSG=msg)
   sql_check = """
               select a.source_cnt,b.target_cnt
               from(select count(1) as source_cnt
                    from %s.%s where etl_date = '%s'
                   ) a
               inner join (
                    select count(1) as target_cnt
                    from %s.%s where etl_date = '%s'
               ) b
               on 1 = 1
               where a.source_cnt <> b.target_cnt
               """%(SourceDB,SourceTable,ExecDate,TargetDB,TargetTable,ExecDate)
   #ok, data = HiveSession.get_all_rows(sql_check)
   data = []
   ok = True
   print("snap入库数据：" + str(data))
   if ok is False or len(data) > 0:
       msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                              SourceTable="%s.%s" % (SourceDB, SourceTable),
                              TargetTable="%s.%s" % (TargetDB, TargetTable),
                              BeginExecDate=ExecDate,
                              EndExecDate=ExecDate,
                              Status="Error",
                              Log="snap入库数据对比不上！！！",
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
