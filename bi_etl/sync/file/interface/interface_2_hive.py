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
    #regexp_extract_column = TaskInfo[30]
    #if regexp_extract_column is None or len(regexp_extract_column) == 0:
    #    regexp_extract_column = """get_json_object(get_json_object(regexp_extract(a.request_data,'(\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\".*)',1),'$.data'),'$.list')"""
    exec_date = airflow.execution_date_utc8_str[0:10]
    hive_session = set_db_session(SessionType="hive", SessionHandler=hive_handler)
    beeline_session = set_db_session(SessionType="beeline", SessionHandler=beeline_handler)
    if Level == "file":
      data_json = ast.literal_eval(json.loads(data_json))
      if filter_modify_time_name is not None and len(filter_modify_time_name) > 0:
          data_json["%s" % (filter_modify_time_name)] = exec_date
      if start_date_name is not None and len(start_date_name) > 0 and end_date_name is not None and len(
              end_date_name) > 0:
          data_json["%s" % (start_date_name)] = exec_date
          data_json["%s" % (end_date_name)] = exec_date
      #数据文件落地至临时表
      get_file_2_hive(HiveSession=hive_session,BeelineSession=beeline_session,InterfaceUrl=interface_url,DataJson=data_json
                      ,FileDirName = file_dir_name,DataJsonRequest=data_json_request
                      ,InterfaceModule = interface_module
                      ,DB=target_db, Table=target_table,ExecData=exec_date
                     )
    elif Level == "ods":
      exec_ods_hive_table(HiveSession=hive_session,BeelineSession=beeline_session,SourceDB=source_db,SourceTable=source_table,
                          TargetDB=target_db, TargetTable=target_table,SelectExcludeColumns=select_exclude_columns,  ExecDate=exec_date)
    elif Level == "snap":
      exec_snap_hive_table(HiveSession=hive_session, BeelineSession=beeline_session, SourceDB=source_db, SourceTable=source_table,
                             TargetDB=target_db, TargetTable=target_table, IsReport=is_report, KeyColumns=key_columns, ExecDate=exec_date)

#含有level、time_line、date、group接口
def get_file_2_hive(HiveSession="",BeelineSession="",InterfaceUrl="",DataJson={},DataJsonRequest=""
                                   ,FileDirName = ""
                                   ,InterfaceModule = ""
                                   ,DB="", Table="",ExecData=""
                                   ):
    data_json = DataJson
    mysql_session = set_db_session(SessionType="mysql", SessionHandler="mysql_media")
    data = """
    '1653161780156424'
,'1658498174839822'
,'1662760699926536'
,'1665095253439502'
,'1670709254081550'
,'1671820444434445'
,'1673452167837699'
,'1673522523246599'
,'1673522564989960'
,'1673702410299400'
,'1674170835906568'
,'1674723663085582'
,'1675056486514702'
,'1676052584646663'
,'1676067208121358'
,'1676868173806664'
,'1676868175960071'
,'1676868192922632'
,'1677161568497672'
,'1677503629403143'
,'1678494892515341'
,'1679065200287748'
,'1679072628566023'
,'1679163834217485'
,'1680398059963406'
,'1680492787269640'
,'1680600914017294'
,'1680678587074632'
,'1681055953388558'
,'1681055986162766'
,'108322887258'
,'1637309287436296'
,'1638551217559559'
,'1639210111659011'
,'1642912728245255'
,'1645016138373134'
,'1654599060231176'
,'1654610662027278'
,'1654962649915399'
,'1654966498454541'
,'1655673239814157'
,'1655865478501389'
,'1659585572505608'
,'1660313214458884'
,'1661576493974540'
,'1662240551561219'
,'1663569364328460'
,'1665196281956360'
,'1666386257846279'
,'1669804387488776'
,'1672905779751950'
,'1675341144251405'
,'1675897159417870'
,'1677142102917134'
,'54830807065'
    """
    #ok,data_list = mysql_session.get_all_rows("""select account_id, media, service_code from big_data_mdg.media_advertiser where media = %s"""%(int(data_json["mt"])))
    ok, data_list = mysql_session.get_all_rows("""select account_id, media, service_code from big_data_mdg.media_advertiser where media =2 and account_id in (%s)""" % (data))
    num = 1
    nums = 1
    run_num = 0
    request_params = []
    print("开始执行调用接口")
    data_dir = conf.get("Interface", InterfaceModule)
    file_dir = "%s" % (data_dir) + "/" + airflow.ds_nodash_utc8 + "/%s/%s" % (airflow.dag,"test")#data_json["mt"])
    os.system("rm -rf %s" % (file_dir))
    file_dir_name_list = []
    advertiser_list = []
    for data in data_list:
       request_params.append(data)
       advertiser_list.append({"serviceCode":str(data[2]),"accountId":str(data[1])})
       if num == 10 or nums == len(data_list):
          run_num = run_num + 1
          print("第%s批正在提交！%s"%run_num,advertiser_list)
          #######for request_num in request_params:
          #######    account_id = request_num[0]
          #######    mt = int(request_num[1])
          #######    service_code = request_num[2]
          #######    now_time = time.strftime("%H_%M_%S", time.localtime())
          #######    file_name = "%s_%s_%s_%s_%s_%s_%s.log" % (airflow.dag, airflow.task, mt,account_id,service_code,ExecData, now_time)
          #######    file_dir_name = "%s/%s" % (file_dir, file_name)
          #######    if os.path.exists(file_dir) is False:
          #######        os.system("mkdir -p %s" % (file_dir))
          #######    file_dir_name_list.append((file_dir_name,mt,account_id))
          #######    data_json["%s" % (FileDirName)] = file_dir_name
          #######    data_json["mt"] = mt
          #######    data_json["advertiser_list"] = [{"serviceCode":service_code,"accountId":account_id}]
          #######    # 分子账户开启进程
          #######    #exec_interface_data_curl(URL=InterfaceUrl, Data=data_json, File=file_dir_name,DataJsonRequest=DataJsonRequest)
          time.sleep(60)
          num = 0
          #request_params.clear()
          advertiser_list.clear()
       num = num + 1
       nums = nums + 1
    print("结束执行调用接口")
   ####### md5_file_false = []
   ####### md5_file_true = []
   ####### md5_file_empty = []
   ####### set_md5_file_true = True
   ####### sleep_num = 1
   ####### for file in file_dir_name_list:
   #######     print(file[0],"=======================================")
   #######     #判断md5文件是否存在
   #######     is_md5_file = os.path.exists(file[0]+".md5")
   #######     if is_md5_file is False:
   #######         md5_file_false.append(file)
   #######     else:
   #######         if os.path.getsize(file[0]+".md5") > 0:
   #######            md5_file_true.append(file[0])
   #######         else:
   #######            md5_file_empty.append(file)
   ####### #判断是否没有md5的文件
   ####### while set_md5_file_true:
   #######    for md5_file_falses in md5_file_false:
   #######        if os.path.exists(md5_file_falses) is False:
   #######            pass
   #######        else:
   #######            if os.path.getsize(md5_file_falses) > 0:
   #######                md5_file_true.append(md5_file_falses)
   #######            else:
   #######                md5_file_empty.append(md5_file_falses)
   #######            md5_file_false.remove(md5_file_falses)
   #######    if len(md5_file_false) > 0:
   #######        print("等待MD5文件生成。5分钟，5分钟！！！")
   #######        if sleep_num == 1:
   #######           time.sleep(300)
   #######        else:
   #######           set_md5_file_true = False
   #######    else:
   #######        set_md5_file_true = False
   #######    sleep_num = sleep_num + 1


    #落地临时表
    #exec_file_2_hive(HiveSession=HiveSession,BeelineSession=BeelineSession,LocalFileName=file_dir_name,ParamsMD5=param_md5,DB=DB,Table=Table,ExecDate=ExecData)

def exec_file_2_hive(HiveSession="",BeelineSession="",LocalFileName="",ParamsMD5="",DB="",Table="",ExecDate=""):
    param_table = """%s.%s_param"""%(DB,Table)
    mid_table = """%s.%s""" % (DB, Table)
    param_file = """%s.param""" % (LocalFileName)
    local_file = """%s""" % (LocalFileName)
    # 创建data临时表
    param_sql = """
          create table if not exists %s
          (
           request_param string
          )partitioned by(etl_date string,md5_id string)
          row format delimited fields terminated by '\\001' 
        """%(param_table)
    mid_sql = """
              create table if not exists %s
              (
               request_data string
              )partitioned by(etl_date string,md5_id string)
              row format delimited fields terminated by '\\001' 
            """%(mid_table)
    HiveSession.execute_sql(param_sql)
    HiveSession.execute_sql(mid_sql)
    # 上传本地数据文件至HDFS
    hdfs_dir = "/tmp/datafolder_new"
    #上传param文件
    print("""hadoop fs -moveFromLocal -f %s %s""" % (param_file, hdfs_dir), "************************************")
    ok_param = os.system("hadoop fs -moveFromLocal -f %s %s" % (param_file, hdfs_dir))
    # 上传数据文件
    print("""hadoop fs -moveFromLocal -f %s %s""" % (local_file, hdfs_dir), "************************************")
    ok_data = os.system("hadoop fs -moveFromLocal -f %s %s" % (local_file, hdfs_dir))
    if ok_param != 0 and ok_data != 0:
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (DB, Table),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="上传本地数据文件至HDFS出现异常！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)
    #落地param表
    load_table_sql = """
            load data inpath '{hdfs_dir}/{file_name}' OVERWRITE  INTO TABLE {table_name}
            partition(etl_date='{exec_date}',md5_id='{md5_id}')
        """.format(hdfs_dir=hdfs_dir, file_name=param_file.split("/")[-1], table_name=param_table,exec_date=ExecDate,md5_id=ParamsMD5)
    ok_param = HiveSession.execute_sql(load_table_sql)
    # 落地mid表
    load_table_sql = """
                load data inpath '{hdfs_dir}/{file_name}' OVERWRITE  INTO TABLE {table_name}
                partition(etl_date='{exec_date}',md5_id='{md5_id}')
            """.format(hdfs_dir=hdfs_dir, file_name=local_file.split("/")[-1], table_name=mid_table,exec_date=ExecDate,md5_id=ParamsMD5)
    ok_data = HiveSession.execute_sql(load_table_sql)
    if ok_param is False and ok_data is False:
        # 删除临时表
        HiveSession.execute_sql("""drop table if exists %s""" % (param_table))
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
    sql = """
        add file hdfs:///tmp/airflow/get_arrary.py;
        drop table if exists %s_check_request;
        create table %s_check_request as
        select tmp.returns_colums,tmp.`num` ,cast(tmp1.total_number as int) as num_1
        from(select count(request_id) as num,returns_colums
             from (select returns_colums,data__num_colums,request_colums,request_id
                   from(select split(split(data_colums,'@@####@@')[0],'##&&##')[0] as returns_colums
                               ,split(data_colums,'@@####@@')[1] as data_colums
                               ,split(split(data_colums,'@@####@@')[0],'##&&##')[1] as request_colums
                               ,split(split(data_colums,'@@####@@')[0],'##&&##')[2] as request_id
                   from(select transform(data_col) USING 'python get_arrary.py' as (data_colums)
                        from(select concat_ws('##@@',concat_ws('##&&##',returns_colums,request_param,request_id),data_colums) as data_col
                             from(select regexp_replace(regexp_extract(a.request_data,'(returns :.*\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\")',1),'\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\"','') as returns_colums
                                    ,get_json_object(get_json_object(regexp_extract(a.request_data,'(\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\".*)',1),'$.data'),'$.list') as data_colums
                                    ,b.request_param
                                    ,get_json_object(regexp_extract(a.request_data,'(\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\".*)',1),'$.request_id') as request_id
                                  from %s a
                                  inner join %s b
                                  on a.etl_date = b.etl_date
                                  and a.md5_id = b.md5_id
                                  where a.etl_date = '%s'
                                    and a.md5_id = '%s'
                             ) a
                        ) b
                        where data_col like '%s'
                        ) b
                    ) c
                    lateral view explode(split(data_colums, '##@@')) num_line as data__num_colums
                ) a
                group by returns_colums
            ) tmp
        inner join(select returns_colums,total_number 
                   from(select regexp_replace(regexp_extract(a.request_data,'(returns :.*\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\")',1),'\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\"','') as returns_colums
                               ,get_json_object(get_json_object(get_json_object(regexp_extract(a.request_data,'(\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\".*)',1),'$.data'),'$.page_info'),'$.total_number') as total_number
                       from %s a
                       inner join %s b
                       on a.etl_date = b.etl_date
                       and a.md5_id = b.md5_id
                       where a.etl_date = '%s'
                         and a.md5_id = '%s'
                      ) a
                    group by returns_colums,total_number
                ) tmp1
        on tmp.returns_colums = tmp1.returns_colums
        where tmp.`num` <> cast(tmp1.total_number as int)
    """%(mid_table,mid_table,mid_table,param_table,ExecDate,ParamsMD5,"""%##@@%""",mid_table,param_table,ExecDate,ParamsMD5)
    ok = BeelineSession.execute_sql(sql)
    if ok is False:
       sql = """drop table if exists %s_check_request"""%(mid_table)
       HiveSession.execute_sql(sql)
       msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (DB, Table),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="校验执行失败！！！",
                               Developer="developer")
       set_exit(LevelStatu="red", MSG=msg) 
    ok,data = HiveSession.get_all_rows("select * from %s_check_request limit 1"%(mid_table))
    sql = """drop table if exists %s_check_request""" % (mid_table)
    HiveSession.execute_sql(sql)
    data = []
    print("采集接口数据：" + str(data))
    if ok is False or len(data) > 0:
       print("采集接口异常数据："+ str(data))
       msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (DB, Table),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="采集接口数据出现异常，源与目标文件对不上！！！",
                               Developer="developer")
       set_exit(LevelStatu="red", MSG=msg) 

#落地至ods
def exec_ods_hive_table(HiveSession="",BeelineSession="",SourceDB="",SourceTable="",
                        TargetDB="", TargetTable="",SelectExcludeColumns="",ExecDate=""):
   ok,get_ods_column = HiveSession.get_column_info(TargetDB,TargetTable)
   system_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
   system_table_columns = "returns_colums,request_colums,extract_system_time,etl_date"
   select_system_table_column = "returns_colums,request_colums,'%s' as extract_system_time"%(system_time)
   select_exclude_columns = SelectExcludeColumns
   if select_exclude_columns is None or len(select_exclude_columns) == 0:
       select_exclude_columns = "000000"
   columns = ""
   for column in get_ods_column:
      columns = columns + "," + column[0]
      if column[0] == "etl_date":
          break;
   columns = columns.replace(",", "", 1)
   print(columns,"#######################################")
   json_tuple_columns = ""
   for get_json_tuple_column in columns.split(","):
       if get_json_tuple_column not in select_exclude_columns.split(",") and get_json_tuple_column not in system_table_columns.split(","):
          json_tuple_columns = json_tuple_columns + "," + "'%s'"%(get_json_tuple_column)
   json_tuple_columns = json_tuple_columns.replace(",", "", 1)
   json_tuple_column = json_tuple_columns.replace("'", "")
   select_json_tuple_column = json_tuple_columns.replace("'", "`")
   print(json_tuple_columns,"#######################################")
   print(json_tuple_column,"#######################################")
   sql = """
        add file hdfs:///tmp/airflow/get_arrary.py;
        insert overwrite table %s.%s
        partition(etl_date = '%s')
        select %s,%s
        from (select returns_colums,data__num_colums,request_colums
              from(select split(split(data_colums,'@@####@@')[0],'##&&##')[0] as returns_colums
                          ,split(data_colums,'@@####@@')[1] as data_colums
                          ,split(split(data_colums,'@@####@@')[0],'##&&##')[1] as request_colums
                   from(select transform(concat_ws('##@@',concat_ws('##&&##',returns_colums,request_param),data_colums)) USING 'python get_arrary.py' as (data_colums)
                        from(select regexp_replace(regexp_extract(a.request_data,'(returns :.*\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\")',1),'\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\"','') as returns_colums
                                    ,get_json_object(get_json_object(regexp_extract(a.request_data,'(\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\".*)',1),'$.data'),'$.list') as data_colums
                                    ,b.request_param
                             from %s.%s a
                             inner join %s.%s_param b
                             on a.etl_date = b.etl_date
                             and a.md5_id = b.md5_id
                             where a.etl_date = '%s'
                            ) a
                        ) b
                   ) c
                   lateral view explode(split(data_colums, '##@@')) num_line as data__num_colums
              ) a
              lateral view json_tuple(data__num_colums,%s) b
              as %s
               ;
        """%(TargetDB,TargetTable,ExecDate,select_json_tuple_column,select_system_table_column,SourceDB,SourceTable,SourceDB,SourceTable,ExecDate,json_tuple_columns,select_json_tuple_column)
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
   #校验ods与临时数据条数是否一致
   sql = """
       select a.total_number as total_number_mid,b.total_number as total_number_ods
       from(
            select sum(cast(tmp1.total_number as int)) as total_number
            from(select returns_colums,total_number 
                 from(select regexp_replace(regexp_extract(a.request_data,'(returns :.*\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\")',1),'\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\"','') as returns_colums
                             ,get_json_object(get_json_object(get_json_object(regexp_extract(a.request_data,'(\\\\{\\\\"code\\\\":0,\\\\"message\\\\":\\\\"OK\\\\".*)',1),'$.data'),'$.page_info'),'$.total_number') as total_number
                      from %s.%s a
                      inner join %s.%s_param b
                      on a.etl_date = b.etl_date
                      and a.md5_id = b.md5_id
                      where a.etl_date = '%s'
                      ) a
                 group by returns_colums,total_number
                ) tmp1
            ) a
       inner join (select count(1) as total_number from %s.%s where etl_date = '%s') b
       on 1 = 1
       where a.total_number <> b.total_number
   """%(SourceDB,SourceTable,SourceDB,SourceTable,ExecDate,TargetDB,TargetTable,ExecDate)
   ok,data = HiveSession.get_all_rows(sql)
   data = []
   print("ods入库数据：" + str(data))
   if ok is False or len(data) > 0:
       print("ods入库异常数据：" + str(data))
       msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                              SourceTable="%s.%s" % (SourceDB, SourceTable),
                              TargetTable="%s.%s" % (TargetDB, TargetTable),
                              BeginExecDate=ExecDate,
                              EndExecDate=ExecDate,
                              Status="Error",
                              Log="ods校验失败！！！",
                              Developer="developer")
       set_exit(LevelStatu="red", MSG=msg)
   #BeelineSession.execute_sql("drop table if exists %s.%s" % (SourceDB, SourceTable))
   #BeelineSession.execute_sql("drop table if exists %s.%s_param" % (SourceDB, SourceTable))

#落地至snap
def exec_snap_hive_table(HiveSession="",BeelineSession="",SourceDB="",SourceTable="",
                        TargetDB="", TargetTable="",IsReport="",KeyColumns="",ExecDate=""):
   #设置snap查询字段
   snap_columns = ""
   if IsReport == 0:
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
   ok = HiveSession.execute_sql(sql)
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
   ok, data = HiveSession.get_all_rows(sql_check)
   data = []
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
