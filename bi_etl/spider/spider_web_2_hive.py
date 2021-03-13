# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: spider_web_2_hive.py
# @Software: PyCharm
# function info：用于同步网络爬虫数据


from ecsage_bigdata_etl_engineering.common.base.get_config import Conf
from ecsage_bigdata_etl_engineering.common.base.airflow_instance import Airflow
from ecsage_bigdata_etl_engineering.common.operator.mysql.conn_mysql_metadb import EtlMetadata
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_data_2_ods
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_data_2_snap
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_alert_info_d
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_local_hdfs_thread
import os
import json
import socket

conf = Conf().conf
etl_md = EtlMetadata()

#入口方法
def main(TaskInfo,**kwargs):
    global airflow
    airflow = Airflow(kwargs)
    spider_home = conf.get("Spider", "spider_home")
    spider_data_home = conf.get("Spider", "spider_data_home")
    exec_date = airflow.execution_date_utc8_str[0:10]
    module_id = TaskInfo[5]
    module_name = TaskInfo[6]
    platform_id = TaskInfo[3]
    platform_name = TaskInfo[4]
    spider_id = TaskInfo[2]
    url = TaskInfo[7]
    data_level = TaskInfo[8]
    source_db = TaskInfo[10]
    source_table = TaskInfo[11]
    target_db = TaskInfo[13]
    target_table = TaskInfo[14]
    key_columns = TaskInfo[15]
    is_report = TaskInfo[16]
    beeline_session = set_db_session(SessionType="beeline", SessionHandler="beeline")
    if data_level == "spider":
       exec_spider(BeelineSession=beeline_session,SpiderId=spider_id,PlatformId=platform_id,
                   PlatformName=platform_name,ModuleId=module_id,ModuleName=module_name,URL=url,
                   ExecDate=exec_date,SpiderDataHome=spider_data_home,SpiderHome=spider_home,
                   TargetDB=target_db,TargetTable=target_table,RequestType=module_id
                   )
    elif data_level == "ods":
      hive_session = set_db_session(SessionType="hive", SessionHandler="hive")
      get_data_2_ods(HiveSession=hive_session, BeelineSession=beeline_session, SourceDB=source_db,
                     SourceTable=source_table, TargetDB=target_db, TargetTable=target_table,
                     ExecDate=exec_date, ArrayFlag="list", KeyColumns=key_columns, IsReplace=is_report,
                     DagId=airflow.dag, TaskId=airflow.task, CustomSetParameter="")
    #落地snap
    elif data_level == "snap":
      hive_session = set_db_session(SessionType="hive", SessionHandler="hive")
      get_data_2_snap(HiveSession=hive_session, BeelineSession=beeline_session, SourceDB=source_db,
                      SourceTable=source_table,TargetDB=target_db, TargetTable=target_table,IsReport=is_report,
                      KeyColumns=key_columns, ExecDate=exec_date,DagId=airflow.dag, TaskId=airflow.task
                      )
    else:
      print("元数据表字段data_level未指定对应值：spider、ods、snap")

def exec_spider(BeelineSession="",SpiderId="",PlatformId="",PlatformName="",
                ModuleId="",ModuleName="",URL="",ExecDate="",SpiderDataHome="",
                SpiderHome="",TargetDB="",TargetTable="",RequestType=""
                ):
    spider_info = {}
    spider_info["spider_date"] = ExecDate
    spider_info["module_id"] = ModuleId
    spider_info["module_name"] = ModuleName
    spider_info["platform_id"] = PlatformId
    spider_info["platform_name"] = PlatformName
    spider_info["spider_id"] = SpiderId
    spider_info["url"] = URL
    data_dir = SpiderDataHome + "/" + socket.gethostname() + "/" + ModuleId + "/" + ExecDate
    data_file = "%s_spider_data.%s.data" % (ModuleId, ExecDate)
    spider_info["data_dir"] = data_dir
    spider_info["data_file"] = data_file
    os.chdir(SpiderHome)
    ok = os.system("""python3 ecsage_bigdata_spider/spiders_main.py '%s'""" % (json.dumps(spider_info)))
    if ok != 0:
       print("爬虫出现异常")
    # 获取数据文件
    target_file = os.listdir(data_dir)
    data_task_file_list = []
    for files in target_file:
        if str(data_file.split("/")[-1]).split(".")[0] in files and '.lock' not in files:
            data_task_file_list.append("%s/%s" % (data_dir, files))
    load_data_2_etl_mid(BeelineSession=BeelineSession, LocalFileList=data_task_file_list,TargetDB=TargetDB,
                        TargetTable=TargetTable,ExecDate=ExecDate,RequestType=RequestType)
def load_data_2_etl_mid(BeelineSession="",LocalFileList="",TargetDB="",TargetTable="",ExecDate="",RequestType=""):
   if LocalFileList is None or len(LocalFileList) == 0:
      msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (TargetDB, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="API采集没执行！！！",
                               Developer="developer")
      set_exit(LevelStatu="yellow", MSG=msg)
   else:
    mid_sql = """
        create table if not exists %s.%s
        (
         request_data string
        )partitioned by(etl_date string,request_type string)
        row format delimited fields terminated by '\\001' 
        ;
    """ % (TargetDB,TargetTable)
    BeelineSession.execute_sql(mid_sql)
    load_num = 0
    hdfs_dir = conf.get("Airflow_New", "hdfs_home")
    load_table_sqls = ""
    load_table_sql_0 = ""
    load_table_sql = ""
    for data in LocalFileList:
        print(data,"####################################")
        local_file = """%s""" % (data)
        # 落地mid表
        if load_num == 0:
            load_table_sql_0 = """
                         load data inpath '{hdfs_dir}/{file_name}' OVERWRITE  INTO TABLE {target_db}.{target_table}
                         partition(etl_date='{exec_date}',request_type='{request_type}')
                         ;\n
            """.format(hdfs_dir=hdfs_dir, file_name=local_file.split("/")[-1], target_db=TargetDB,
                       target_table=TargetTable,exec_date=ExecDate,request_type=RequestType)
        else:
            load_table_sql = """
                         load data inpath '{hdfs_dir}/{file_name}' INTO TABLE {target_db}.{target_table}
                         partition(etl_date='{exec_date}',request_type='{request_type}')
                         ;\n
                     """.format(hdfs_dir=hdfs_dir, file_name=local_file.split("/")[-1],
                                target_db=TargetDB,target_table=TargetTable,exec_date=ExecDate,request_type=RequestType
                                )
        load_table_sqls = load_table_sql + load_table_sqls
        load_num = load_num + 1
    load_table_sqls = load_table_sql_0 + load_table_sqls
    # 上传hdfs
    get_local_hdfs_thread(TargetDb=TargetDB, TargetTable=TargetTable, ExecDate=ExecDate, DataFileList=LocalFileList,HDFSDir=hdfs_dir)
    print("结束上传HDFS，启动load")
    # 落地至hive
    ok_data = BeelineSession.execute_sql(load_table_sqls)
    if ok_data is False:
        msg = get_alert_info_d(DagId=airflow.dag, TaskId=airflow.task,
                               SourceTable="%s.%s" % ("SourceDB", "SourceTable"),
                               TargetTable="%s.%s" % (TargetDB, TargetTable),
                               BeginExecDate=ExecDate,
                               EndExecDate=ExecDate,
                               Status="Error",
                               Log="HDFS数据文件load入仓临时表出现异常！！！",
                               Developer="developer")
        set_exit(LevelStatu="red", MSG=msg)