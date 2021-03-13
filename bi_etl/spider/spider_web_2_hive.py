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
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
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
    beeline_session = set_db_session(SessionType="beeline", SessionHandler="beeline")
    if data_level == "spider":
       exec_spider(SpiderId=spider_id,PlatformId=platform_id,PlatformName=platform_name,
                   ModuleId=module_id,ModuleName=module_name,URL=url,ExecDate=exec_date,
                   SpiderDataHome=spider_data_home,SpiderHome=spider_home
                   )
    elif data_level == "ods":
      hive_session = set_db_session(SessionType="hive", SessionHandler="hive")
      get_data_2_ods(HiveSession=hive_session, BeelineSession=beeline_session, SourceDB=source_db,
                     SourceTable=source_table, TargetDB=target_db, TargetTable=target_table,
                     ExecDate=exec_date, ArrayFlag="list", KeyColumns=key_columns, IsReplace="N",
                     DagId=airflow.dag, TaskId=airflow.task, CustomSetParameter="")

    elif data_level == "snap":
      pass
    else:
      print("元数据表字段data_level未指定对应值：spider、ods、snap")

def exec_spider(SpiderId="",PlatformId="",PlatformName="",
                ModuleId="",ModuleName="",URL="",ExecDate="",
                SpiderDataHome="",SpiderHome=""
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

