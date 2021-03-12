# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: spider_web_2_hive.py
# @Software: PyCharm
# function info：用于同步网络爬虫数据


from ecsage_bigdata_etl_engineering.common.base.get_config import Conf
from ecsage_bigdata_etl_engineering.common.base.airflow_instance import Airflow
from ecsage_bigdata_etl_engineering.common.operator.mysql.conn_mysql_metadb import EtlMetadata
import os
import json

conf = Conf().conf
etl_md = EtlMetadata()

#入口方法
def main(TaskInfo,**kwargs):
    global airflow
    airflow = Airflow(kwargs)
    spider_info = {}
    spider_home = conf.get("Spider", "spider_home")
    spider_data_home = conf.get("Spider", "spider_data_home")
    spider_date = airflow.execution_date_utc8_str[0:10]
    module_id = TaskInfo[5]
    module_name = TaskInfo[6]
    platform_id = TaskInfo[3]
    platform_name = TaskInfo[4]
    spider_id = TaskInfo[2]
    url = TaskInfo[7]
    data_level = TaskInfo[8]
    spider_info["spider_data_home"] = spider_data_home
    spider_info["spider_date"] = spider_date
    spider_info["module_id"] = module_id
    spider_info["module_name"] = module_name
    spider_info["platform_id"] = platform_id
    spider_info["platform_name"] = platform_name
    spider_info["spider_id"] = spider_id
    spider_info["url"] = url
    if data_level == "spider":
      os.chdir(spider_home)
      ok = os.system("""python3 ecsage_bigdata_spider/spiders_main.py '%s'"""%(json.dumps(spider_info)))
    elif data_level == "ods":
      pass
    elif data_level == "snap":
      pass
    else:
      print("元数据表字段data_level未指定对应值：spider、ods、snap")
