# -*- coding: utf-8 -*-
# @Time    : 2019/1/23 19:30
# @Author  : wangsong
# @FileName: exec_interface_script.py
# @Software: PyCharm
# function info：处理报表接口请求

import time
import json
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.bi_etl.web_interface.web_interface_comm import get_interface_meta
from ecsage_bigdata_etl_engineering.bi_etl.web_interface.web_interface_comm import get_interface_module


def execute(InterfaceParamsInfo=""):
    request_begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    interface_data = {}
    mysql_session = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
    interface_info = InterfaceParamsInfo
    try:
      interface_id = interface_info["interface_id"]
    except Exception as e:
        interface_data["code"] = 30003
        interface_data["msg"] = "NO_PARAM_INTERFACE_ID_ERROR"
        interface_data["data"] = {}
        interface_data["data"]["list"] = []
        interface_data["request_begin_time"] = request_begin_time
        interface_data["request_end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        return interface_data
    ok,data = get_interface_meta(MysqlSession=mysql_session,InterfaceId=interface_id)
    if data is None or len(data) == 0:
        interface_data["code"] = 30002
        interface_data["msg"] = "PARAM_INTERFACE_ID_ERROR"
        interface_data["data"] = {}
        interface_data["data"]["list"] = []
        interface_data["request_begin_time"] = request_begin_time
        interface_data["request_end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        return interface_data
    #定义元数据
    project = data[0][2]
    item = data[0][3]
    is_page = data[0][4]
    engine_type = data[0][5]
    engine_handle = data[0][6]
    #获取对应接口文件
    code,module = get_interface_module(Project=project,Item=item, EngineType=engine_type,Interface=interface_id)
    if int(code) == 40001:
        interface_data["code"] = 40001
        interface_data["msg"] = "SYS_DATA_ERROR"
        interface_data["data"] = {}
        interface_data["data"]["list"] = []
        interface_data["request_begin_time"] = request_begin_time
        interface_data["request_end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        return interface_data
    if engine_type is not None and len(engine_type) > 0:
       try:
          exec_session = set_db_session(SessionType=engine_type, SessionHandler=engine_handle)
          if exec_session is None:
              interface_data["code"] = 40004
              interface_data["msg"] = "SYS_DATA_ERROR"
              interface_data["data"] = {}
              interface_data["data"]["list"] = []
              interface_data["request_begin_time"] = request_begin_time
              interface_data["request_end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
              return interface_data
       except Exception as e:
           interface_data["code"] = 40003
           interface_data["msg"] = "SYS_DATA_ERROR"
           interface_data["data"] = {}
           interface_data["data"]["list"] = []
           interface_data["request_begin_time"] = request_begin_time
           interface_data["request_end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
           return interface_data
    else:
        interface_data["code"] = 40002
        interface_data["msg"] = "SYS_DATA_ERROR"
        interface_data["data"] = {}
        interface_data["data"]["list"] = []
        interface_data["request_begin_time"] = request_begin_time
        interface_data["request_end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        return interface_data
    # 获取执行sql
    try:
      exec_sql = module.SQL(InterfaceParamsInfo=InterfaceParamsInfo)
    except Exception as e:
        interface_data["code"] = 40005
        interface_data["msg"] = "SYS_DATA_ERROR"
        interface_data["data"] = {}
        interface_data["data"]["list"] = []
        interface_data["request_begin_time"] = request_begin_time
        interface_data["request_end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        return interface_data
    #执行sql
    ok, results,columns_list = exec_session.get_all_rows(sql=exec_sql)
    if ok is False:
        interface_data["code"] = 40006
        interface_data["msg"] = "SYS_DATA_ERROR"
        interface_data["data"] = {}
        interface_data["data"]["list"] = []
        interface_data["request_begin_time"] = request_begin_time
        interface_data["request_end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        return interface_data
    for columns in columns_list:
        if " " in columns:
            interface_data["code"] = 40007
            interface_data["msg"] = "SYS_DATA_ERROR"
            interface_data["data"] = {}
            interface_data["data"]["list"] = []
            interface_data["request_begin_time"] = request_begin_time
            interface_data["request_end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            return interface_data
            break;
    keys = columns_list
    try:
      #元组转换json格式
      list_json = [dict(zip(keys, item)) for item in results]
      data = json.dumps(list_json, indent=2, ensure_ascii=False)
      interface_data["code"] = 0
      interface_data["msg"] = "OK"
      interface_data["data"] = {}
      interface_data["data"]["list"] = data
      interface_data["request_begin_time"] = request_begin_time
      interface_data["request_end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    except Exception as e:
        interface_data["code"] = 40008
        interface_data["msg"] = "SYS_DATA_ERROR"
        interface_data["data"] = {}
        interface_data["data"]["list"] = []
        interface_data["request_begin_time"] = request_begin_time
        interface_data["request_end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        return interface_data
    return interface_data
