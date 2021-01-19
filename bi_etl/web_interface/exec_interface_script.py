# -*- coding: utf-8 -*-
# @Time    : 2019/1/23 19:30
# @Author  : wangsong
# @FileName: exec_script_sql.py
# @Software: PyCharm
# function info：处理报表接口请求

import time
import datetime
import importlib


from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session


def execute(InterfaceInfo=""):
    mysql_session = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
    interface_info = InterfaceInfo
    interface_id = interface_info["interface_id"]
    #获取接口元数据信息
    sql = """
      select interface_id,interface_name,project,interface_item,is_page,engine_type
      from metadb.web_interface_info
      where status = 1
        and interface_id = '%s'
    """%(interface_id)
    ok,data = mysql_session.get_all_rows(sql)
    if data is None or len(data) == 0:
        code = 30002
        return None
    project = data[2]
    item = data[3]
    is_page = data[4]
    engine_type = data[5]
    #获取执行sql
    code,module = get_interface_module(Project=project,Item=item, EngineType=engine_type,Interface=interface_id)
    if int(code) == 40001:
        return None
    if engine_type == "hive":
        pass
    elif engine_type == "impala":
        pass
    else:
        code = 40002
        return None
    sql_dict = module.SQL["sql"]
    sql_str = replace_placeholder(sql_dict)
    #获取执行sql数据

#替换参数方法
def replace_placeholder(Sql=""):

    return Sql

#获取处理接口对应脚本
def get_interface_module(Project="",Item="", EngineType="",Interface=""):
    #Project：所属项目名称,Item：所属项目的下一级包名,EngineType：计算引擎
    code = 0
    try:
        pkg = ".%s.%s.%s" % (Item, EngineType, Interface)
        module = importlib.import_module(pkg, package=Project)
        return code,module
    except Exception as e:
        code = 40001
        module = "获取处理接口文件异常，读取不到对应接口文件！"
        return code,module
