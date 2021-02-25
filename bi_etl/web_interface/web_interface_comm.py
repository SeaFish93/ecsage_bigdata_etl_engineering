# -*- coding: utf-8 -*-
# @Time    : 2019/1/23 19:30
# @Author  : wangsong
# @FileName: web_interface_comm.py
# @Software: PyCharm
# function info：处理报表接口请求公共方法
import importlib
import time

#替换参数方法
def replace_placeholder(Sql=""):

    return Sql

# 获取接口元数据信息
def get_interface_meta(MysqlSession="",InterfaceId=""):
    sql = """
          select interface_id
                 ,interface_name
                 ,project
                 ,interface_item
                 ,is_page
                 ,engine_type
                 ,engine_handle
          from metadb.web_interface_info
          where status = 1
            and interface_id = '%s'
        """ % (InterfaceId)
    ok, data = MysqlSession.get_all_rows(sql)
    return ok, data

#获取处理接口对应脚本
def get_interface_module(Project="",Item="", EngineType="",Interface=""):
    #Project：所属项目名称,Item：所属项目的下一级包名,EngineType：计算引擎
    code = 0
    try:
        pkg = ".%s.%s.%s" % (Item, EngineType, Interface)
        module = importlib.import_module(pkg, package=Project)
        return code,module
    except Exception as e:
        print("获取不到文件错误日志：%s"%(str(e)))
        print("文件参数：%s,%s,%s,%s"%(Project,Item, EngineType,Interface))
        code = 40001
        module = "获取处理接口文件异常，读取不到对应接口文件！"
        return code,module

def get_interface_requset_param_exception(code="",msg="",request_begin_time="",request_end_time=""):
    interface_data = {}
    interface_data["code"] = code
    interface_data["msg"] = msg
    interface_data["data"] = {}
    interface_data["data"]["list"] = []
    interface_data["request_begin_time"] = request_begin_time
    interface_data["request_end_time"] = request_end_time
    return interface_data