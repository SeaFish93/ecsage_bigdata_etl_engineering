# -*- coding: utf-8 -*-
# @Time    : 2019/1/23 19:30
# @Author  : wangsong
# @FileName: web_interface_comm.py
# @Software: PyCharm
# function info：处理报表接口请求公共方法
import importlib

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
        code = 40001
        module = "获取处理接口文件异常，读取不到对应接口文件！"
        return code,module
