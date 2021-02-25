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
from ecsage_bigdata_etl_engineering.bi_etl.web_interface.web_interface_comm import get_interface_requset_param_exception

def execute(InterfaceParamsInfo=""):
    request_begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    interface_data = {}
    mysql_session = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
    interface_info = InterfaceParamsInfo
    try:
      interface_id = interface_info["interface_id"]
    except Exception as e:
        return get_interface_requset_param_exception(code=30003,msg="NO_PARAM_INTERFACE_ID_ERROR",request_begin_time=request_begin_time,request_end_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    ok,data = get_interface_meta(MysqlSession=mysql_session,InterfaceId=interface_id)
    if data is None or len(data) == 0:
        return get_interface_requset_param_exception(code=30002,msg="PARAM_INTERFACE_ID_ERROR",request_begin_time=request_begin_time,request_end_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    #定义元数据
    project = data[0][2]
    item = data[0][3]
    is_page = data[0][4]
    engine_type = data[0][5]
    engine_handle = data[0][6]
    #判断是否分页，若是，则请求参数必须包含：page、page_size两个参数
    if int(is_page) == 1:
       try:
         page = interface_info["page"]
       except Exception as e:
           return get_interface_requset_param_exception(code=30004,msg="NO_PARAM_PAGE_ERROR",request_begin_time=request_begin_time,request_end_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
       try:
         page_size = interface_info["page_size"]
       except Exception as e:
           return get_interface_requset_param_exception(code=30005,msg="NO_PARAM_PAGE_SIZE_ERROR",request_begin_time=request_begin_time,request_end_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    #获取对应接口文件
    code,module = get_interface_module(Project=project,Item=item, EngineType=engine_type,Interface=interface_id)
    if int(code) == 40001:
        return get_interface_requset_param_exception(code=40001,msg="SYS_DATA_ERROR",request_begin_time=request_begin_time,request_end_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    if engine_type is not None and len(engine_type) > 0:
       try:
          exec_session = set_db_session(SessionType=engine_type, SessionHandler=engine_handle)
          if exec_session is None:
              return get_interface_requset_param_exception(code=40004,msg="SYS_DATA_ERROR",request_begin_time=request_begin_time,request_end_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
       except Exception as e:
           print("连接计算引擎报错：%s" % (str(e)))
           return get_interface_requset_param_exception(code=40003,msg="SYS_DATA_ERROR",request_begin_time=request_begin_time,request_end_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    else:
        return get_interface_requset_param_exception(code=40002,msg="SYS_DATA_ERROR",request_begin_time=request_begin_time,request_end_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    # 获取执行sql
    try:
      InterfaceParamsInfo["is_page"] = is_page
      exec_sql = module.SQL(InterfaceParamsInfo=InterfaceParamsInfo)
    except Exception as e:
        print("处理接口组装SQL异常错误日志：%s" % (str(e)))
        return get_interface_requset_param_exception(code=40005,msg="SYS_DATA_ERROR",request_begin_time=request_begin_time,request_end_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    #执行sql
    total_page = 0
    if int(is_page) == 0:
       ok, results,columns_list = exec_session.get_all_rows(sql=exec_sql)
    else:
       #分页
       #获取总条数
       query_total_sql = """%s %s %s"""%("select count(1) from(",exec_sql," ) aa")
       query_total_ok, query_total, columns_list = exec_session.get_all_rows(sql=query_total_sql)
       if query_total_ok is False:
           return get_interface_requset_param_exception(code=40009, msg="SYS_DATA_ERROR",request_begin_time=request_begin_time,request_end_time=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime()))
       #获取总页数
       total_page = query_total/interface_info["page_size"]
       if total_page % interface_info["page_size"] > 0:
           total_page = total_page + 1
       #分页数据
       offset = int(interface_info["page_size"]) * int(interface_info["page"]) - int(interface_info["page_size"])
       limit = " limit %s offset %s"%(int(interface_info["page_size"]),offset)
       query_total_sql = """%s %s""" % (exec_sql, limit)
       ok, results, columns_list = exec_session.get_all_rows(sql=query_total_sql)
    if ok is False:
        return get_interface_requset_param_exception(code=40006,msg="SYS_DATA_ERROR",request_begin_time=request_begin_time,request_end_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    for columns in columns_list:
        if " " in columns:
            print("SQL字段名含有空格异常，请使用字段别名：%s" % (columns))
            return get_interface_requset_param_exception(code=40007,msg="SYS_DATA_ERROR",request_begin_time=request_begin_time,request_end_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            break;
    keys = columns_list
    try:
      #元组转换json格式
      list_json = [dict(zip(keys, item)) for item in results]
      data = list_json#json.dumps(list_json)
      interface_data["code"] = 0
      interface_data["msg"] = "OK"
      interface_data["data"] = {}
      interface_data["data"]["list"] = data
      interface_data["request_begin_time"] = request_begin_time
      interface_data["request_end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    except Exception as e:
        print("生成数据json格式异常错误：%s" % (str(e)))
        return get_interface_requset_param_exception(code=40008,msg="SYS_DATA_ERROR",request_begin_time=request_begin_time,request_end_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    return interface_data
