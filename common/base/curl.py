# -*- coding: utf-8 -*-
# @Time    : 2020/01/06 18:04
# @Author  : wangsong
# @FileName: curl.py
# @Software: PyCharm
# function info：调用接口

import requests
import json
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_create_dag_alert
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit
import os
import time

def exec_interface_data_curl(URL="",Data={},File="",DataJsonRequest=""):
    headers = {'Content-Type': "application/json","Connection":"close"}
    param_md5 = ""
    try:
        requests.post(URL, data=json.dumps(Data), headers=headers)
        #########exit_while = True
        #########data_file = File
        #########param_file = "%s.param"%(File)
        #########md5_file = "%s.md5"%(File)
        #########while exit_while:
        #########  is_md5 = os.path.exists(md5_file)
        #########  is_md5 = True
        #########  if is_md5:
        #########    is_file = os.path.exists(data_file)
        #########    is_file = True
        #########    if is_file:
        #########       #file_md5 = os.popen("md5sum %s"%(data_file))
        #########       #file_md5_value = file_md5.read().split()[0]
        #########       #md5_file_md5 = os.popen("cat %s"%(md5_file))
        #########       #md5_file_md5_value = ""##########md5_file_md5.read().split()[0]
        #########       file_md5_value = "md5"
        #########       md5_file_md5_value = "md5"
        #########       #print("MD5：【%s,%s】"%(file_md5_value,md5_file_md5_value))
        #########       if file_md5_value != md5_file_md5_value:
        #########           msg = get_create_dag_alert(FileName="%s" % (os.path.basename(__file__)),
        #########                                      Log="执行数据接口采集生成数据文件md5对不上！！！",
        #########                                      Developer="工程维护")
        #########           set_exit(LevelStatu="red", MSG=msg)
        #########       else:
        #########           #print("数据文件已生成且MD5已对上：【%s,%s】"%(data_file,md5_file))
        #########           dir_json = Data
        #########           dir_json["ec_fn"] = ""
        #########           #param_md5 = os.popen("""echo '%s'|md5sum|awk '{print $1}'"""%(DataJsonRequest)).read().split()[0]
        #########           #os.system("""echo '%s'>>%s"""%(DataJsonRequest,param_file))
        #########           exit_while = False
        #########    else:
        #########        msg = get_create_dag_alert(FileName="%s" % (os.path.basename(__file__)),
        #########                                   Log="执行数据接口采集生成数据文件出现异常！！！",
        #########                                   Developer="工程维护")
        #########        set_exit(LevelStatu="red", MSG=msg)
        #########  else:
        #########    #print("等待数据文件md5生成：【%s】"%(md5_file))
        #########    exit_while = False
        #########    #time.sleep(120)
        #########return param_md5,param_file
    except Exception as e:
        msg = ""
        set_exit(LevelStatu="red", MSG=msg)
        #return None
