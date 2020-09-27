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

def exec_interface_data_curl(URL="",Data={}):
    headers = {'Content-Type': "application/json"}
    try:
        response = requests.post(URL, data=json.dumps(Data), headers=headers)
        if response.status_code == 404:
            msg = get_create_dag_alert(FileName="%s" % (os.path.basename(__file__)),
                                       Log="执行数据接口采集出现异常！！！",
                                       Developer="工程维护")
            set_exit(LevelStatu="red", MSG=msg)
        return response.status_code
    except Exception as e:
        msg = get_create_dag_alert(FileName="%s" % (os.path.basename(__file__)), Log="执行数据接口采集出现异常！！！",
                                   Developer="工程维护")
        set_exit(LevelStatu="red", MSG=msg)
        return None
