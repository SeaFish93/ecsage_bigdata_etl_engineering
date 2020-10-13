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

def exec_interface_data_curl(URL="",Data={},File=""):
    headers = {'Content-Type': "application/json"}
    try:
        response = requests.post(URL, data=json.dumps(Data), headers=headers)
        while True:
          is_md5 = os.path.exists("%s.md5"%(File))
          if is_md5:
            is_file = os.path.exists("%s"%(File))
            if is_file:
               file_md5 = os.popen("md5sum %s"%(File))
               file_md5_value = file_md5.read().split()[0]
               md5_file_md5 = os.popen("cat %s.md5"%(File))
               md5_file_md5_value = md5_file_md5.read().split()[0]
               if file_md5_value != md5_file_md5_value:
                   msg = get_create_dag_alert(FileName="%s" % (os.path.basename(__file__)),
                                              Log="执行数据接口采集出现异常！！！",
                                              Developer="工程维护")
                   set_exit(LevelStatu="red", MSG=msg)
               else:
                   exit(0)
          time.sleep(120)
        return response.status_code
    except Exception as e:
        msg = get_create_dag_alert(FileName="%s" % (os.path.basename(__file__)), Log="执行数据接口采集出现异常！！！",
                                   Developer="工程维护")
        set_exit(LevelStatu="red", MSG=msg)
        return None
