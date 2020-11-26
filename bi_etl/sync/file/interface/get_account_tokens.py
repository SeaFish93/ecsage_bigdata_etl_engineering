# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: get_account_tokens.py
# @Software: PyCharm
# function info：用于获取接口子账户token

import requests
import os
import time
import datetime

def get_oe_account_token(ServiceCode=""):
    headers = {'Content-Type': "application/json", "Connection": "close"}
    token_url = """http://token.ecsage.net/service-media-token/rest/getToken?code=%s""" % (ServiceCode)
    set_true = True
    n = 1
    token_data = None
    while set_true:
      try:
        token_data_list = requests.post(token_url,headers=headers).json()
        token_data = token_data_list["t"]["token"]
        set_true = False
      except Exception as e:
        if n > 3:
            os.system("""echo "错误子账户token：%s %s">>/tmp/account_token_token.log """%(ServiceCode,datetime.datetime.now()))
            set_true = False
        else:
            time.sleep(2)
      n = n + 1
    return token_data

