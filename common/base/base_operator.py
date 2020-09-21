# -*- coding: utf-8 -*-
# @Time    : 2020/5/26 15:28
# @Author  : wangsong12
# @FileName: base_operator.py
# @Software: PyCharm

import base64

class BaseDB(object):
    def __init__(self, port=None, host=None, user=None, password=None, default_db="", timeout=60):
        if port is None or host is None:
            print("host/port is not None.")
            raise Exception()
        self.host = host
        self.port = int(port)
        self.user = user
        self.default_db = default_db
        # 没有给定password时，给一个默认值，以防下面报错
        password = "WmFHVm1rWVhWc2RBPT0s" if password is None else password
        try:
            #self.password = str(base64.decodestring(bytes(password, 'utf-8'))).replace("b'","").replace("'","")
            self.password = str(base64.b64decode(password).decode("utf-8"))
        except Exception as e:
            self.password = ""
            print("获取密码出错。" + e)
            raise Exception()
        self.timeout = timeout

    def get_connect(self):
        return

    def set_host(self, host):
        self.host = host

    def get_host(self):
        return self.host

    def set_port(self, port):
        self.port = port

    def get_port(self):
        return self.port

    def set_user(self, user):
        self.user = user

    def get_user(self):
        return self.user

    def set_password(self, password):
        self.password = password

    def get_password(self):
        return self.password
