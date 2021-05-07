# -*- coding: utf-8 -*-
# @Time    : 2019/1/23 19:30
# @Author  : wangsong
# @FileName: get_passwd.py
# @Software: PyCharm

import base64

passwd = "hive@Yk"
bytes_passwd = passwd.encode("utf-8")
str_passwd = base64.b64encode(bytes_passwd)
print(str_passwd)
