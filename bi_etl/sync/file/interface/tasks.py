# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: tasks.py
# @Software: PyCharm
# function info：定义celery任务

from __future__ import absolute_import, unicode_literals
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tylerscope import app
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_oe_async_status_content_content
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_oe_save_exception_file
import time
import socket

hostname = socket.gethostname()

#定义oe任务状态
@app.task
def get_oe_async_tasks_status(AsyncNotemptyFile="",AsyncEmptyFile="",AsyncStatusExceptionFile="",ExecData=""):
    account_id = ExecData[0]
    set_true = True
    n = 1
    print("执行子账户：%s"%(account_id))
    while set_true:
      try:
         set_oe_async_status_content_content(ExecData=ExecData,AsyncNotemptyFile=AsyncNotemptyFile,AsyncEmptyFile=AsyncEmptyFile)
         set_true = False
      except Exception as e:
         if n > 3:
            print("异常子账户：%s" % (account_id))
            get_oe_save_exception_file(ExecData=ExecData,AsyncNotemptyFile=AsyncNotemptyFile,AsyncStatusExceptionFile=AsyncStatusExceptionFile)
            set_true = False
         else:
          time.sleep(2)
      n = n + 1
