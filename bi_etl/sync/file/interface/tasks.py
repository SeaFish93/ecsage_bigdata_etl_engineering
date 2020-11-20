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
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_oe_async_tasks_data
#from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.set_Logger import Logger
import time
import socket

hostname = socket.gethostname()

#定义oe任务状态
@app.task
def get_oe_async_tasks_status(AsyncNotemptyFile="",AsyncEmptyFile="",AsyncStatusExceptionFile="",ExecData="",ExecDate=""):
    account_id = ExecData[0]
    set_true = True
    n = 1
    print("执行子账户：%s"%(account_id))
    while set_true:
      try:
         set_oe_async_status_content_content(ExecData=ExecData,AsyncNotemptyFile=AsyncNotemptyFile,AsyncEmptyFile=AsyncEmptyFile,ExecDate=ExecDate)
         set_true = False
      except Exception as e:
         if n > 3:
            print("异常子账户：%s" % (account_id))
            get_oe_save_exception_file(ExecData=ExecData,AsyncNotemptyFile=AsyncNotemptyFile,AsyncStatusExceptionFile=AsyncStatusExceptionFile,ExecDate=ExecDate)
            set_true = False
         else:
          time.sleep(2)
      n = n + 1

#定义oe任务数据
@app.task
def get_oe_async_tasks_data(DataFile="",ExceptionFile="",ExecData="",ExecDate="",LogSession=""):
    logger = get_oe_async_tasks_data.get_logger(logfile ="%s/tasks.log"%(DataFile))
    account_id = ExecData[0]
    #log = Logger("""%s.%s"""% (DataFile,hostname),level='info')
    set_true = True
    n = 1
    print("执行子账户：%s"%(account_id))
    while set_true:
       code = set_oe_async_tasks_data(DataFile=DataFile,ExecData=ExecData,LogSession=logger)
       if code != 0:
         if n > 3:
            print("异常子账户：%s" % (account_id))
            get_oe_save_exception_file(ExecData=ExecData, AsyncNotemptyFile="",AsyncStatusExceptionFile=ExceptionFile,ExecDate=ExecDate)
            set_true = False
         else:
            time.sleep(2)
       else:
         set_true = False
       n = n + 1
