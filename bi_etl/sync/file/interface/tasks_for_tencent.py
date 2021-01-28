# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: tasks.py
# @Software: PyCharm
# function info：定义celery任务

from __future__ import absolute_import, unicode_literals
from ecsage_bigdata_etl_engineering.common.base.get_config import Conf
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tylerscope import app
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_not_page
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import set_pages
import os
import time
import socket
conf = Conf().conf
hostname = socket.gethostname()

#定义oe任务创建
@app.task(rate_limit='5/m')
def get_test(**kwargs):
    now = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    return kwargs

#处理不分页
@app.task(rate_limit='200/m')
def get_not_page(UrlPath="",ParamJson="",ServiceCode="",Token="",ReturnAccountId="",TaskFlag="",DataFileDir="",DataFile="",TaskExceptionFile="",ArrayFlag="",TargetFlag="oe"):
    set_true = True
    n = 0
    while set_true:
      code = set_not_page(UrlPath=UrlPath,ParamJson=ParamJson,ServiceCode=ServiceCode,Token=Token
                          ,DataFileDir=DataFileDir,DataFile=DataFile,ReturnAccountId=ReturnAccountId,ArrayFlag=ArrayFlag,TargetFlag=TargetFlag)
      if TargetFlag == "tc":
          sucess_code=[ int(x) for x in conf.get("Tc_Code", "sucess_code").split(",")]
      else:
          sucess_code=[ int(x) for x in conf.get("Oe_Code", "sucess_code").split(",")]
      if int(code) in sucess_code:
          set_true = False
      else:
          if n > 2:
            print("处理不分页异常：%s,%s"%(ReturnAccountId,ServiceCode))
            for i in range(100):
              status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ",""),ServiceCode,str(ReturnAccountId).replace(" ",""), TaskFlag,Token,int(code), TaskExceptionFile + ".%s" % hostname))
              if int(status) == 0:
                  break;
            set_true = False
          else:
            time.sleep(5)
      n = n + 1

#处理分页
@app.task(rate_limit='200/m')
def get_pages(UrlPath="",ParamJson="",ServiceCode="",Token="",DataFileDir=""
              ,DataFile="",ReturnAccountId="",TaskFlag="",PageTaskFile="",TaskExceptionFile="",Pagestyle="",ArrayFlag="",TargetFlag="oe"):
    set_true = True
    n = 0
    while set_true:
      code = set_pages(UrlPath=UrlPath,ParamJson=ParamJson,Token=Token,
                            ServiceCode=ServiceCode,DataFileDir=DataFileDir,
                            DataFile=DataFile,ReturnAccountId=ReturnAccountId,
                            TaskFlag=TaskFlag,PageTaskFile=PageTaskFile,Pagestyle=Pagestyle,ArrayFlag=ArrayFlag,TargetFlag=TargetFlag
                           )
      if TargetFlag == "tc":
          sucess_code=[ int(x) for x in conf.get("Tc_Code", "sucess_code").split(",")]
      else:
          sucess_code=[ int(x) for x in conf.get("Oe_Code", "sucess_code").split(",")]
      print(sucess_code)
      if int(code) in sucess_code:
          set_true = False
      else:
          if n > 2:
            print("异常分页：%s,%s"%(ReturnAccountId,ServiceCode))
            for i in range(100):
              status = os.system("""echo "%s %s %s %s %s %s %s">>%s """ % (UrlPath, str(ParamJson).replace(" ", ""), ServiceCode, str(ReturnAccountId).replace(" ", ""), TaskFlag,Token,int(code),TaskExceptionFile + ".%s" % hostname))
              if int(status) == 0:
                 break;
            set_true = False
          else:
            time.sleep(5)
      n = n + 1
