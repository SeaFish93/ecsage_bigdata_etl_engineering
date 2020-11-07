# -*- coding: utf-8 -*-
# @Time    : 2020/01/06 18:04
# @Author  : wangsong
# @FileName: curl.py
# @Software: PyCharm
# function info：获取异步状态

from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
import requests
import os
import sys
import time

etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
def get_async_status(MediaType="",SqlList="",AsyncNotemptyFile="",AsyncEmptyFile="",AsyncStatusExceptionFile=""):
    sql_list = eval(SqlList)
    if sql_list is not None and len(sql_list) > 0:
        i = 0
        th = []
        for sql in sql_list:
                i = i + 1
                etl_thread = EtlThread(thread_id=i, thread_name="%s%d" % (MediaType,i),
                                   my_run=get_download_content,
                                   Sql = sql,AsyncNotemptyFile=AsyncNotemptyFile,AsyncEmptyFile=AsyncEmptyFile,
                                   AsyncStatusExceptionFile=AsyncStatusExceptionFile,MediaType=MediaType
                                   )
                etl_thread.start()
                time.sleep(2)
                th.append(etl_thread)
        for etl_th in th:
            etl_th.join()
        #记录有效子账户
        ####insert_sql = """
        ####   load data local infile '%s' into table metadb.oe_valid_account_interface fields terminated by ' ' lines terminated by '\\n' (account_id,media_type,service_code,token_data)
        #### """ % (AsyncNotemptyFile)
        ####etl_md.execute_sql("""delete from metadb.oe_valid_account_interface where media_type=%s and service_code='%s' """ % (MediaType, ServiceCode))
        ####etl_md.local_file_to_mysql(sql=insert_sql)

        print("the end!!!!!")

def get_download_content(Sql="",AsyncNotemptyFile="",AsyncEmptyFile="",AsyncStatusExceptionFile="",MediaType="",arg=None):
  if arg is not None:
    Sql = arg["Sql"]
    AsyncNotemptyFile = arg["AsyncNotemptyFile"]
    AsyncEmptyFile = arg["AsyncEmptyFile"]
    AsyncStatusExceptionFile = arg["AsyncStatusExceptionFile"]
    MediaType = arg["MediaType"]
    ok,datas = etl_md.get_all_rows(Sql)
    for data in datas:
        token = data[0]
        service_code = data[1]
        account_id = data[2]
        task_id = data[3]
        task_name = data[4]
        try:
          set_download_content(MediaType=MediaType,ServiceCode=service_code,AccountId=account_id, TaskId=task_id, Token=token,AsyncNotemptyFile=AsyncNotemptyFile,AsyncEmptyFile=AsyncEmptyFile)
        except Exception as e:
          set_true = True
          n = 1
          while set_true:
              time.sleep(2)
              try:
                  set_download_content(MediaType=MediaType,ServiceCode=service_code,AccountId=account_id, TaskId=task_id, Token=token,AsyncNotemptyFile=AsyncNotemptyFile,AsyncEmptyFile=AsyncEmptyFile)
                  set_true = False
              except Exception as e:
                  if n > 3:
                      os.system("""echo "%s %s %s %s %s">>%s """ % (token, service_code, account_id,task_id,task_name,AsyncStatusExceptionFile))
                      set_true = False
              n = n + 1

def set_download_content(MediaType="",ServiceCode="",AccountId="",TaskId="",Token="",AsyncNotemptyFile="",AsyncEmptyFile=""):
    open_api_domain = "https://ad.toutiao.com"
    path = "/open_api/2/async_task/get/"
    url = open_api_domain + path
    params = {
        "advertiser_id": AccountId,
        "filtering": {
            "task_ids": [TaskId]
        }
    }
    headers = {
        'Content-Type': "application/json",
        'Access-Token': Token
    }
    resp = requests.get(url, json=params, headers=headers)
    resp_data = resp.json()
    file_size = resp_data["data"]["list"][0]["file_size"]
    task_status = resp_data["data"]["list"][0]["task_status"]
    print("文件大小：%s，任务状态：%s"%(file_size,task_status))
    if int(file_size) <= 12:
        os.system("""echo "%s %s %s">>%s """%(AccountId,TaskId,Token,AsyncEmptyFile))
    else:
        os.system("""echo "%s %s %s %s">>%s """ % (AccountId, MediaType,ServiceCode, Token, AsyncNotemptyFile))


if __name__ == '__main__':
    media_type = sys.argv[1]
    sqls_list = sys.argv[2]
    async_notempty_file = sys.argv[3]
    async_empty_file = sys.argv[4]
    async_status_exception_file = sys.argv[5]
    get_async_status(MediaType=media_type,SqlList=sqls_list,AsyncNotemptyFile=async_notempty_file,AsyncEmptyFile=async_empty_file,AsyncStatusExceptionFile=async_status_exception_file)
