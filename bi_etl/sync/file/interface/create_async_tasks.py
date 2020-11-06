import math
import requests
import sys
import os
import time
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
import json
import ast

etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
#创建任务
def oe_create_tasks(MysqlSession="",SqlList="",AsyncTaskFile="",AsyncTaskExceptionFile="",AsyncTask=""):
    sql_list = eval(SqlList)
    if sql_list is not None and len(sql_list) > 0:
        i = 0
        th = []
        for sql in sql_list:
           i = i + 1
           etl_thread = EtlThread(thread_id=i, thread_name="%s%d" % (AsyncTask,i),
                                   my_run=oe_run_create_task,
                                   Sql = sql,ThreadName="%s%d" % (AsyncTask,i),
                                   AsyncTaskFile=AsyncTaskFile,AsyncTaskExceptionFile=AsyncTaskExceptionFile,
                                   MysqlSession = MysqlSession
                                   )
           etl_thread.start()
           import time
           time.sleep(2)
           th.append(etl_thread)
        for etl_th in th:
            etl_th.join()
        #insert_sql = """
        #  load data local infile '%s' into table metadb.oe_async_task_interface fields terminated by ' ' lines terminated by '\\n' (media_type,token_data,service_code,account_id,task_id,task_name)
        #"""%(AsyncTaskFile)
        #etl_md.execute_sql("""delete from metadb.oe_async_task_interface where media_type=%s and service_code='%s' """%(MediaType,ServiceCode))
        #etl_md.local_file_to_mysql(sql=insert_sql)

def oe_run_create_task(MysqlSession="",Sql="",ThreadName="",AsyncTaskFile="",AsyncTaskExceptionFile="",arg=None):
    account_id = ""
    token_data = ""
    service_code = ""
    num = 1
    if arg is not None:
       Sql = arg["Sql"]
       ThreadName = arg["ThreadName"]
       AsyncTaskFile = arg["AsyncTaskFile"]
       AsyncTaskExceptionFile = arg["AsyncTaskExceptionFile"]
       MysqlSession = arg["MysqlSession"]
       ok, data_list = MysqlSession.get_all_rows_thread(Sql)
       print("线程：%s,长度：%s,=================================="%(ThreadName,len(data_list)))
       for data in data_list:
           account_id = data[1]
           service_code = data[3]
           token_data = data[4]
           media_type = data[2]
           set_true = True
           n = 1
           while set_true:
             try:
               set_async_tasks(MediaType=media_type,ServiceCode=service_code, AccountId=account_id, ThreadName=ThreadName, Num=num,Token=token_data,AsyncTaskFile=AsyncTaskFile)
               set_true = False
             except Exception as e:
               if n > 3:
                  os.system("""echo "%s %s %s %s">>%s """%(service_code,token_data,service_code,account_id,AsyncTaskExceptionFile))
                  set_true = False
               else:
                  time.sleep(2)
             n = n + 1

def set_async_tasks(MediaType="",ServiceCode="",AccountId="",ThreadName="",Num="",Token="",AsyncTaskFile=""):
    open_api_domain = "https://ad.toutiao.com"
    path = "/open_api/2/async_task/create/"
    url = open_api_domain + path
    set_true = True
    n = 0
    user_agent = """Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36"""
    user_agent_list = [
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 ',
        'Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
         'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11'
    ]
    user_agent = random.choice(user_agent_list)
    params = {
        "advertiser_id": AccountId,
        "task_name": "%s%s" % (ThreadName, Num),
        "task_type": "REPORT",
        "force": "true",
        "task_params": {
            "start_date": "2020-11-05",
            "end_date": "2020-11-05",
            "group_by": ["STAT_GROUP_BY_CAMPAIGN_ID"]
        }
    }
    headers = {
        'Content-Type': "application/json",
        'Access-Token': Token,
        'Connection': "close"
    }
    resp = requests.post(url, json=params, headers=headers)
    resp_data = resp.json()
    task_id = resp_data["data"]["task_id"]
    task_name = resp_data["data"]["task_name"]
    os.system("""echo "%s %s %s %s %s %s">>%s """ % (MediaType,Token, ServiceCode, AccountId, task_id, task_name,AsyncTaskFile))

if __name__ == '__main__':
    media_type = sys.argv[1]
    async_task = sys.argv[2]
    sqls_list = sys.argv[3]
    async_task_file = sys.argv[4]
    async_task_exception_file = sys.argv[5]
    oe_create_tasks(MysqlSession=etl_md, SqlList=sqls_list, AsyncTaskFile=async_task_file,AsyncTaskExceptionFile=async_task_exception_file, AsyncTask=async_task)