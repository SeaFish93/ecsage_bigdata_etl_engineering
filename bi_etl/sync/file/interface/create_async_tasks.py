import math
import requests
import sys
import os
import time
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session

etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
#创建任务
def oe_create_tasks(MysqlSession="",SqlList="",AsyncTaskFile="",AsyncTaskExceptionFile="",AsyncTask="",ExecDate=""):
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
                                   MysqlSession = MysqlSession,ExecDate=ExecDate
                                   )
           etl_thread.start()
           import time
           time.sleep(2)
           th.append(etl_thread)
        for etl_th in th:
            etl_th.join()
        insert_sql = """
          load data local infile '%s' into table metadb.oe_async_task_interface fields terminated by ' ' lines terminated by '\\n' (media_type,token_data,service_code,account_id,task_id,task_name)
        """%(AsyncTaskFile)
        etl_md.local_file_to_mysql(sql=insert_sql)

def oe_run_create_task(MysqlSession="",Sql="",ThreadName="",AsyncTaskFile="",AsyncTaskExceptionFile="",ExecDate="",arg=None):
    account_id = ""
    token_data = ""
    service_code = ""
    num = 1
    nums = 1
    if arg is not None:
       Sql = arg["Sql"]
       ThreadName = arg["ThreadName"]
       AsyncTaskFile = arg["AsyncTaskFile"]
       AsyncTaskExceptionFile = arg["AsyncTaskExceptionFile"]
       MysqlSession = arg["MysqlSession"]
       ExecDate = arg["ExecDate"]
       ok, data_list = MysqlSession.get_all_rows_thread(Sql)
       print("线程：%s,长度：%s,=================================="%(ThreadName,len(data_list)))
       for data in data_list:
           account_id = data[1]
           os.system("""echo "%s">>/tmp/account.log """%(account_id))
           service_code = data[3]
           token_data = data[4]
           media_type = data[2]
           set_true = True
           n = 1
           while set_true:
             try:
               set_async_tasks(MediaType=media_type,ServiceCode=service_code, AccountId=account_id, ThreadName=ThreadName, Num=num,Token=token_data,AsyncTaskFile=AsyncTaskFile,Nums=nums,ExecDate=ExecDate)
               set_true = False
             except Exception as e:
               if n > 3:
                  os.system("""echo "%s %s %s %s %s">>%s """%(service_code,token_data,service_code,account_id,"%s%s" % (ThreadName, num),AsyncTaskExceptionFile))
                  set_true = False
               else:
                  time.sleep(2)
             n = n + 1
             nums = nums + 1

def set_async_tasks(MediaType="",ServiceCode="",AccountId="",ThreadName="",Num="",Token="",AsyncTaskFile="",Nums="",ExecDate=""):
    open_api_domain = "https://ad.toutiao.com"
    path = "/open_api/2/async_task/create/"
    url = open_api_domain + path
    params = {
        "advertiser_id": AccountId,
        "task_name": "%s%s" % (ThreadName, Num),
        "task_type": "REPORT",
        "force": "true",
        "task_params": {
            "start_date": ExecDate,
            "end_date": ExecDate,
            "group_by": ["STAT_GROUP_BY_CAMPAIGN_ID"]
        }
    }
    headers = {
        'Content-Type': "application/json",
        'Access-Token': Token,
        'Connection': "close"
    }
    if Nums%2 == 0:
       time.sleep(2)
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
    exec_date = sys.argv[6]
    os.system("""rm -f %s """ % (async_task_exception_file))
    os.system("""rm -f %s """ % (async_task_file))

    oe_create_tasks(MysqlSession=etl_md, SqlList=sqls_list, AsyncTaskFile=async_task_file,AsyncTaskExceptionFile=async_task_exception_file, AsyncTask=async_task,ExecDate=exec_date)
