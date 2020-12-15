import math
import requests
import sys
import os
import time
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session

etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
#创建任务
def oe_create_tasks(MysqlSession="",SqlList="",AsyncTaskFile="",AsyncTaskExceptionFile="",AsyncTask="",ExecDate="",GroupBy="",Fields=""):
    sql_list = eval(SqlList)
    group_by = GroupBy.split(",")
    if Fields == "" or Fields is None or len(Fields) == 0:
        fields = []
    else:
        fields = Fields.split(",")
    os.system("""echo "%s,%s">>/tmp/datadata.log """%(group_by,fields))
    if sql_list is not None and len(sql_list) > 0:
        i = 0
        th = []
        for sql in sql_list:
           i = i + 1
           etl_thread = EtlThread(thread_id=i, thread_name="%s%d" % (AsyncTask,i),
                                   my_run=oe_run_create_task,
                                   Sql = sql,ThreadName="%s%d" % (AsyncTask,i),
                                   AsyncTaskFile=AsyncTaskFile,AsyncTaskExceptionFile=AsyncTaskExceptionFile,
                                   MysqlSession = MysqlSession,ExecDate=ExecDate,GroupBy=group_by,Fields=fields
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

def oe_run_create_task(MysqlSession="",Sql="",ThreadName="",AsyncTaskFile="",AsyncTaskExceptionFile="",ExecDate="",GroupBy="",Fields="",arg=None):
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
       GroupBy = arg["GroupBy"]
       Fields = arg["Fields"]
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
               os.system("""echo "%s">>/tmp/account.log"""%(account_id))
               set_async_tasks(MediaType=media_type,ServiceCode=service_code, AccountId=account_id, ThreadName=ThreadName, Num=num,Token=token_data,AsyncTaskFile=AsyncTaskFile,Nums=nums,ExecDate=ExecDate,GroupBy=GroupBy,Fields=Fields)
               set_true = False
             except Exception as e:
               if n > 3:
                  os.system("""echo "%s %s %s %s %s %s">>%s """ % (media_type,token_data, service_code, account_id, 0, 9999,AsyncTaskFile))
                  os.system("""echo "%s %s %s %s %s">>%s """%(service_code,token_data,service_code,account_id,"%s%s" % (ThreadName, num),AsyncTaskExceptionFile))
                  print("""异常：%s %s %s"""%(token_data,service_code,account_id))
                  set_true = False
               else:
                  time.sleep(2)
             n = n + 1
             nums = nums + 1

def set_async_tasks(MediaType="",ServiceCode="",AccountId="",ThreadName="",Num="",Token="",AsyncTaskFile="",Nums="",ExecDate="",GroupBy="",Fields=""):
    resp_data = set_tasks_status(AccountId=AccountId, ThreadName=ThreadName, Num=Num, Nums=Nums, Fields=Fields, ExecDate=ExecDate, Token=Token, GroupBy=GroupBy)
    data = resp_data["code"]
    if data == 40105:
       token = get_account_token(ServiceCode=ServiceCode)
       resp_data = set_tasks_status(AccountId=AccountId, ThreadName=ThreadName, Num=Num, Nums=Nums, Fields=Fields, ExecDate=ExecDate, Token=token, GroupBy=GroupBy)
    task_id = resp_data["data"]["task_id"]
    task_name = resp_data["data"]["task_name"]
    os.system("""echo "%s %s %s %s %s %s">>%s """ % (MediaType,Token, ServiceCode, AccountId, task_id, task_name,AsyncTaskFile))

def get_account_token(ServiceCode=""):
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
            set_true = False
        else:
            time.sleep(2)
      n = n + 1
    return token_data

def set_tasks_status(AccountId="",ThreadName="",Num="",Nums="",Fields="",ExecDate="",Token="",GroupBy=""):
    open_api_domain = "https://ad.toutiao.com"
    path = "/open_api/2/async_task/create/"
    url = open_api_domain + path
    if Fields is None or len(Fields) == 0:
        params = {
            "advertiser_id": AccountId,
            "task_name": "%s%s" % (ThreadName, Num),
            "task_type": "REPORT",
            "force": "true",
            "task_params": {"start_date": ExecDate,
                            "end_date": ExecDate,
                            "group_by": GroupBy
                            }
        }
    else:
        params = {
            "advertiser_id": AccountId,
            "task_name": "%s%s" % (ThreadName, Num),
            "task_type": "REPORT",
            "force": "true",
            "task_params": {"start_date": ExecDate,
                            "end_date": ExecDate,
                            "group_by": GroupBy,
                            "fields": Fields
                            }
        }
    headers = {
        'Content-Type': "application/json",
        'Access-Token': Token,
        'Connection': "close"
    }
    if Nums % 2 == 0:
        time.sleep(2)
    resp = requests.post(url, json=params, headers=headers)
    resp_data = resp.json()
    return resp_data

if __name__ == '__main__':
    media_type = sys.argv[1]
    async_task = sys.argv[2]
    sqls_list = sys.argv[3]
    async_task_file = sys.argv[4]
    async_task_exception_file = sys.argv[5]
    exec_date = sys.argv[6]
    group_by = sys.argv[7]
    fields = sys.argv[8]
    #os.system("""rm -f %s """ % (async_task_exception_file))
    #os.system("""rm -f %s """ % (async_task_file))

    oe_create_tasks(MysqlSession=etl_md, SqlList=sqls_list, AsyncTaskFile=async_task_file,AsyncTaskExceptionFile=async_task_exception_file, AsyncTask=async_task,ExecDate=exec_date,GroupBy=group_by,Fields=fields)
