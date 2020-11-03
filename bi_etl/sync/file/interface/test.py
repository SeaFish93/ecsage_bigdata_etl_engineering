import math
import requests

import os
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread

mysql_session = set_db_session(SessionType="mysql", SessionHandler="mysql_media")
def get_account_sql(AccountType=""):
    #获取子账户
    source_data_sql = """
                   select account_id, media, service_code from(
                   select account_id, media, service_code,@row_num:=@row_num+1 as rn
                   from big_data_mdg.media_advertiser a,(select @row_num:=0) r
                   where media = %s ) tmp
                """%(AccountType)
    #获取子账户条数
    get_account_count_sql = """
       select count(1),min(rn),max(rn) 
       from(select account_id, media, service_code,@row_num:=@row_num+1 as rn
            from big_data_mdg.media_advertiser a,(select @row_num:=0) r
            where media = %s
           ) tmp
    """%(AccountType)
    ok,all_rows = mysql_session.get_all_rows(get_account_count_sql)
    fcnt = 0
    sql_list = []
    if all_rows is not None and len(all_rows) > 0:
        fcnt = all_rows[0][0]
        fmin = int(all_rows[0][1])
        fmax = int(all_rows[0][2])
    if fcnt > 0:
        source_cnt = fcnt
        print("min=%s, max=%s, count=%s" % (str(fmin), str(fmax), str(fcnt)))
        if fcnt < 500:
            # 500以下的数据量不用分批跑
            sql_list.clear()
            sql_list.append(source_data_sql)
        else:
            sql_list.clear()
            num_proc = int(fmax) - int(fmin)
            if num_proc > 5:
                # 最多5个进程同时获取数据
                num_proc = 5
            # 每一个进程查询量的增量
            d = math.ceil((int(fmax) - int(fmin) + 1) / num_proc)
            i = 0
            while i < num_proc:
                s_ind = int(fmin) + i * d
                e_ind = s_ind + d
                if i == num_proc - 1:
                    e_ind = int(fmax) + 1
                sql = source_data_sql + " where rn" + " >= " + str(s_ind) + " and rn" + " < " + str(e_ind)
                sql_list.append(sql)
                i = i + 1
    return sql_list
def get_account_token(AccountId="",ServiceCode=""):
    token_url = """http://token.ecsage.net/service-media-token/rest/getToken?code=%s""" % (ServiceCode)
    account_id = AccountId
    service_code = ServiceCode
    token_data_list = requests.post(token_url).json()
    token_data = token_data_list["t"]["token"]
    return account_id,service_code,token_data

def create_task(Sql="",ThreadName="",arg=None):
    account_id = ""
    token_data = ""
    service_code = ""
    num = 1
    if arg is not None:
       Sql = arg["Sql"]
       ThreadName = arg["ThreadName"]
       ok, data_list = mysql_session.get_all_rows_thread(Sql)
       print(data_list)
       ########################for data in data_list:
       ########################    account_id,service_code,token_data = get_account_token(AccountId=data[0], ServiceCode=data[2])
       ########################    open_api_domain = "https://ad.toutiao.com"
       ########################    path = "/open_api/2/async_task/create/"
       ########################    url = open_api_domain + path
       ########################    params = {
       ########################        "advertiser_id": account_id,
       ########################        "task_name": "%s_%s" % (ThreadName,num),
       ########################        "task_type": "REPORT",
       ########################        "task_params": {
       ########################            "start_date": "2020-11-02",
       ########################            "end_date": "2020-11-02",
       ########################            "group_by": ["STAT_GROUP_BY_CAMPAIGN_ID"]
       ########################        }
       ########################    }
       ########################    headers = {
       ########################        'Content-Type': "application/json",
       ########################        'Access-Token': token_data
       ########################    }
       ########################    ###############resp = requests.post(url, json=params, headers=headers)
       ########################    ###############resp_data = resp.json()
       ########################    num = num + 1
       ########################    print(ThreadName,num, "**********************************************")
           ###############task_id = resp_data["data"]["task_id"]
           ###############task_name = resp_data["data"]["task_name"]
           ###############print(task_id, task_name, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
           ###############os.system("""echo "%s %s %s %s %s">>/tmp/task_status.log """%(token_data,service_code,account_id,task_id,task_name))
def exec_create_task(AccountType=2):
    sql_list = get_account_sql(AccountType=AccountType)
    if sql_list is not None and len(sql_list) > 0:
        i = 0
        th = []
        for sql in sql_list:
            i = i + 1
            etl_thread = EtlThread(thread_id=i, thread_name="Thread_%s.%s_%d" % ("airflow.dag", "airflow.task", i),
                                   my_run=create_task,
                                   Sql = sql,ThreadName="Thread_%s.%s_%d" % ("airflow.dag", "airflow.task", i)
                                   )
            etl_thread.start()
            #th.append(etl_thread)
if __name__ == '__main__':
    exec_create_task(AccountType=2)
    ###################import time
    ###################
    ###################time.sleep(30)
    #################### 9324963 test_200
    #################### 获取任务状态
    ###################import requests
    ###################
    ###################open_api_domain = "https://ad.toutiao.com"
    ###################path = "/open_api/2/async_task/get/"
    ###################url = open_api_domain + path
    ###################params = {
    ###################    "advertiser_id": account_id,
    ###################    "filtering": {
    ###################        "task_ids": [9324963],
    ###################        "task_name": "test_200"
    ###################    }
    ###################}
    ###################headers = {
    ###################    'Content-Type': "application/json",
    ###################    'Access-Token': token_data
    ###################}
    ###################resp = requests.get(url, json=params, headers=headers)
    ###################resp_data = resp.json()
    ###################print(resp_data)
    ###################task_status = resp_data["data"]["list"][0]["task_status"]
    ###################if resp_data["data"]["list"][0]["task_status"] == "ASYNC_TASK_STATUS_COMPLETED":
    ###################    import requests
    ###################
    ###################    open_api_domain = "https://ad.toutiao.com"
    ###################    path = "/open_api/2/async_task/download/"
    ###################    url = open_api_domain + path
    ###################    params = {
    ###################        "advertiser_id": account_id,
    ###################        "task_id": task_id
    ###################    }
    ###################    headers = {
    ###################        'Content-Type': "application/json",
    ###################        'Access-Token': token_data
    ###################    }
    ###################    resp = requests.get(url, json=params, headers=headers)
    ###################    resp_data = resp.content
    ###################    print(resp_data, "=========================================")
    ###################    if resp_data == "empty result":
    ###################        print(account_id, service_code)
    ###################else:
    ###################    print(task_id, task_status, "#########################################")
    ###################