import math
import requests

import os
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread

mysql_session = set_db_session(SessionType="mysql", SessionHandler="mysql_media")
etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")

def get_account_sql(MediaType="",ServiceCode=""):
    #获取子账户
    source_data_sql = """
                   select account_id, media, service_code from(
                   select account_id, media, service_code,@row_num:=@row_num+1 as rn
                   from big_data_mdg.media_advertiser a,(select @row_num:=0) r
                   where media = %s 
                     and service_code = '%s'
                   ) tmp
                """%(MediaType,ServiceCode)
    #获取子账户条数
    get_account_count_sql = """
       select count(1),min(rn),max(rn) 
       from(select account_id, media, service_code,@row_num:=@row_num+1 as rn
            from big_data_mdg.media_advertiser a,(select @row_num:=0) r
            where media = %s
              and service_code = '%s'
           ) tmp
    """%(MediaType,ServiceCode)
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
                num_proc = 20
            #if fcnt > 10000:
            #    num_proc = 20
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
def get_account_token(MediaType="",ServiceCode=""):
    # 获取子账户
    source_data_sql = """
                       select account_id, media, service_code
                       from(select account_id, media, service_code,@row_num:=@row_num+1 as rn
                            from big_data_mdg.media_advertiser a,(select @row_num:=0) r
                            where media = %s 
                              and service_code = '%s'
                       ) tmp
                    """ % (MediaType,ServiceCode)
    ok, source_data = mysql_session.get_all_rows(source_data_sql)
    headers = {'Content-Type': "application/json", "Connection": "close"}
    token_url = """http://token.ecsage.net/service-media-token/rest/getToken?code=%s""" % (ServiceCode)
    service_code = ServiceCode
    token_data_list = requests.post(token_url,headers=headers).json()
    token_data = token_data_list["t"]["token"]
    return source_data,service_code,token_data
def get_token(MediaType=""):
    token = []
    # 获取service code
    get_service_code_sql = """
         select  service_code,count(1)
         from big_data_mdg.media_advertiser a
         where media = %s
           and service_code = 'tt-hnhd-02'
         group by service_code
        """%(MediaType)
    ok, all_rows = mysql_session.get_all_rows(get_service_code_sql)
    for data in all_rows:
        source_data,service_code,token_data = get_account_token(MediaType=2,ServiceCode=data[0])
        sql_list = get_account_sql(MediaType=2, ServiceCode=data[0])
        token.append((token_data,sql_list))
        #print(token)
    return token

def create_task(Sql="",ThreadName="",Token="",MediaType="",arg=None):
    account_id = ""
    token_data = ""
    service_code = ""
    num = 1
    if arg is not None:
       Sql = arg["Sql"]
       ThreadName = arg["ThreadName"]
       Token = arg["Token"]
       MediaType = arg["MediaType"]
       ok, data_list = mysql_session.get_all_rows_thread(Sql)
       print("线程：%s,长度：%s,=================================="%(ThreadName,len(data_list)))
       for data in data_list:
           account_id = data[0]
           service_code = data[2]
           token_data = Token
           ####### open_api_domain = "https://ad.toutiao.com"
           ####### path = "/open_api/2/async_task/create/"
           ####### url = open_api_domain + path
           ####### params = {
           #######            "advertiser_id": account_id,
           #######            "task_name": "%s_%s" % (ThreadName,num),
           #######            "task_type": "REPORT",
           #######            "force": "true",
           #######            "task_params": {
           #######                  "start_date": "2020-11-02",
           #######                  "end_date": "2020-11-02",
           #######                  "group_by": ["STAT_GROUP_BY_CAMPAIGN_ID"]
           #######             }
           #######           }
           ####### headers = {
           #######     'Content-Type': "application/json",
           #######     'Access-Token': token_data,
           #######     'Connection': "close"
           ####### }
           try:
             set_async_tasks(ServiceCode=service_code, AccountId=account_id, ThreadName=ThreadName, Num=num, Token=token_data)
             num = num + 1
             print(ThreadName,num, "**********************************************")
           except Exception as e:
             import time
             set_true = True
             n = 1
             while set_true:
               time.sleep(2)
               try:
                 set_async_tasks(ServiceCode=service_code, AccountId=account_id, ThreadName=ThreadName, Num=num,Token=token_data)
                 set_true = False
               except Exception as e:
                 if n > 3:
                    set_true = False
                    os.system("""echo "%s %s %s">>/tmp/exception_log.log """%(token_data, service_code, account_id))
               n = n + 1

def exec_create_task(MediaType=2):
    sql_list = get_token(MediaType=MediaType)
    if sql_list is not None and len(sql_list) > 0:
        i = 0
        th = []
        for sqls in sql_list:
            token = sqls[0]
            for get_sql in sqls[1]:
                i = i + 1
                etl_thread = EtlThread(thread_id=i, thread_name="Thread.%d" % (i),
                                   my_run=create_task,
                                   Sql = get_sql,ThreadName="Thread%d" % (i),Token=token,
                                   MediaType = MediaType
                                   )
                etl_thread.start()
                import time
                time.sleep(2)
                th.append(etl_thread)
        for etl_th in th:
            etl_th.join()
        os.system("""date >>/tmp/task_status_1.log """)
        insert_sql = """
          load data local infile '/tmp/create_task_status_1.log' into table metadb.oe_async_task_interface_bak1 fields terminated by ' ' lines terminated by '\\n' (token_data,service_code,account_id,task_id,task_name)
        """
        etl_md.execute_sql("""delete from metadb.oe_async_task_interface_bak1""")
        etl_md.execute_sql(insert_sql)
def set_async_tasks(ServiceCode="",AccountId="",ThreadName="",Num="",Token=""):
    open_api_domain = "https://ad.toutiao.com"
    path = "/open_api/2/async_task/create/"
    url = open_api_domain + path
    params = {
        "advertiser_id": AccountId,
        "task_name": "%s_%s" % (ThreadName, Num),
        "task_type": "REPORT",
        "force": "true",
        "task_params": {
            "start_date": "2020-11-02",
            "end_date": "2020-11-02",
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
    os.system("""echo "%s %s %s %s %s">>/tmp/create_task_status_1.log """ % (Token, ServiceCode, AccountId, task_id, task_name))

def set_download_content(AccountId="",TaskId="",Token=""):
    open_api_domain = "https://ad.toutiao.com"
    path = "/open_api/2/async_task/download/"
    url = open_api_domain + path
    params = {
        "advertiser_id": AccountId,
        "task_id": TaskId
    }
    headers = {
        'Content-Type': "application/json",
        'Access-Token': Token
    }
    resp = requests.get(url, json=params, headers=headers)
    resp_data = resp.content
    if str(resp_data) == """b'empty result'""":
        os.system("""echo "%s %s %s">>/tmp/empty1.log """%(AccountId,TaskId,Token))
    else:
        os.system("""echo "%s">>/tmp/notempty1.log """%(resp_data))

def get_download_content(Sql="",arg=None):
  if arg is not None:
    Sql = arg["Sql"]
    print(Sql,"################################")
    ok,datas = etl_md.get_all_rows(Sql)
    for data in datas:
        token = data[0]
        service_code = data[1]
        account_id = data[2]
        task_id = data[3]
        task_name = data[4]
        try:
          set_download_content(AccountId=account_id, TaskId=task_id, Token=token)
        except Exception as e:
          import time
          set_true = True
          n = 1
          while set_true:
              time.sleep(2)
              try:
                  set_download_content(AccountId=account_id, TaskId=task_id, Token=token)
                  set_true = False
              except Exception as e:
                  if n > 3:
                      set_true = False
                      #os.system("""echo "%s %s %s">>/tmp/exception_log.log """ % (token_data, service_code, account_id))
              n = n + 1
def get_download_task():
    sql_list = get_download_sql(ServiceCode="tt-hnhd-02")
    if sql_list is not None and len(sql_list) > 0:
        i = 0
        th = []
        for sqls in sql_list:
                i = i + 1
                etl_thread = EtlThread(thread_id=i, thread_name="Thread.%d" % (i),
                                   my_run=get_download_content,
                                   Sql = sqls
                                   )
                etl_thread.start()
                import time
                time.sleep(2)
                th.append(etl_thread)
        for etl_th in th:
            etl_th.join()
        print("the end!!!!!")

def get_download_sql(ServiceCode=""):
    #获取子账户
    source_data_sql = """
                   select token_data,service_code,account_id,task_id,task_name from(
                   select token_data,service_code,account_id,task_id,task_name,@row_num:=@row_num+1 as rn
                   from metadb.oe_async_task_interface_bak1 a,(select @row_num:=0) r
                   where service_code = '%s'
                   ) tmp
                """%(ServiceCode)
    #获取子账户条数
    get_account_count_sql = """
       select count(1),min(rn),max(rn) 
       from(select token_data,service_code,account_id,task_id,task_name,@row_num:=@row_num+1 as rn
            from metadb.oe_async_task_interface_bak1 a,(select @row_num:=0) r
            where service_code = '%s'
           ) tmp
    """%(ServiceCode)
    ok,all_rows = etl_md.get_all_rows(get_account_count_sql)
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
                num_proc = 20
            #if fcnt > 10000:
            #    num_proc = 20
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

if __name__ == '__main__':
    #etl_md.execute_sql("""delete from metadb.oe_async_task_interface where dag_id='%s' and dag_task_id = '%s'""" % ("test", "test"))
    ###########os.system("""rm -f /tmp/task_status_1.log """)
    ###########os.system("""rm -f /tmp/create_task_status_1.log""")
    ###########os.system("""date >>/tmp/task_status_1.log """)
    ###########os.system("""rm -f /tmp/exception_log.log""")
    ###########exec_create_task(MediaType=2)
    get_download_task()
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