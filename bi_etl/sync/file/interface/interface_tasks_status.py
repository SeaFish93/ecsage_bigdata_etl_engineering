import math
import requests
import sys
import os
import time
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.create_async_tasks import oe_create_tasks
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.remote_proc import exec_remote_proc

mysql_session = set_db_session(SessionType="mysql", SessionHandler="mysql_media")
etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")

def get_download_task(MediaType="",AsyncNotemptyFile="",AsyncEmptyFile="",AsyncStatusExceptionFile=""):
    sql_list = get_download_sql(MediaType=MediaType)
    if sql_list is not None and len(sql_list) > 0:
        i = 0
        th = []
        for sqls in sql_list:
                i = i + 1
                etl_thread = EtlThread(thread_id=i, thread_name="%s%d" % (MediaType,i),
                                   my_run=get_download_content,
                                   Sql = sqls,AsyncNotemptyFile=AsyncNotemptyFile,AsyncEmptyFile=AsyncEmptyFile,
                                   AsyncStatusExceptionFile=AsyncStatusExceptionFile,MediaType=MediaType
                                   )
                etl_thread.start()
                import time
                time.sleep(2)
                th.append(etl_thread)
        for etl_th in th:
            etl_th.join()
        #记录有效子账户
        insert_sql = """
           load data local infile '%s' into table metadb.oe_valid_account_interface fields terminated by ' ' lines terminated by '\\n' (account_id,media_type,service_code,token_data)
         """ % (AsyncNotemptyFile)
        etl_md.execute_sql("""delete from metadb.oe_valid_account_interface where media_type=%s and service_code='%s' """ % (MediaType, ServiceCode))
        etl_md.local_file_to_mysql(sql=insert_sql)

        print("the end!!!!!")

def get_download_sql(MediaType=""):
    #获取子账户
    source_data_sql = """
                   select token_data,service_code,account_id,task_id,task_name from(
                   select token_data,service_code,account_id,task_id,task_name,@row_num:=@row_num+1 as rn
                   from (select distinct token_data,service_code,account_id,task_id,task_name
                         from metadb.oe_async_task_interface 
                         where media_type = %s
                        ) a,(select @row_num:=0) r
                   ) tmp
                """%(MediaType)
    #获取子账户条数
    get_account_count_sql = """
       select count(1),min(rn),max(rn) 
       from(select token_data,service_code,account_id,task_id,task_name,@row_num:=@row_num+1 as rn
            from (select distinct token_data,service_code,account_id,task_id,task_name
                  from metadb.oe_async_task_interface 
                  where media_type = %s
                 ) a,(select @row_num:=0) r
           ) tmp
    """%(MediaType)
    ok,all_rows = etl_md.get_all_rows(get_account_count_sql)
    fcnt = 0
    sql_list = []
    if all_rows is not None and len(all_rows) > 0:
        fcnt = all_rows[0][0]
    if fcnt > 0:
        fmin = int(all_rows[0][1])
        fmax = int(all_rows[0][2])
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
          set_download_content(MediaType=MediaType,ServiceCode=ServiceCode,AccountId=account_id, TaskId=task_id, Token=token,AsyncNotemptyFile=AsyncNotemptyFile,AsyncEmptyFile=AsyncEmptyFile)
        except Exception as e:
          import time
          set_true = True
          n = 1
          while set_true:
              time.sleep(2)
              try:
                  set_download_content(MediaType=MediaType,ServiceCode=ServiceCode,AccountId=account_id, TaskId=task_id, Token=token,AsyncNotemptyFile=AsyncNotemptyFile,AsyncEmptyFile=AsyncEmptyFile)
                  set_true = False
              except Exception as e:
                  if n > 3:
                      os.system("""echo "%s %s %s">>%s """ % (token, service_code, account_id,AsyncStatusExceptionFile))
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


def get_task_status_sql(MediaType=""):
    #获取子账户
    source_data_sql = """
      select token_data,service_code,account_id,task_id,task_name
      from(select token_data,service_code,account_id,task_id,task_name,@row_num:=@row_num+1 as rn
           from (select distinct token_data,service_code,account_id,task_id,task_name
                 from metadb.oe_async_task_interface 
                 where media_type = %s
                ) a,(select @row_num:=0) r
          ) tmp
    """%(MediaType)
    #获取子账户条数
    get_account_count_sql = """
       select count(1),min(rn),max(rn)
      from(select token_data,service_code,account_id,task_id,task_name,@row_num:=@row_num+1 as rn
           from (select distinct token_data,service_code,account_id,task_id,task_name
                 from metadb.oe_async_task_interface 
                 where media_type = %s
                ) a,(select @row_num:=0) r
          ) tmp
    """%(MediaType)
    ok,all_rows = etl_md.get_all_rows(get_account_count_sql)
    fcnt = 0
    sql_list = []
    max_min = []
    if all_rows is not None and len(all_rows) > 0:
        fcnt = all_rows[0][0]
    if fcnt > 0:
        fmin = int(all_rows[0][1])
        fmax = int(all_rows[0][2])
        source_cnt = fcnt
        print("min=%s, max=%s, count=%s" % (str(fmin), str(fmax), str(fcnt)))
        if fcnt < 100:
            # 100以下的数据量不用分批跑
            sql_list.clear()
            sql_list.append(source_data_sql)
        else:
            sql_list.clear()
            num_proc = int(fmax) - int(fmin)
            if num_proc > 4:
                # 最多20个进程同时获取数据
                num_proc = 20
            # 每一个进程查询量的增量
            d = math.ceil((int(fmax) - int(fmin) + 1) / num_proc)
            i = 0
            while i < num_proc:
                s_ind = int(fmin) + i * d
                e_ind = s_ind + d
                if i == num_proc - 1:
                    e_ind = int(fmax) + 1
                sql = source_data_sql + " and b.rn" + " >= " + str(s_ind) + " and b.rn" + " < " + str(e_ind)
                max_min.append([s_ind,e_ind])
                i = i + 1
    return source_data_sql,max_min

if __name__ == '__main__':
    media_type = sys.argv[1]
    ######### async_task = sys.argv[2]
    ######### async_status_exception_file = """/tmp/async_status_exception_%s.log""" % (media_type)
    ######### async_notempty_file = """/tmp/async_notempty_%s.log"""%(media_type)
    ######### async_empty_file = """/tmp/async_empty_%s.log""" % (media_type)
    ######### os.system("""rm -f %s"""%(async_notempty_file))
    ######### os.system("""rm -f %s"""%(async_empty_file))
    ######### os.system("""rm -f %s"""%(async_status_exception_file))
    print("开始启动下载内容!!!!!")
    sqls,max_min_list = get_task_status_sql(MediaType=media_type)
    print(max_min_list,"#########################################")
    #import time
    #time.sleep(120)
    #get_download_task(MediaType=media_type,AsyncNotemptyFile=async_notempty_file,AsyncEmptyFile=async_empty_file,AsyncStatusExceptionFile=async_status_exception_file)
