import math
import requests
import sys
import os
import time
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.remote_proc import exec_remote_proc
from ecsage_bigdata_etl_engineering.common.base.airflow_instance import Airflow
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread

mysql_session = set_db_session(SessionType="mysql", SessionHandler="mysql_media")
etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")

#入口方法
def main(TaskInfo,**kwargs):
    global airflow
    global developer
    global regexp_extract_column
    airflow = Airflow(kwargs)
    print(TaskInfo,"####################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    exec_date = airflow.execution_date_utc8_str[0:10]
    media_type = TaskInfo[1]
    account_token_file = """/tmp/account_token_file_%s.log"""%(media_type)
    account_token_exception_file = """/tmp/account_token_exception_file_%s.log"""%(media_type)
    async_task_file = """/tmp/async_create_%s.log""" % (media_type)
    async_task_exception_file = """/tmp/async_create_exception_%s.log""" % (media_type)
    os.system("""rm -f %s """ % (async_task_exception_file))
    os.system("""rm -f %s """%(async_task_file))
    os.system("""rm -f %s """ % (account_token_file))
    os.system("""rm -f %s """ % (account_token_exception_file))
    etl_md.execute_sql("""delete from metadb.oe_async_task_interface where media_type=%s """ % (media_type))
    #获取token
    get_token(MediaType=media_type, AccountTokenFile=account_token_file, AccountTokenExceptionFile=account_token_exception_file)
    #获取每台服务处理数据量
    sql,max_min = get_account_sql(MediaType=media_type)
    ok,host_data = etl_md.get_all_rows("""select ip,user_name,passwd from metadb.request_account_host""")
    n = 0
    host_num = 0
    host_i = 0
    start_end_list = []
    th = []
    for get_data in max_min:
        start_end_list.append(max_min[n])
        if len(start_end_list) == 5 or len(max_min) < 5 or len(max_min)-1 == n:
           print("[%s]执行机器" % (host_data[host_i][0]))
           for start_end in start_end_list:
               max = start_end[1]
               min = start_end[0]
               count = max - min
               if n == 0:
                   min_n = 0
               else:
                   min_n = 1
               sqls_list = get_run_sql(Sql=sql, Max=max, Min=min, Count=count, MinN=min_n)
               shell_cmd = """
                  python3 /root/bigdata_item_code/ecsage_bigdata_etl_engineering/bi_etl/sync/file/interface/create_async_tasks.py "%s" "%s" "%s" "%s" "%s" "%s" > /root/wangsong/create_async.log
               """ % (media_type, "test", sqls_list, async_task_file, async_task_exception_file,exec_date)
               #exec_remote_proc(HostName=host_data[host_i][0], UserName=host_data[host_i][1], PassWord=host_data[host_i][2], ShellCommd=shell_cmd)
               etl_thread = EtlThread(thread_id=n, thread_name="fetch%d" % (n),
                                      my_run=exec_remote_proc,HostName=host_data[host_i][0],
                                      UserName=host_data[host_i][1],PassWord=host_data[host_i][2], ShellCommd=shell_cmd
                                      )
               etl_thread.start()
               th.append(etl_thread)
           start_end_list = []
           host_i = host_i + 1
        host_num = host_num + 1
        n = n + 1
    for etl_th in th:
        etl_th.join()

def get_run_sql(Sql="",Max="",Min="",Count="",MinN=""):
    fcnt = int(Count)
    sql_list = []
    if fcnt > 0:
        fmin = int(Min)
        fmax = int(Max)
        source_cnt = fcnt
        print("min=%s, max=%s, count=%s" % (str(fmin), str(fmax), str(fcnt)))
        if fcnt < 0:
            # 100以下的数据量不用分批跑
            sql_list.clear()
            sql_list.append(Sql)
        else:
            sql_list.clear()
            num_proc = int(fmax) - int(fmin)
            if num_proc > 5:
                # 最多20个进程同时获取数据
                num_proc = 5
            # 每一个进程查询量的增量
            d = math.ceil((int(fmax) - int(fmin) + 1) / num_proc)
            i = 0
            while i < num_proc:
                s_ind = int(fmin) + MinN + i * d
                e_ind = s_ind + d
                if i == num_proc - 1:
                    e_ind = int(fmax) + 1
                sql = Sql + " and b.id" + " >= " + str(s_ind) + " and b.id" + " < " + str(e_ind)
                sql_list.append(sql)
                #max_min.append([s_ind,e_ind])
                i = i + 1
    return sql_list

def get_account_sql(MediaType=""):
    #保存子账户
    account_file = "/tmp/oe_request_get_account_%s.log"%(MediaType)
    account_2_mysql_sql = """
       select concat_ws(' ',@row_num:=@row_num+1,account_id, media, service_code)
            from big_data_mdg.media_advertiser a,(select @row_num:=0) r
            where media = %s
       """ % (MediaType)
    os.system("rm -f %s"%(account_file))
    mysql_session.select_data_to_local_file(sql=account_2_mysql_sql,filename=account_file)
    insert_sql = """
        load data local infile '%s' into table metadb.request_account_interface fields terminated by ' ' lines terminated by '\\n' (id,account_id,media_type,service_code)
      """ % (account_file)
    etl_md.execute_sql("""delete from metadb.request_account_interface where media_type = %s"""%(MediaType))
    etl_md.local_file_to_mysql(sql=insert_sql)
    #获取子账户
    source_data_sql = """
           select b.id,b.account_id,b.media_type,b.service_code,a.token_data
           from metadb.request_account_token_interface a
           inner join metadb.request_account_interface b
           on a.service_code = b.service_code
           and a.media_type = b.media_type
           where a.media_type = %s
    """%(MediaType)
    #获取子账户条数
    get_account_count_sql = """
       select count(1),min(id),max(id) 
       from metadb.request_account_interface
       where media_type = %s
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
                sql = source_data_sql + " and b.id" + " >= " + str(s_ind) + " and b.id" + " < " + str(e_ind)
                #sql_list.append(sql)
                max_min.append([s_ind,e_ind])
                i = i + 1
    return source_data_sql,max_min

def get_account_token(MediaType="",ServiceCode="",AccountTokenFile="",AccountTokenExceptionFile=""):
    headers = {'Content-Type': "application/json", "Connection": "close"}
    token_url = """http://token.ecsage.net/service-media-token/rest/getToken?code=%s""" % (ServiceCode)
    service_code = ServiceCode
    set_true = True
    n = 1
    while set_true:
      try:
        token_data_list = requests.post(token_url,headers=headers).json()
        token_data = token_data_list["t"]["token"]
        os.system("""echo "%s %s %s">>%s """%(MediaType,service_code,token_data,AccountTokenFile))
        set_true = False
      except Exception as e:
        if n > 3:
            os.system("""echo "%s %s">>%s """ % (service_code, MediaType,AccountTokenExceptionFile))
            set_true = False
        else:
            time.sleep(2)
      n = n + 1

#获取token
def get_token(MediaType="",AccountTokenFile="",AccountTokenExceptionFile=""):
    token = []
    # 获取service code
    get_service_code_sql = """
         select  service_code,count(1)
         from big_data_mdg.media_advertiser a
         where media = %s
         group by service_code
        """%(MediaType)
    ok, all_rows = mysql_session.get_all_rows(get_service_code_sql)
    for data in all_rows:
        get_account_token(MediaType=MediaType,ServiceCode=data[0],AccountTokenFile=AccountTokenFile,AccountTokenExceptionFile=AccountTokenExceptionFile)
    insert_sql = """
              load data local infile '%s' into table metadb.request_account_token_interface fields terminated by ' ' lines terminated by '\\n' (media_type,service_code,token_data)
            """ % (AccountTokenFile)
    etl_md.execute_sql("""delete from metadb.request_account_token_interface where media_type=%s """ % (MediaType))
    etl_md.local_file_to_mysql(sql=insert_sql)
