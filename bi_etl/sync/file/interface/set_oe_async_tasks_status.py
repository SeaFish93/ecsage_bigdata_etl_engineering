import math
import requests
import sys
import os
import time
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.common.base.airflow_instance import Airflow
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.fetch_service import get_run_sql
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.fetch_service import get_task_status_sql
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.remote_proc import exec_remote_proc
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread

etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")

#入口方法
def main(TaskInfo,**kwargs):
    #time.sleep(60)
    global airflow
    global developer
    global regexp_extract_column
    airflow = Airflow(kwargs)
    print(TaskInfo,"####################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

    media_type = TaskInfo[1]
    async_status_exception_file = """/tmp/async_status_exception_%s.log""" % (media_type)
    async_notempty_file = """/tmp/async_notempty_%s.log""" % (media_type)
    async_empty_file = """/tmp/async_empty_%s.log""" % (media_type)
    async_not_succ_file = """/tmp/async_not_succ_file_%s.log""" % (media_type)
    os.system("""rm -f %s""" % (async_not_succ_file))
    os.system("""rm -f %s""" % (async_notempty_file))
    os.system("""rm -f %s""" % (async_empty_file))
    os.system("""rm -f %s""" % (async_status_exception_file))
    etl_md.execute_sql("""delete from metadb.oe_valid_account_interface where media_type=%s """ % (media_type))
    sql, max_min_list = set_task_status_sql(MediaType=media_type)
    ok, host_data = etl_md.get_all_rows("""select ip,user_name,passwd from metadb.request_account_host""")
    n = 0
    host_num = 0
    host_i = 0
    start_end_list = []
    th = []
    nu = 1
    nnn = 0
    for get_data in max_min_list:
        start_end_list.append(max_min_list[n])
        if len(start_end_list) == 5 or len(max_min_list) < 5 or len(max_min_list) - 1 == n:
           print("[%s]执行机器" % (host_data[host_i][0]))
           if nnn == 0:
              nn = 0
           else:
              nn = 1
           for start_end in start_end_list:
               max = start_end[1]
               if nn == 0:
                 min = start_end[0]
               else:
                 min = start_end[0] + 1
               count = max - min
               left_filter = """ where b.id """
               right_filter = """ and b.id """
               print(nn,"==============================@@@@@@@@@@@@@@@@@@===============")
               sqls_list = get_run_sql(Sql=sql, Max=max, Min=min, Count=count, MinN=0,LeftFilter=left_filter,RightFilter=right_filter)
               for sqls in sqls_list:
                  os.system("""echo "%s %s %s">>/tmp/sql1213.sql """%(nn,nu,sqls))
               nn = nn + 1
               shell_cmd = """
                   python3 /root/bigdata_item_code/ecsage_bigdata_etl_engineering/bi_etl/sync/file/interface/get_async_tasks_status.py "%s" "%s" "%s" "%s" "%s" "%s" >> /root/wangsong/status_async.log 2>&1 &
                 """ % (media_type, sqls_list, async_notempty_file, async_empty_file, async_status_exception_file, async_not_succ_file)
               etl_thread = EtlThread(thread_id=n, thread_name="fetch%d" % (n),
                                       my_run=exec_remote_proc, HostName=host_data[host_i][0],
                                       UserName=host_data[host_i][1], PassWord=host_data[host_i][2],
                                       ShellCommd=shell_cmd
                                       )
               etl_thread.start()
               th.append(etl_thread)
           start_end_list = []
           host_i = host_i + 1
           nnn = nnn + 1
        host_num = host_num + 1
        n = n + 1
        nu = nu + 1
    for etl_th in th:
        etl_th.join()

def set_task_status_sql(MediaType=""):
    #获取子账户
    source_data_sql = """
      select token_data,service_code,account_id,task_id,task_name
      from(select token_data,service_code,account_id,task_id,task_name,@row_num:=@row_num+1 as id
           from (select distinct token_data,service_code,account_id,task_id,task_name
                 from metadb.oe_async_task_interface 
                 where media_type = %s
                ) a,(select @row_num:=0) r
          ) b
    """%(MediaType)
    #获取子账户条数
    get_account_count_sql = """
       select count(1),min(id),max(id)
      from(select token_data,service_code,account_id,task_id,task_name,@row_num:=@row_num+1 as id
           from (select distinct token_data,service_code,account_id,task_id,task_name
                 from metadb.oe_async_task_interface 
                 where media_type = %s
                ) a,(select @row_num:=0) r
          ) b
    """%(MediaType)
    select_sql,max_min_list = get_task_status_sql(MysqlSession=etl_md, SelectAccountSql=source_data_sql, AccountCountSql=get_account_count_sql)
    return select_sql,max_min_list
