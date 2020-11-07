import math
import requests
import sys
import os
import time
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.create_async_tasks import oe_create_tasks
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.remote_proc import exec_remote_proc
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.fetch_service import get_fetch
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.fetch_service import get_task_status_sql


def set_task_status_sql(MediaType=""):
    etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
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

if __name__ == '__main__':
    media_type = sys.argv[1]
    async_status_exception_file = """/tmp/async_status_exception_%s.log""" % (media_type)
    async_notempty_file = """/tmp/async_notempty_%s.log"""%(media_type)
    async_empty_file = """/tmp/async_empty_%s.log""" % (media_type)
    os.system("""rm -f %s"""%(async_notempty_file))
    os.system("""rm -f %s"""%(async_empty_file))
    os.system("""rm -f %s"""%(async_status_exception_file))
    sql,max_min_list = set_task_status_sql(MediaType=media_type)
    left_filter = """ where b.id """
    right_filter = """ and b.id """
    get_fetch(MediaType=media_type, Sql=sql, BeweetFileList=max_min_list,LeftFilter=left_filter,RightFilter=right_filter,
              AsyncNotemptyFile=async_notempty_file,AsyncEmptyFile=async_empty_file,AsyncStatusExceptionFile=async_status_exception_file)
