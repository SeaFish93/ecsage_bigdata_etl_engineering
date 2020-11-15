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
from tasks import *

etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")

#入口方法
def main(TaskInfo,**kwargs):
    #time.sleep(800)
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
    os.system("""rm -f /tmp/sql_%s.sql""")
    os.system("""rm -f /tmp/celery_task_status.log""")
    #etl_md.execute_sql("""delete from metadb.oe_valid_account_interface where media_type=%s """ % (media_type))
    #获取子账户
    source_data_sql = """
                 select distinct account_id,media_type,service_code,token_data,task_id,task_name
                 from metadb.oe_async_task_interface 
                 where media_type = %s
    """%(media_type)
    ok, datas = etl_md.get_all_rows(source_data_sql)
    for get_data in datas:
        status_id = run_task_exception.delay(AsyncNotemptyFile=async_notempty_file,AsyncEmptyFile=async_empty_file,
                                 AsyncNotSuccFile=async_not_succ_file,AsyncStatusExceptionFile=async_status_exception_file,ExecData=get_data)
        os.system("""echo "%s">>/tmp/celery_task_status.log"""%(status_id))
        break;
    #获取状态
    with open("/tmp/celery_task_status.log") as lines:
       array=lines.readlines()
       for data in array:
          get_data = data.strip('\n').split(" ")
          print(get_data,"=============================")
