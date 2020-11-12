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
    os.system("""rm -f /tmp/get_interface_oe_account.log """)
    exec_date = airflow.execution_date_utc8_str[0:10]
    sql = """
           create temporary table %s_%s as
           select account_id,@row_num:=@row_num+1 as rn
           from(select account_id from big_data_mdg.media_advertiser
                where media in(2,201,203)
                group by account_id ) tmp
               ,(select @row_num:=0) r
        """%(airflow.dag,airflow.task)
    mysql_session.execute_sql(sql)
    sql = """select concat_ws(' ',a.account_id, a.media, a.service_code,b.rn)
           from big_data_mdg.media_advertiser a
           inner join %s_%s b
           on a.account_id = b.account_id
           where a.media in(2,201,203)
           """%(airflow.dag,airflow.task)
    ok, all_rows = mysql_session.get_all_rows(sql)
    for data in all_rows:
        os.system("""echo "%s">>/tmp/get_interface_oe_account.log """%(data[0]))
