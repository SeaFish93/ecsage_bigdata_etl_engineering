# -*- coding: utf-8 -*-
# @Time    : 2019/11/12 18:04
# @Author  : wangsong
# @FileName: interface_oe_async_2_hive.py
# @Software: PyCharm
# function info：定义oe异步接口

from celery.result import AsyncResult
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_alert_info_d
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.common.base.airflow_instance import Airflow
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_oe_sync_tasks_data_return as get_oe_sync_tasks_data_return_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import get_oe_sync_tasks_data as get_oe_sync_tasks_data_celery
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.interface_comm import get_local_hdfs_thread
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.get_account_tokens import get_oe_account_token
from ecsage_bigdata_etl_engineering.common.base.sync_method import get_table_columns_info
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.get_data_2_snap import exec_snap_hive_table
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread
from ecsage_bigdata_etl_engineering.common.base.get_config import Conf
import os
import time
import json
import ast

conf = Conf().conf
etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")

def get_sync_pages_number():
    ParamJson = {"end_date": "2020-11-29", "page_size": "1000", "start_date": "2020-11-29",
                 "advertiser_id": 1654599060231176, "group_by": ['STAT_GROUP_BY_FIELD_ID','STAT_GROUP_BY_CITY_NAME'],
                 "time_granularity": "STAT_TIME_GRANULARITY_DAILY",
                 "page": 1,
                 "service_code": "tt-1654599060231176"
                 }
    ParamJson = str(ParamJson)
    UrlPath = "/open_api/2/report/creative/get/"
    celery_task_id = get_oe_sync_tasks_data_return_celery.delay(ParamJson=ParamJson,UrlPath=UrlPath)
    print(celery_task_id,"====================")
    set_run = True
    while set_run:
      celery_task_status = get_celery_job_status(CeleryTaskId=celery_task_id)
      if celery_task_status is True:
          set_run = False
      else:
          print("等待！！！")
    page_numbers = AsyncResult(id=celery_task_id)
    page_number = int(page_numbers.get())
    print(page_number,"##############################")
    for page in range(page_number):
        pages = page + 1
        param_json = json.dumps(ParamJson)
        param_json = ast.literal_eval(json.loads(param_json))
        param_json["page"] = pages
        celery_task_id = get_oe_sync_tasks_data_celery.delay(ParamJson=ParamJson, UrlPath=UrlPath)

def get_celery_job_status(CeleryTaskId=""):
    set_task = AsyncResult(CeleryTaskId)
    status = set_task.status
    if status == "SUCCESS":
       return True
    else:
       return False

if __name__ == '__main__':
    get_sync_pages_number()