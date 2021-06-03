# -*- coding: utf-8 -*-
# @Time    : 2019/11/19 17:05
# @Author  : wangsong
# @FileName: interface_oe_sync_auto.py
# @Software: PyCharm
#function info：接口数据采集

import datetime
from ecsage_bigdata_etl_engineering.bi_etl.unload.db.mysql.interface_hive_to_mysql import main as hive_to_mysql_main
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
import airflow

from ecsage_bigdata_etl_engineering.common.operator.mysql.conn_mysql_metadb import EtlMetadata
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_create_dag_alert
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit
from ecsage_bigdata_etl_engineering.common.base.dep_task import dep_task_main
from ecsage_bigdata_etl_engineering.common.base.dep_task_timing import dep_task_main_timing
import os

etl_meta = EtlMetadata()
#获取dag信息
ok, get_dags = etl_meta.execute_sql(sqlName="get_data_dags_sql",Parameter={"exec_type":"interface_hive_mysql"},IsReturnData="Y")
if ok is False:
    msg = get_create_dag_alert(FileName="%s"%(os.path.basename(__file__)),Log="获取Dags元数据出现异常！！！",Developer="工程维护")
    set_exit(LevelStatu="red", MSG=msg)
dag_num = 0
#循环创建dag
for dag_info in get_dags:
    #定义dag属性
    dag_id = dag_info[0]
    owner = dag_info[1]
    retries = int(dag_info[2])
    batch_type = dag_info[3]
    schedule_interval = dag_info[4]
    utc_hour = datetime.datetime.utcnow().hour
    day_offset,hour_offset = (0,utc_hour -1) if utc_hour >= 1 else (1,23)#Airfflow 不能突破UTC所在的小时，需要偏移1小时
    if batch_type == "hour":
        start_date = airflow.utils.dates.days_ago(day_offset,hour=hour_offset)
    elif batch_type == "day":
        start_date = airflow.utils.dates.days_ago(2)
    else:
        print("dag【%s】配置作业出现异常，未提供正确批次频率！！！"%(dag_id))
        msg = get_create_dag_alert(FileName="%s" % (os.path.basename(__file__)),
                                   Log="dag【%s】配置作业出现异常，未提供正确批次频率！！！"%(dag_id),
                                   Developer="工程维护")
        set_exit(LevelStatu="red", MSG=msg)
        start_date = datetime.datetime.now()
    if int(dag_info[5]) == 1:
        depends_on_past = True
    else:
        depends_on_past = False
    priority_weight = int(dag_info[6])

    #airflow dag属性
    args = {
        'owner': owner,
        'depends_on_past': depends_on_past,
        'priority_weight': priority_weight,
        'retries': retries,
        'retry_delay': datetime.timedelta(minutes=2),
        'start_date': start_date,
       # 'on_failure_callback': hour_failure_callback
    }
    names = locals()
    #动态创建dag实例
    names['dag_%s' % dag_num] = DAG(
        dag_id=dag_id,
        default_args=args,
        schedule_interval=schedule_interval)
    dag_name ='dag_%s' % dag_num
    #动态获取dag实例
    dag = locals()[dag_name]
    start_sync_task = DummyOperator(task_id="start_sync_task", dag=dag)
    end_sync_task = DummyOperator(task_id="end_sync_task", dag=dag)
    # 同步任务配置
    ok, get_tasks = etl_meta.execute_sql(sqlName="get_interface_hive_mysql_sql",Parameter={"dag_id":dag_id},IsReturnData="Y")
    if ok is False:
      msg = get_create_dag_alert(FileName="%s" % (os.path.basename(__file__)), Log="获取Tasks元数据出现异常！！！",Developer="工程维护")
      set_exit(LevelStatu="red", MSG=msg)
    tasks = []
    if len(get_tasks) > 0:
       for tasks_info in get_tasks:
          #配置跑批任务属性
          task_id = tasks_info[0]
          print(task_id,"@@@@@@@@@@@@@@@@@@!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!================================")
          tasks.append({"task_id": task_id, "batch_type": batch_type})
          task = locals()
          #定义task对象

          # 动态创建dag实例
          task['%s' % (task_id)] = PythonOperator(task_id=task_id,
                                         python_callable=hive_to_mysql_main,
                                         provide_context=True,
                                         op_args=(tasks_info,),
                                         dag=dag)
       for task_name in tasks:
          if task_name["batch_type"] in ("day","hour"):
              #小时与天的嗅探方法不同
              dep_task_main_flag = dep_task_main if task_name["batch_type"] == "day" else dep_task_main_timing
              # 设置task依赖
              ok, task_deps = etl_meta.execute_sql(sqlName="get_task_dep_sql", Parameter={"task_id": task_name["task_id"]},IsReturnData="Y")
              if len(task_deps) > 0:
                  external_task = locals()
                  for task_dep in task_deps:
                      if task_dep[0] == task_dep[3]:
                          task[task_dep[2]].set_upstream(task[task_dep[1]])
                          ok, task_downstream_deps = etl_meta.execute_sql(sqlName="get_downstream_depend_sql",Parameter={"task_id": task_name["task_id"]},IsReturnData="Y")
                          if len(task_downstream_deps) == 0:
                              task[task_dep[1]].set_upstream(start_sync_task)
                      else:
                          external_task_id = 'external_%s_%s' % (task_dep[0], task_dep[1])
                          if external_task_id in list(external_task.keys()) and external_task[external_task_id].dag_id == dag_id:
                              task[task_dep[2]].set_upstream(external_task[external_task_id])
                          else:
                              external_task['%s' % (external_task_id)] = PythonOperator(task_id=external_task_id,
                                                                                        python_callable=dep_task_main_flag,
                                                                                        provide_context=True,
                                                                                        op_args=(task_dep[0], task_dep[1],task_dep[4],),
                                                                                        dag=dag)
                              task[task_dep[2]].set_upstream(external_task[external_task_id])
                              external_task[external_task_id].set_upstream(start_sync_task)
              else:
                  task['%s' % (task_name["task_id"])].set_upstream(start_sync_task)
              ok, task_upstream_deps = etl_meta.execute_sql(sqlName="get_ods_upstream_depend_sql",Parameter={"dep_task_id": task_name["task_id"]}, IsReturnData="Y")
              if len(task_upstream_deps) == 0:
                  end_sync_task.set_upstream(task['%s' % (task_name["task_id"])])
    else:
        end_sync_task.set_upstream(start_sync_task)
    dag_num = dag_num + 1