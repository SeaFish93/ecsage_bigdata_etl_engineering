# -*- coding: utf-8 -*-
# @Time    : 2020/01/06 17:05
# @Author  : wangsong
# @FileName: data_etl_script.py
# @Software: PyCharm
#function info：etl跑批

import datetime
from ecsage_bigdata_etl_engineering.common.exec_script.bat.exec_script_sql import run as etl_main
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
import airflow
from ecsage_bigdata_etl_engineering.common.operator.mysql.conn_mysql_metadb import EtlMetadata
from ecsage_bigdata_etl_engineering.common.alert.alert_info import get_create_dag_alert
from ecsage_bigdata_etl_engineering.common.base.set_process_exit import set_exit
from ecsage_bigdata_etl_engineering.common.base.dep_task import dep_task_main
import os

etl_md = EtlMetadata()
cron_str = ""
seconds = ""
schedule_interval = ""

#获取dag信息
ok, get_dags = etl_md.execute_sql(sqlName="get_data_dags_sql",Parameter={"exec_type":"etl"},IsReturnData="Y")
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
    if batch_type == "hour":
        start_date = datetime.datetime.now() + datetime.timedelta(hours=-2)
    elif batch_type == "day":
        start_date = airflow.utils.dates.days_ago(2)
    else:
        print("dag【%s】配置作业出现异常，未提供正确批次频率！！！" % (dag_id))
        msg = get_create_dag_alert(FileName="%s" % (os.path.basename(__file__)),
                                   Log="dag【%s】配置作业出现异常，未提供正确批次频率！！！" % (dag_id),
                                   Developer="工程维护")
        set_exit(LevelStatu="red", MSG=msg)
        start_date = datetime.datetime.now()
    schedule_interval = dag_info[4]
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
        #'on_failure_callback': hour_failure_callback
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
    start_etl_task = DummyOperator(task_id="start_etl_task", dag=dag)
    end_etl_task = DummyOperator(task_id="end_etl_task", dag=dag)
    # 同步任务配置
    ok, get_tasks = etl_md.execute_sql(sqlName="get_etl_task_sql", Parameter={"dag_id": dag_id},IsReturnData="Y")
    print(get_tasks,"=================================================")
    if ok is False:
        msg = get_create_dag_alert(FileName="%s" % (os.path.basename(__file__)), Log="获取Tasks元数据出现异常！！！",
                                   Developer="工程维护")
        set_exit(LevelStatu="red", MSG=msg)
    tasks = []
    dags = {}
    if len(get_tasks) > 0:
       for tasks_info in get_tasks:
          #配置跑批任务属性
          task_id = tasks_info[0]
          #level = tasks_info[3]
          task_type = tasks_info[27]
          tasks.append({"task_id":task_id,"batch_type":batch_type,"task_type":task_type}) #+ """,%s""" % (task_id)
          dags[task_id] = dag_id
          no_run_time = []
          no_run_date = []
          if tasks_info[8] is not None and tasks_info[8] != "":
            for time_no in (tasks_info[8]).split(","):
              no_run_time.append(str(int(time_no)))
          #if tasks_info[15] is not None and tasks_info[15] != "":
          #  for date_no in (tasks_info[15]).split(","):
          #    no_run_date.append(str(int(date_no)))
          task = locals()
          #定义task对象
          if batch_type == "hour":
             pass
          elif batch_type == "day" and task_type == 'etl':
              task['%s' % (task_id)] = PythonOperator(task_id=task_id,
                                                      python_callable=etl_main,
                                                      provide_context=True,
                                                      op_args=(tasks_info,no_run_date),
                                                      dag=dag)
          elif batch_type == "day" and task_type == 'check':
             pass
             ## task['%s' % (task_id)] = PythonOperator(task_id=task_id,
             ##                                         #pool='check_a',
             ##                                         python_callable=check_data_quality_main,
             ##                                         provide_context=True,
             ##                                         op_args=(tasks_info, "unique", dag_id + "." + task_id),
             ##                                         dag=dag)
          else:
              pass
       for task_name in tasks:
          if task_name["batch_type"] == "day" and task_name["task_type"] in("check","etl"):
             # 设置task依赖
             ok, task_deps = etl_md.execute_sql(sqlName="get_task_dep_sql", Parameter={"task_id": task_name["task_id"]},
                                                IsReturnData="Y")
             if len(task_deps) > 0:
                 num = 1
                 for task_dep in task_deps:
                     if task_dep[0] == task_dep[3]:
                         task[task_dep[2]].set_upstream(task[task_dep[1]])
                         ok, task_downstream_deps = etl_md.execute_sql(sqlName="get_downstream_depend_sql",Parameter={"task_id": task_name["task_id"]},IsReturnData="Y")
                         if len(task_downstream_deps) == 0:
                             task[task_dep[1]].set_upstream(start_etl_task)
                     else:
                         external_task = PythonOperator(task_id='external_%s_%s_%s' % (num,task_dep[0], task_dep[1]),
                                                        python_callable=dep_task_main,
                                                        provide_context=True,
                                                        op_args=(task_dep[0], task_dep[1], task_dep[4],),
                                                        dag=dag)
                         task[task_dep[2]].set_upstream(external_task)
                         external_task.set_upstream(start_etl_task)
                         num = num + 1
             else:
                 task['%s' % (task_name["task_id"])].set_upstream(start_etl_task)
             ok, task_upstream_deps = etl_md.execute_sql(sqlName="get_upstream_depend_sql", Parameter={"dep_task_id": task_name["task_id"]},IsReturnData="Y")
             if len(task_upstream_deps) == 0:
               end_etl_task.set_upstream(task['%s' % (task_name["task_id"])])
    else:
        end_etl_task.set_upstream(start_etl_task)
    dag_num = dag_num + 1
