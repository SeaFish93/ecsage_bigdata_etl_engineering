# -*- coding: utf-8 -*-
# @Time    : 2020/01/06 18:04
# @Author  : wangsong
# @FileName: dep_task.py
# @Software: PyCharm
# function info：airflow 任务依赖

from datetime import datetime
from croniter import croniter
from airflow.sensors.external_task_sensor import ExternalTaskSensor
import airflow
from ecsage_bigdata_etl_engineering.common.base.airflow_instance import Airflow
from airflow.models import DAG
import pendulum


def dep_task_main(DepDagID="",DepTaskID="",DepTaskCrontab="",**kwargs):
    global execution_date
    execution_date = Airflow(kwargs).execution_date
    print("【execution_date】：%s " % execution_date)
    execution_date_utc8 = Airflow(kwargs).execution_date_utc8
    print("【execution_date_utc8】：%s " % execution_date_utc8)

    dag_id = "external_" + DepDagID
    utc_hour = datetime.utcnow().hour
    day_offset,hour_offset = (0,utc_hour -1) if utc_hour >= 1 else (1,23)#Airfflow 不能突破UTC所在的小时，需要偏移1小时
    args = {
        'owner': 'etl',
        'depends_on_past': False,
        'priority_weight': 10000,
        'retries': 0,
        'start_date':  airflow.utils.dates.days_ago(day_offset,hour=hour_offset),
        'queue': 'airflow',
    }
    dag = DAG(
        dag_id=dag_id,
        default_args=args)

    def external_schedule_interval(execution_date):
        # 服务器上的pendulum版本为1.4.4，此版本不支持转为datetime类型，固此处人工转化
        ex_date_datetime = datetime(execution_date.year, execution_date.month, execution_date.day, execution_date.hour,
                                    execution_date.minute, execution_date.second)
        # pendulum 2.0.5及以后，可以直接传入execution_date（pendulum类型）
        cron = croniter(DepTaskCrontab, ex_date_datetime)
        cron_prev = cron.get_prev(datetime)
        cron_current = cron.get_current(datetime)
        cron_next = cron.get_next(datetime)
        print("上一个调度周期：%s" % cron_prev)
        print("当前调度周期：%s" % cron_current)
        print("下一个调度周期：%s"%cron_next)
        cron_current = cron_next
        cron_prev_pendulum = pendulum.datetime(cron_current.year,
                                                  cron_current.month,
                                                  cron_current.day,
                                                  cron_current.hour,
                                                  cron_current.minute,
                                                  cron_current.second,
                                                  cron_current.microsecond)
        print(ex_date_datetime,"##",DepTaskCrontab,"##",cron_prev_pendulum,"##",cron_prev,"##",cron_current,"##",cron_next,"====================================")
        return cron_prev_pendulum
    external_task = ExternalTaskSensor(external_task_id=DepTaskID,
                                       external_dag_id=DepDagID,
                                       task_id='external_%s_%s' % (DepDagID, DepTaskID),
                                       execution_date_fn=external_schedule_interval,
                                       dag=dag)
    context = {}
    context['execution_date'] = execution_date
    external_task.execute(context=context)

