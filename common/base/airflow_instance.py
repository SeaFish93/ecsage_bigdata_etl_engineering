# -*- coding: utf-8 -*-
# @Time    : 2019/1/16 17:25
# @Author  : luoyh
# @FileName: airflow_instance.py
# @Software: PyCharm

import pendulum
import datetime
from pytz import timezone


class Airflow:
    def __init__(self, kwargs):
        print("Airflow __init__ :")
        if "execution_date" in kwargs:
            self.execution_date = kwargs["execution_date"]
            self.execution_date_utc8 = kwargs['execution_date'].in_timezone("Asia/Shanghai")
            self.execution_date_utc8_str = kwargs['execution_date'].in_timezone("Asia/Shanghai").to_datetime_string()
            self.dt = pendulum.datetime(self.execution_date_utc8.year,
                                        self.execution_date_utc8.month,
                                        self.execution_date_utc8.day,
                                        self.execution_date_utc8.hour,
                                        self.execution_date_utc8.minute,
                                        self.execution_date_utc8.second,
                                        self.execution_date_utc8.microsecond,
                                        "Asia/Shanghai")
            self.ds_nodash_utc8 = self.dt.to_date_string().replace("-", "")
            self.yesterday_ds_nodash_utc8 = self.dt.subtract(days=1).to_date_string().replace("-", "")
            self.tomorrow_ds_nodash_utc8 = self.dt.add(days=1).to_date_string().replace("-", "")

            tz = timezone('Asia/Shanghai')
            ex_date = datetime.datetime(self.execution_date_utc8.year,
                                        self.execution_date_utc8.month,
                                        self.execution_date_utc8.day,
                                        0, 0, 0)
            tz_utc8 = tz.localize(ex_date)
            end_date_up_limit = tz_utc8 + datetime.timedelta(days=1)
            # 执行时间当天开始时间戳，精确到毫秒
            self.execution_date_start_timestamp = int(tz_utc8.timestamp()) * 1000
            # 执行时间当天最晚时间戳，精确到毫秒
            self.execution_date_end_timestamp = int(end_date_up_limit.timestamp()) * 1000 - 1
        else:
            self.execution_date = None
        if "ds_nodash" in kwargs:
            self.ds_nodash = kwargs["ds_nodash"]
        else:
            self.ds_nodash = None

        if "yesterday_ds" in kwargs:
            self.yesterday_ds = kwargs["yesterday_ds"]
        else:
            self.yesterday_ds = None
        if "yesterday_ds_nodash" in kwargs:
            self.yesterday_ds_nodash = kwargs["yesterday_ds_nodash"]
        else:
            self.yesterday_ds_nodash = None

        if "tomorrow_ds" in kwargs:
            self.tomorrow_ds = kwargs["tomorrow_ds"]
        else:
            self.tomorrow_ds = None
        if "tomorrow_ds_nodash" in kwargs:
            self.tomorrow_ds_nodash = kwargs["tomorrow_ds_nodash"]
        else:
            self.tomorrow_ds_nodash = None

        if "dag" in kwargs:
            self.dag = str(kwargs["dag"].dag_id)
        else:
            self.dag = None
        if "task" in kwargs:
            self.task = str(kwargs["task"].task_id)
        else:
            self.task = None

    def print(self):
        print(self.execution_date)


"""
task执行中传递的参数
def python_method(ds, **kwargs):
    print(kwargs)
 kwargs  = 
    {
        'execution_date': < Pendulum[2019 - 01 - 01 T00: 00: 00 + 00: 00] > ,
        'ti': < TaskInstance: test_utils.doit 2019 - 01 - 01 T00: 00: 00 + 00: 00[success] > ,
        'conf': < module 'airflow.configuration'
        from '/var/lib/hive/.local/lib/python3.5/site-packages/airflow/configuration.py' > ,
        'tomorrow_ds_nodash': '20190102',
        'END_DATE': '2019-01-01',
        'task_instance': < TaskInstance: test_utils.doit 2019 - 01 - 01 T00: 00: 00 + 00: 00[success] > ,
        'dag_run': None,
        'tables': None,
        'ds_nodash': '20190101',
        'tomorrow_ds': '2019-01-02',
        'ts': '2019-01-01T00:00:00+00:00',
        'task': < Task(PythonOperator): doit > ,
        'next_ds': None,
        'test_mode': True,
        'params': {},
        'inlets': [],
        'prev_ds': None,
        'macros': < module 'airflow.macros'
        from '/var/lib/hive/.local/lib/python3.5/site-packages/airflow/macros/__init__.py' > ,
        'dag': < DAG: test_utils > ,
        'latest_date': '2019-01-01',
        'templates_dict': None,
        'yesterday_ds_nodash': '20181231',
        'var': {
            'json': None,
            'value': None
        },
        'next_execution_date': None,
        'task_instance_key_str': 'test_utils__doit__20190101',
        'yesterday_ds': '2018-12-31',
        'run_id': None,
        'outlets': [],
        'end_date': '2019-01-01',
        'prev_execution_date': None,
        'ts_nodash': '20190101T000000+0000'
    }
"""


"""
taskc成功/失败结束时返回的参数
def on_success_callback(context, **kwargs):
    print(context)
context = 
{
    'task_instance': < TaskInstance: test_utils.doit 2019 - 01 - 01 T00: 00: 00 + 00: 00[success] > ,
    'prev_ds': None,
    'latest_date': '2019-01-01',
    'END_DATE': '2019-01-01',
    'tables': None,
    'var': {
        'value': None,
        'json': None
    },
    'dag_run': None,
    'end_date': '2019-01-01',
    'macros': < module 'airflow.macros'
    from '/var/lib/hive/.local/lib/python3.5/site-packages/airflow/macros/__init__.py' > ,
    'ds': '2019-01-01',
    'task': < Task(PythonOperator): doit > ,
    'yesterday_ds_nodash': '20181231',
    'tomorrow_ds': '2019-01-02',
    'ds_nodash': '20190101',
    'test_mode': True,
    'dag': < DAG: test_utils > ,
    'tomorrow_ds_nodash': '20190102',
    'yesterday_ds': '2018-12-31',
    'templates_dict': None,
    'params': {},
    'ti': < TaskInstance: test_utils.doit 2019 - 01 - 01 T00: 00: 00 + 00: 00[success] > ,
    'execution_date': < Pendulum[2019 - 01 - 01 T00: 00: 00 + 00: 00] > ,
    'conf': < module 'airflow.configuration'
    from '/var/lib/hive/.local/lib/python3.5/site-packages/airflow/configuration.py' > ,
    'ts_nodash': '20190101T000000+0000',
    'ts': '2019-01-01T00:00:00+00:00',
    'inlets': [],
    'outlets': [],
    'task_instance_key_str': 'test_utils__doit__20190101',
    'run_id': None,
    'next_ds': None,
    'prev_execution_date': None,
    'next_execution_date': None
}
"""