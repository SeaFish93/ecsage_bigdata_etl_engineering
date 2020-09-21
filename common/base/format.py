# -*- coding: utf-8 -*-
# @Time    : 2019/1/8 17:12
# @Author  : wangsong
# @FileName: format.py
# @Software: PyCharm
#20191127 新增获取时间戳参数trx_datetime，用于openpay发送邮件 by wangsong

import datetime
import calendar
import pendulum


def parm_replace(sql, trx_dt):
    trx_yr = trx_dt[0:4]
    trx_month = trx_dt[4:6]
    trx_day = trx_dt[6:8]
    last_day = calendar.monthrange(int(trx_yr), int(trx_month))[1]
    trx_first_day_of_month = trx_yr + trx_month + "01"
    trx_last_day_of_month = trx_yr + trx_month + str(last_day)
    yesterday = datetime.datetime.strptime(trx_dt, '%Y%m%d') + datetime.timedelta(days=-1)
    trx_yesterday = yesterday.strftime('%Y%m%d')
    trx_yesterday_date = yesterday.strftime('%Y-%m-%d')
    trx_10_dt = datetime.datetime.strptime(trx_dt, '%Y%m%d').strftime('%Y-%m-%d')

    return sql.replace("${trx_dt}", trx_dt)\
        .replace("${trx_yr}", trx_yr)\
        .replace("${trx_month}", trx_month)\
        .replace("${trx_day}",trx_day)\
        .replace("${trx_first_day_of_month}", trx_first_day_of_month)\
        .replace("${trx_last_day_of_month}", trx_last_day_of_month)\
        .replace("${trx_yesterday}", trx_yesterday) \
        .replace("${trx_10_dt}", trx_10_dt) \
        .replace("${trx_yesterday_date}", trx_yesterday_date)


def get_trx_dt():
    utc_dt = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
    trx_dt = (utc_dt.date() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    return trx_dt


def replace_placeholder(airflow, txt):
    trx_dt = airflow.ds_nodash_utc8
    trx_yr = airflow.dt.year
    if airflow.dt.month > 9:
        trx_month = str(airflow.dt.month)
    else:
        trx_month = "0"+str(str(airflow.dt.month))
    if airflow.dt.day > 9:
        trx_day = str(airflow.dt.day)
    else:
        trx_day = "0"+str(str(airflow.dt.day))
    dt = airflow.dt
    trx_first_day_of_month = pendulum.datetime(dt.year, dt.month, 1, 0, 0, 0, 0, "Asia/Shanghai").to_date_string().replace("-", "")
    trx_last_day_of_month = pendulum.datetime(dt.year, dt.month, 1, 0, 0, 0, 0, "Asia/Shanghai") \
        .add(months=1).subtract(days=1).to_date_string().replace("-", "")
    yesterday = airflow.yesterday_ds_nodash_utc8
    trx_yesterday_date = airflow.dt.subtract(days=1).to_date_string()
    trx_next_date = airflow.dt.subtract(days=-1).to_date_string()
    trx_next_dt = airflow.dt.subtract(days=-1).to_date_string().replace("-", "")
    trx_date = airflow.dt.to_date_string()
    if int(airflow.execution_date_utc8_str[11:13]) == 20:
      trx_datetime = airflow.dt.add(days=1).to_date_string()
    else:
      trx_datetime = trx_date

    return txt.replace("${trx_dt}", str(trx_dt)) \
        .replace("${trx_yr}", str(trx_yr)) \
        .replace("${trx_month}", str(trx_month)) \
        .replace("${trx_day}", str(trx_day)) \
        .replace("${trx_first_day_of_month}", trx_first_day_of_month) \
        .replace("${trx_last_day_of_month}", trx_last_day_of_month) \
        .replace("${trx_yesterday}", str(yesterday)) \
        .replace("${trx_date}", str(trx_date))\
        .replace("${trx_yesterday_date}", str(trx_yesterday_date))\
        .replace("${trx_next_date}", str(trx_next_date))\
        .replace("${trx_next_dt}", str(trx_next_dt))\
        .replace("${trx_datetime}", str(trx_datetime))


def replace_y_m_d(dt, txt):
    yyyy = dt.year
    if dt.month > 9:
        mm = str(dt.month)
    else:
        mm = "0"+str(str(dt.month))
    if dt.day > 9:
        dd = str(dt.day)
    else:
        dd = "0"+str(str(dt.day))
    return txt.replace("yyyy", str(yyyy)) \
        .replace("mm", str(mm)) \
        .replace("dd", str(dd))


def substr_y_m_d(txt):
    return str(txt).replace("_yyyy", "").replace("_mm", "").replace("_dd", "")

