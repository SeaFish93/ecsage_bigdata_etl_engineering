# -*- coding: utf-8 -*-
# @Time    : 2019/11/19 17:05
# @Author  : wangsong
# @FileName: alter_info.py
# @Software: PyCharm
#function info：告警

import datetime

#获取预警信息
def get_create_dag_alert(FileName=None,Log=None,Developer=None):
    system_time = (datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    msg = """
        【FileName】：%s
        【SystemTime】：%s
        【Log】：%s
        【Developer】：%s
    """ % (FileName,system_time,Log,Developer)
    return msg.replace("        ","")

#获取T+1预警信息
def get_alert_info_d(DagId="",TaskId="",SourceTable="",TargetTable="",BeginExecDate="",EndExecDate="",Status="",Log="",Developer=""):
    system_time = (datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    msg = """
        【TaskInstance】：%s.%s
        【SourceTable】：%s
        【TargetTable】：%s
        【BeginExecDate】：%s
        【EndExecDate】：%s
        【SystemTime】：%s
        【Status】：%s
        【Log】：%s
        【Developer】：%s
    """ % (DagId, TaskId, SourceTable,TargetTable,BeginExecDate,EndExecDate,system_time,Status,Log,Developer)
    return msg.replace("        ","")