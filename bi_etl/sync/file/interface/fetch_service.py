# -*- coding: utf-8 -*-
# @Time    : 2020/01/06 18:04
# @Author  : wangsong
# @FileName: curl.py
# @Software: PyCharm
# function info：分发服务

from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
import math


def get_fetch(MediaType="",Sql="",BeweetFileList=""):
    etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
    get_host_sql = """select ip,user_name,passwd from metadb.request_account_host"""
    ok,host_data = etl_md.get_all_rows(get_host_sql)
    n = 0
    host_num = 0
    host_i = 0
    start_end_list = []
    service_run_num = 5
    for get_data in BeweetFileList:
        start_end_list.append(BeweetFileList[n])
        if len(start_end_list) == service_run_num or len(BeweetFileList) < service_run_num or len(BeweetFileList)-1 == n:
           print("[%s]执行机器" % (host_data[host_i][0]))
           for start_end in start_end_list:
               max = start_end[1]
               min = start_end[0]
               count = max - min
               if n == service_run_num-1:
                   min_n = 0
               else:
                   min_n = 1
               sqls_list = get_run_sql(Sql=Sql, Max=max, Min=min, Count=count, MinN=min_n)
               for sql in sqls_list:
                   print(sql,"###########################################")
           start_end_list = []
           host_i = host_i + 1
        host_num = host_num + 1
        n = n + 1

def get_run_sql(Sql="",Max="",Min="",Count="",MinN="",LeftFilter="",RightFilter=""):
    fcnt = int(Count)
    sql_list = []
    if fcnt > 0:
        fmin = int(Min)
        fmax = int(Max)
        source_cnt = fcnt
        print("min=%s, max=%s, count=%s" % (str(fmin), str(fmax), str(fcnt)))
        if fcnt < 0:
            # 100以下的数据量不用分批跑
            sql_list.clear()
            sql_list.append(Sql)
        else:
            sql_list.clear()
            num_proc = int(fmax) - int(fmin)
            if num_proc > 5:
                # 最多20个进程同时获取数据
                num_proc = 5
            # 每一个进程查询量的增量
            d = math.ceil((int(fmax) - int(fmin) + 1) / num_proc)
            i = 0
            while i < num_proc:
                s_ind = int(fmin) + MinN + i * d
                e_ind = s_ind + d
                if i == num_proc - 1:
                    e_ind = int(fmax) + 1
                sql = Sql + " %s"%(LeftFilter) + " >= " + str(s_ind) + " %s"%(RightFilter) + " < " + str(e_ind)
                sql_list.append(sql)
                #max_min.append([s_ind,e_ind])
                i = i + 1
    return sql_list

#获取分批子账户个数
def get_task_status_sql(MysqlSession="",SelectAccountSql="",AccountCountSql=""):
    #获取子账户
    source_data_sql = SelectAccountSql
    #获取子账户条数
    get_account_count_sql = AccountCountSql
    ok,all_rows = MysqlSession.get_all_rows(get_account_count_sql)
    fcnt = 0
    sql_list = []
    max_min = []
    if all_rows is not None and len(all_rows) > 0:
        fcnt = all_rows[0][0]
    if fcnt > 0:
        fmin = int(all_rows[0][1])
        fmax = int(all_rows[0][2])
        source_cnt = fcnt
        print("min=%s, max=%s, count=%s" % (str(fmin), str(fmax), str(fcnt)))
        if fcnt < 100:
            # 100以下的数据量不用分批跑
            sql_list.clear()
            sql_list.append(source_data_sql)
        else:
            sql_list.clear()
            num_proc = int(fmax) - int(fmin)
            if num_proc > 4:
                # 最多20个进程同时获取数据
                num_proc = 20
            # 每一个进程查询量的增量
            d = math.ceil((int(fmax) - int(fmin) + 1) / num_proc)
            i = 0
            while i < num_proc:
                s_ind = int(fmin) + i * d
                e_ind = s_ind + d
                if i == num_proc - 1:
                    e_ind = int(fmax) + 1
                max_min.append([s_ind,e_ind])
                i = i + 1
    return source_data_sql,max_min