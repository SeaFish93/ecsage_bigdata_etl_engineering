# -*- coding: utf-8 -*-
# @Time    : 2019/2/21 16:19
# @Author  : luoyh
# @FileName: beeline_operator.py
# @Software: PyCharm

from etl_main.common.base_operator import BaseDB
import os
import time


class BeelineNoSqlDB(BaseDB):
    def __init__(self, host=None, port=None, user=None, password=None, metastore_uris=None):
        super().__init__(host=host, port=port, user=user, password=password)
        self.metastore_uris = metastore_uris
        print("beeline NoSql DB:" + self.metastore_uris)
        #modify by wangsong（source /etc/profile）
        self.conn = "/root/hive/apache-hive-2.3.7-bin/bin/beeline -u 'jdbc:hive2://%s/' -n %s -d org.apache.hive.jdbc.HiveDriver -p '%s'" % (self.metastore_uris, self.user, self.password)
        # self.conn = "/usr/bin/beeline -u 'jdbc:hive2://%s/' -n %s " % (self.metastore_uris, self.user)

    def execute_sql(self, sql, task_name=""):
        t = time.time()
        sql_file = "/tmp/tmp_%s_%s.sql" % (task_name, str(t))
        f = open(sql_file, mode="w")
        #f.write("set hive.server2.logging.operation.level=NONE;\n")
        sql_set = ""
        f.write(sql_set)
        f.write(sql)
        f.flush()
        # add by wangsong（print sql）
        print(sql_set)
        print("beeline exec sql：\n" + sql)
        res = os.system("%s -f %s" % (self.conn, sql_file))
        os.system("rm %s" % sql_file)
        f.close()
        if res != 0:
            print("beeline execute_sql sql Error:" + sql)
            return False
        else:
            return True
        # os.system("beeline -u 'jdbc:hive2://cdh-master2:10000/' -n hive -d org.apache.hive.jdbc.HiveDriver -f %s" % sql_file)

    def get_password(self):
        return self.password


"""
set hive.exec.parallel.thread.number=3;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=256000000;
set mapred.min.split.size.per.rack=256000000;
set hive.exec.reducers.bytes.per.reducer=256000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.parallel=true;
"""
