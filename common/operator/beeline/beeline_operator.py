# -*- coding: utf-8 -*-
# @Time    : 2019/2/21 16:19
# @Author  : wangsong
# @FileName: beeline_operator.py
# @Software: PyCharm

from ecsage_bigdata_etl_engineering.common.base.base_operator import BaseDB
import os
import time
import subprocess


class BeelineNoSqlDB(BaseDB):
    def __init__(self, host=None, port=None, user=None, password=None, metastore_uris=None):
        super().__init__(host=host, port=port, user=user, password=password)
        self.metastore_uris = metastore_uris
        print("beeline NoSql DB:" + self.metastore_uris)
        #modify by wangsong（source /etc/profile）
        self.conn = "/opt/hive/apache-hive-2.1.1-bin/bin/beeline -u 'jdbc:hive2://%s' -n %s -d org.apache.hive.jdbc.HiveDriver -p '%s'" % (self.metastore_uris, self.user, self.password)
        #print(self.conn,"#########################################=======================")
        # self.conn = "/usr/bin/beeline -u 'jdbc:hive2://%s/' -n %s " % (self.metastore_uris, self.user)

    #hive数据落地本地
    def execute_sql_result_2_local_file(self,sql="",file_name="",task_name=""):
        config_param = """set mapred.task.timeout=1800000;
                          set mapreduce.map.memory.mb=2048;
                          set hive.auto.convert.join=false;
          """
        exec_sql = config_param + sql
        (res, output) = subprocess.getstatusoutput("""%s --showHeader="false" --outputformat="tsv2" -e "%s">%s"""%(self.conn, exec_sql,file_name))
        if res != 0:
            print("beeline execute_sql_result_2_local_file sql Error:" + sql)
            print("错误日志：%s" % output)
            return False
        else:
            return True

    def execute_sql(self, sql,custom_set_parameter="", task_name=""):
        t = time.time()
        sql_file = "/tmp/tmp_%s_%s.sql" % (task_name, str(t))
        f = open(sql_file, mode="w")
        #f.write("set hive.server2.logging.operation.level=NONE;\n")
        if custom_set_parameter is not None and len(custom_set_parameter) > 0:
            custom_set_parameter
        else:
            custom_set_parameter=""
        sql_set = """
          set mapred.task.timeout=1800000;
          set mapreduce.map.memory.mb=2048;
          set hive.auto.convert.join=false;
        """ + str(custom_set_parameter)
        f.write(sql_set)
        f.write(sql)
        f.flush()
        # add by wangsong（print sql）
        print(sql_set)
        print("beeline exec sql：\n" + sql)
        #res = os.system("%s -f %s" % (self.conn, sql_file))
        (res, output) = subprocess.getstatusoutput("%s -f %s" % (self.conn, sql_file))
        os.system("rm %s" % sql_file)
        f.close()
        if res != 0:
            print("beeline execute_sql sql Error:" + sql)
            print("错误日志：%s"%output)
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
