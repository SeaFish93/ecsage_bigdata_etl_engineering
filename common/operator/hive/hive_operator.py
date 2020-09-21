# -*- coding: utf-8 -*-
# @Time    : 2019/1/14 12:22
# @Author  : wangsong
# @FileName: hive_operator.py
# @Software: PyCharm

from etl_main.common.base_operator import BaseDB
from pyhive import hive


class HiveNoSqlDB(BaseDB):
    def __init__(self, host=None, port=None, user=None, password=None, default_db=None, metastore_uris=None):
        super().__init__(host=host, port=port, user=user, password=password)
        print("Hive NoSql DB:" + host + ":" + str(port)+"@"+user)
        self.metastore_uris = metastore_uris
        self.default_db = default_db
        # self.conn = None
        self.cursor = None
        print("get connection")
        conf = {
            "hive.server2.session.check.interval": "7200000",
            "hive.server2.idle.operation.timeout": "7200000",
            "hive.server2.idle.session.timeout": "7200000",
           # "mapreduce.job.queuename": "root.batch.etl",
            # "mapreduce.map.memory.mb": "8000",
            # "mapreduce.map.java.opts": "-Xmx7200m",
            # "mapreduce.reduce.memory.mb": "8000",
            # "mapreduce.reduce.java.opts": "-Xmx7200m",
            # "hive.exec_script.parallel": "true",
            # "hive.exec_script.parallel.thread.number": "3",
            # "mapred.max.split.size": "256000000",
            # "mapred.min.split.size.per.node": "256000000",
            # "mapred.min.split.size.per.rack": "256000000",
            # "hive.exec_script.reducers.bytes.per.reducer": "256000000",
            # "hive.input.format": "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat"
            # "mapreduce.job.max.split.locations": "50"
        }
        print(conf)
        #self.conn = hive.Connection(host=self.host, port=self.port, username=self.user, database=self.default_db,
        #                            password=self.password, configuration=conf, auth='CUSTOM')
        self.conn = hive.Connection(host='master', port=10000, username="hive",password='11@', auth='CUSTOM')

    def __del__(self):
        print("HiveNoSqlDB %s __del__ : do cursor.close()" % self.host)
        self.cursor.close()
        print("HiveNoSqlDB %s __del__ : do conn.close()" % self.host)
        self.conn.close()

    def get_connect(self):
        pass
        """
        try:
            sql = "select 1"
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
        except Exception as e:
            print("get connection")
            conf = {
                "hive.server2.session.check.interval": "7200000",
                "hive.server2.idle.operation.timeout": "7200000",
                "hive.server2.idle.session.timeout": "7200000",
                "mapreduce.job.queuename": "root.batch.etl",
                #"mapreduce.map.memory.mb": "8000",
                #"mapreduce.map.java.opts": "-Xmx7200m",
                #"mapreduce.reduce.memory.mb": "8000",
                #"mapreduce.reduce.java.opts": "-Xmx7200m",
                #"hive.exec_script.parallel": "true",
                #"hive.exec_script.parallel.thread.number": "3",
                #"mapred.max.split.size": "256000000",
                #"mapred.min.split.size.per.node": "256000000",
                #"mapred.min.split.size.per.rack": "256000000",
                #"hive.exec_script.reducers.bytes.per.reducer": "256000000",
                #"hive.input.format": "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat"
                #"mapreduce.job.max.split.locations": "50"
            }
            print(conf)
            self.conn = hive.Connection(host=self.host, port=self.port, username=self.user, database=self.default_db, password=self.password, configuration=conf, auth='LDAP')
            #self.conn = hive.Connection(host=self.host, port=self.port, username=self.user, database=self.default_db, configuration=conf)
        """

    def get_cursor(self):
        self.cursor = self.conn.cursor()

    def get_all_rows(self, sql):
        try:
            self.get_cursor()
            cursor = self.cursor
            # sql = str(sql).replace("`", "")
            cursor.execute(sql)
            rows = cursor.fetchall()
        except Exception as e:
            print("hive get_all_rows sql Error:" + sql)
            print(e)
            return False, None
        return True, rows

    def get_one_row(self, sql):
        self.get_cursor()
        cursor = self.cursor
        try:
            # sql = str(sql).replace("`", "")
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print("hive get_one_row sql Error:" + sql)
            print(e)
            return False, None
        return True, row

    def get_many_rows(self, sql, size=1):
        self.get_cursor()
        cursor = self.cursor
        try:
            cursor.execute(sql)
            rows = cursor.fetchmany(size)
        except Exception as e:
            print("hive execute_sql sql Error:" + sql)
            print(e)
            return False, None
        return True, rows

    def execute_sql(self, sql):
        try:
            self.get_cursor()
            cursor = self.cursor
            # sql = str(sql).replace("`", "")
            cursor.execute(sql)
        except Exception as e:
            print("hive execute_sql sql Error:" + sql)
            print(e)
            return False
        return True

    def create_table(self, ):
        create_sql = ""
        pass
    #append by wangsong（获取hive表字段）
    def get_column_info(self, db,table):
        self.get_cursor()
        num = 0
        sql = 'desc {db}.{table}'.format(db=db, table=table)
        info = ""
        cursor = self.cursor
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            column_num = len(rows)
            for item in rows:
              num = num+1
              if column_num == num:
                info = info+item[0]
              else:
                info = info + item[0] + ","
        except Exception as e:
            print("hive get_column_info sql Error:" + sql)
            print(e)
            return False, None
        return True, rows

    #add by wangsong（判断hive表是否存在）
    def get_table_state(self,db=None,table=None):
        result = False
        sql = """show tables in %s like '%s'""" % (db, table)
        try:
           ok, get_row = self.get_all_rows(sql)
           if ok and len(get_row) > 0:
               result = True
        except Exception as e:
            print("hive exec_script sql Error:" + sql)
            print(e)
            return result
        return result


    
    def get_count(self, db, tb):
        # return count int
        self.get_cursor()
        sql = "select count(1) cnt from %s.%s" % (db, tb)
        cursor = self.cursor
        try:
            cursor.execute(sql)
            cnt = cursor.fetchone()[0]
        except Exception as e:
            print("hive get_count sql Error:" + sql)
            print(e)
            return False, None
        return True, cnt


