# -*- coding: utf-8 -*-
# @Time    : 2019/1/8 17:29
# @Author  : wangsong
# @FileName: mysql_operator.py
# @Software: PyCharm

import pymysql
import os

from yk_bigdata_etl_engineering.common.base.base_operator import BaseDB


class MysqlDB(BaseDB):

    def __init__(self, port=None, host=None, user=None, password=None, default_db="", timeout=60):
        super().__init__(port, host, user, password, default_db, timeout)
        print("MysqlDB : mysql://" + user + ":passwd@" + host + ":" + str(port) + "/" + default_db)

        self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.password,
                                    db=self.default_db, charset='utf8')

    def __del__(self):
        try:
            self.conn.ping()
        except:
            return
        print("MysqlDB %s __del__ : do cursor.close()" % (self.host))
        self.get_cursor().close()
        print("MysqlDB %s __del__ : do conn.close()" % (self.host))
        self.conn.close()

    def get_connect(self):
        # @staticmethod 描述的函数称为静态函数，静态函数可以由类和对象调用，函数中没有隐形参数
        try:
            self.conn.ping()
        except:
            self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.password,
                                        db=self.default_db, charset='utf8')
        return self.conn

    def get_cursor(self):
        conn = self.get_connect()
        return conn.cursor()

    def get_all_rows(self, sql):
        print(sql)
        cursor = self.get_cursor()
        try:
            cursor.execute(sql)
        except Exception as e:
            print("mysql get_all_rows sql Error:" + sql)
            print(e)
            return False, None
        finally: #add by wangsong
            self.conn.commit()
        return True, cursor.fetchall()

    def get_one_row(self, sql):
        print(sql)
        cursor = self.get_cursor()
        try:
            cursor.execute(sql)
        except Exception as e:
            print("mysql get_one_row sql Error:" + sql)
            print(e)
            return False, None
        return True, cursor.fetchone()

    def get_many_rows(self, sql, size=1):
        print(sql)
        cursor = self.get_cursor()
        try:
            cursor.execute(sql)
        except Exception as e:
            print("mysql get_many_rows sql Error:" + sql)
            print(e)
            return False, None
        return True, cursor.fetchmany(size)

    def execute_sql(self, sql):
        print(sql)
        cursor = self.get_cursor()
        try:
            cursor.execute(sql)
        except Exception as e:
            print("mysql get_many_rows sql Error:" + sql)
            print(e)
            return False
        self.conn.commit()
        return True

    def get_table_column(self, db, tb):
        sql = """
SELECT t.COLUMN_NAME              AS c_name,
       t.DATA_TYPE                AS c_d_type,
       t.character_maximum_length AS c_d_len,
       t.COLUMN_TYPE              AS c_type
  FROM information_schema.COLUMNS t
 WHERE t.TABLE_SCHEMA = '%s'
   AND t.TABLE_NAME = '%s'
""" % (db, tb)
        return self.get_all_rows(sql)

    def get_unique_keys(self, db, tb):
        sql = "show index from %s.%s" % (db, tb)
        # columns为已排序好的tuple（元组）
        ok, columns = self.get_all_rows(sql)
        key_name = ""
        unique_keys = []
        for ind in columns:
            if ind[1] == 0:
                if key_name == "" or key_name == ind[2]:
                    key_name = ind[2]
                    unique_keys.append(ind[4])
                if key_name != "" and key_name != ind[2]:
                    # 返回第一组唯一键，表可能拥有多组唯一建
                    break
        return unique_keys
    #获取索引列 add by wangsong
    def get_indexs(self, db, tb):
        sql = "show index from %s.%s" % (db, tb)
        return self.get_all_rows(sql)

    def get_count(self, db, tb):
        # return count int
        sql = "select count(1) a from %s.%s" % (db, tb)
        print(sql)
        cursor = self.get_cursor()
        try:
            cursor.execute(sql)
        except Exception as e:
            print("mysql get_count sql Error:" + sql)
            print(e)
            return False, None
        return True, cursor.fetchone()[0]

    def select_data_to_local_file(self, sql=None, filename=None, arg=None):
        if arg:
            sql = arg["sql"]
            filename = arg["filename"]
        mysql_conn = "mysql -h'%s' -P%d -u'%s' -p'%s' -Ne " % (self.host, self.port, self.user, self.password)
        result = os.system(mysql_conn + '"%s" > %s' % (sql, filename))
        if result == 0:
            return True
        else:
            print("mysql select_data_to_local_file Error:" + sql)
            return False
