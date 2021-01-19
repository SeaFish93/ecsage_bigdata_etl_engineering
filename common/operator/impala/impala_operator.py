# -*- coding: utf-8 -*-
# @Time    : 2019/1/14 12:22
# @Author  : wangsong
# @FileName: impala_operator.py
# @Software: PyCharm

from ecsage_bigdata_etl_engineering.common.base.base_operator import BaseDB
import impala.dbapi as pyimpala


class ImpalaNoSqlDB(BaseDB):
    def __init__(self, host=None, port=None, user=None, password=None, default_db=None, metastore_uris=None):
        super().__init__(host=host, port=port, user=user, password=password)
        print("Impala NoSql DB:" + host + ":" + str(port) + "@" + user)
        self.metastore_uris = metastore_uris
        self.default_db = default_db
        self.cursor = None
        print("get connection")
        self.conn = pyimpala.connect(host=self.host, port=self.port, username=self.user, database=self.default_db,password=self.password)

    def __del__(self):
        print("ImpalaNoSqlDB %s __del__ : do cursor.close()" % self.host)
        if self.cursor is not None:
            self.cursor.close()
        print("ImpalaNoSqlDB %s __del__ : do conn.close()" % self.host)
        self.conn.close()

    def get_connect(self):
        pass

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
            print("impala get_all_rows sql Error:" + sql)
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
            print("impala get_one_row sql Error:" + sql)
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
            print("impala execute_sql sql Error:" + sql)
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
            print("impala execute_sql sql Error:" + sql)
            print(e)
            return False
        return True
