# -*- coding: utf-8 -*-
# @Time    : 2019/1/14 12:22
# @Author  : wangsong
# @FileName: hive_operator.py
# @Software: PyCharm

import os
import sys
from ecsage_bigdata_etl_engineering.common.base.base_operator import BaseDB
from ecsage_bigdata_etl_engineering.common.operator.mysql.conn_mysql_metadb import EtlMetadata

os.environ['SPARK_HOME'] = "/opt/soft/spark"
os.environ['PYSPARK_SUBMIT_ARGS'] = "--master yarn pyspark-shell"
sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python"))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python/lib/py4j-0.10.7-src.zip"))
from pyspark.sql import SparkSession


class SparkNoSqlDB(BaseDB):
    def __init__(self, host=None, port=None, user=None, metastore_uris=None, app_name="etl"):
        super().__init__(host=host, port=port, user=user)
        self.metastore_uris = host
        self.app_name = app_name
        print("SparkNoSqlDB : " + self.metastore_uris + ", appName:" + self.app_name,",port:"+str(self.port))
        self.conn = SparkSession.builder.master("yarn").appName(app_name) \
             .config("spark.submit.deployMode", "client") \
             .config("hive.metastore.uris", "thrift://%s:%s"%(self.metastore_uris,self.port)) \
             .config("spark.sql.hive.convertMetastoreParquet", "false") \
             .config("spark.sql.crossJoin.enabled", "true") \
             .config("spark.num.executors", "2") \
             .config("spark.executor.cores", "1") \
             .config("spark.executor.memory", "1g") \
             .config("spark.driver.memory", "1g") \
             .config("spark.executor.memoryOverhead", "2048") \
             .config("spark.sql.shuffle.partitions", "2") \
             .config("yarn.nodemanager.vmem-check-enabled", "false") \
             .config("spark.port.maxRetries","1000000") \
             .enableHiveSupport().getOrCreate()

    def __del__(self):
        print("SparkNoSqlDB %s __del__ : do cursor.close()" % self.metastore_uris)
        self.conn.stop()

    def get_connect(self):
        return self.conn

    def spark_conn_close(self):
        print("close spark")
        self.conn.stop()

    def execute_sql(self, sql):
        print(sql)
        try:
            self.conn.sql(sql).show()
        except Exception as e:
            print("spark execute_sql sql Error:" + sql)
            print(e)
            return False
        return True

    def get_count(self, db, tb):
        # return count int
        sql = "select count(1) cnt from %s.%s" % (db, tb)
        print(sql)
        try:
            df = self.conn.sql(sql)
        except Exception as e:
            print("spark get_count sql Error:" + sql)
            print(e)
            return False, None
        return True, df.collect()[0].cnt

    def get_columns(self, sql):
        # 返回查询结果集的列名
        # ['id', 'bank_name', 'country', 'bank_code', 'duitku_bank_code', 'etl_time']
        # sql = str(sql).replace("`", "")
        print(sql)
        try:
            df = self.conn.sql(sql)
        except Exception as e:
            print("spark get_columns sql Error:" + sql)
            print(e)
            return False, None
        return True, df.columns

    # add by wangsong
    #spark sql读取MySQL、MsSQL、hive，返回dataframe
    def get_db(self, SessionHandler="", DBType="", Sql=""):
        print(Sql)
        df = None
        state = False
        etl_md = EtlMetadata()
        ok, get_handle = etl_md.execute_sql(sqlName="get_handle_sql",Parameter={"handle_code": SessionHandler},IsReturnData="Y")
        jdbc_host = get_handle[0][0]
        jdbc_user = get_handle[0][2]
        jdbc_password = get_handle[0][3]
        jdbc_port = get_handle[0][1]
        jdbc_default_db = get_handle[0][4]
        try:
            if DBType == "mysql":
                db_info = BaseDB(port=jdbc_port, host=jdbc_host, user=jdbc_user, password=jdbc_password)
                df = self.conn.read \
                    .format("jdbc") \
                    .option("url", "jdbc:mysql://%s:%s/%s?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8" % (jdbc_host, jdbc_port, jdbc_default_db)) \
                    .option("dbtable", """(""" + Sql + """) t""") \
                    .option("user", jdbc_user) \
                    .option("password", db_info.get_password()) \
                    .option("fetchsize",100000) \
                    .load()
            elif DBType == "mssql":
                db_info = BaseDB(port=jdbc_port, host=jdbc_host, user=jdbc_user, password=jdbc_password)
                df = self.conn.read \
                    .format("jdbc") \
                    .option("url", "jdbc:sqlserver://%s:%s;DatabaseName=%s" % (jdbc_host, jdbc_port, jdbc_default_db)) \
                    .option("dbtable", """(""" + Sql + """) t""") \
                    .option("user", jdbc_user) \
                    .option("password", db_info.get_password()) \
                    .option("fetchsize",10000) \
                    .load()
            elif DBType == "hive":
                df = self.conn.sql(Sql)
            else:
                pass
            state = True
        except Exception as e:
            print("spark get df sql Error:" + Sql)
            print(e)
        finally:
            return state, df

    # add by wangsong
    # spark sql写入MySQL
    def insert_df_2_db(self, db_type="", jdbc_host="", jdbc_user="", jdbc_password="", jdbc_default_db="",jdbc_port="",
                       insert_schema="",insert_db="",insert_table="", read_sql=""):
        print(read_sql)
        df = None
        state = False
        try:
         if db_type == "mysql":
            db_info = BaseDB(port=jdbc_port, host=jdbc_host, user=jdbc_user, password=jdbc_password)
            df = self.conn.sql(read_sql)
            df.write \
              .mode("append") \
              .format("jdbc") \
              .option("url", "jdbc:mysql://%s:%s/%s" % (jdbc_host, 3306, jdbc_default_db)) \
              .option("dbtable", "%s.%s"%(insert_db,insert_table)) \
              .option("user", jdbc_user) \
              .option("password", db_info.get_password()) \
              .option("batchsize", 100000) \
              .save()
            state = True
        except Exception as e:
            print("spark insert db sql Error:" + read_sql)
            print(e)
        finally:
            return state
    #处理文本
    def text(self):
        print(self.conn.read.text().rdd.foreach(lambda x:x))
    
    def get_csv(self,IsHeader="",FileDir=""):
        state = False
        df = None
        if IsHeader == "Y":
            header = "True"
        else:
            header = "False"
        try:
          file = FileDir.split("/")[-1]
          os.system("""hadoop fs -rmr /tmp/jupyter/%s"""%(file))
          os.system("""hadoop fs -put %s /tmp/jupyter/"""%(FileDir))
          df = self.conn.read.options(header=header, inferSchema='True', delimiter=',').csv("/tmp/jupyter/%s"%(file))
          state = True
        except Exception as e:
            print("spark select CSV Error:" + FileDir)
            print(e)
        finally:
            return state,df
