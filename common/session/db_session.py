# -*- coding: utf-8 -*-
# @Time    : 2019/11/19 17:05
# @Author  : wangsong
# @FileName: db_session.py
# @Software: PyCharm
#function info：会话连接

#创建DB会话
def set_db_session(SessionType="",SessionHandler="",AppName=""):
    from etl_main.common.conn_metadb import EtlMetadata
    etl_meta = EtlMetadata()
    ok, get_handle = etl_meta.execute_sql(sqlName="get_handle_sql", Parameter={"handle_code": SessionHandler},IsReturnData="Y")
    if SessionType == "hive":
      from etl_main.common.hive_operator import HiveNoSqlDB
      # 创建hive连接session
      session = HiveNoSqlDB(port=get_handle[0][1],
                            host=get_handle[0][0],
                            user=get_handle[0][2],
                            password=get_handle[0][3],
                            default_db=get_handle[0][4])
    elif SessionType == "beeline":
      from etl_main.common.beeline_operator import BeelineNoSqlDB
      session = BeelineNoSqlDB(port=get_handle[0][1],
                               host=get_handle[0][0],
                               user=get_handle[0][2],
                               password=get_handle[0][3],
                               metastore_uris="master:10000")
    elif SessionType == "spark":
      from etl_main.common.spark_operator import SparkNoSqlDB
      session = SparkNoSqlDB(port=get_handle[0][1],
                              host=get_handle[0][0],
                              user=get_handle[0][2],
                              metastore_uris="master:10000",
                              app_name=AppName)
    elif SessionType == "mysql":
      from etl_main.common.mysql_operator import MysqlDB
      session = MysqlDB(port=get_handle[0][1],
                        host=get_handle[0][0],
                        user=get_handle[0][2],
                        password=get_handle[0][3],
                        default_db=get_handle[0][4])
    elif SessionType == "oss":
      pass
      ##from etl_main.common.oss_operator import OSS
      ##session = OSS(host=conf.get(SOURCE_TYPE[SessionHandler]["config"], "host"),
      ##              access_key_id=conf.get(SOURCE_TYPE[SessionHandler]["config"], "accesskeyid"),
      ##              access_key_secret=conf.get(SOURCE_TYPE[SessionHandler]["config"], "accesskeysecret"),
      ##              bucket=conf.get(SOURCE_TYPE[SessionHandler]["config"], "bucket"))
    else:
      session = None

    return session
