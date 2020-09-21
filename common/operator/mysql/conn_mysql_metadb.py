# -*- coding: utf-8 -*-
# @Time    : 2020/5/26 15:28
# @Author  : wangsong12
# @FileName: etl_metadata.py
# @Software: PyCharm

from common.base.get_config import Conf
from common.operator.mysql.mysql_operator import MysqlDB
from sql.etl_meta.etl_metadata import get_sql


class EtlMetadata:
    def __init__(self):
        self.source_nm = "EtlDB"
        self.conf = Conf().conf
        self.session = MysqlDB(host=self.conf.get(self.source_nm, "host"),
                               port=self.conf.get(self.source_nm, "port"),
                               user=self.conf.get(self.source_nm, "user"),
                               password=self.conf.get(self.source_nm, "password"))

      #  self.SENSITIVE_COLUMN = self.get_sensitive_column()

    #获取敏感字段
    def get_sensitive_column(self):
        sensitive_column = {}
        ok, columns = self.execute_sql(sqlName="get_sensitive_column_sql",Parameter={},IsReturnData="Y")
        if ok:
            for column in columns:
                if column[0] in sensitive_column:
                    if isinstance(sensitive_column[column[0]], dict):
                        pass
                    else:
                        sensitive_column[column[0]] = {}
                else:
                    sensitive_column[column[0]] = {}

                if column[1] in sensitive_column[column[0]]:
                    if isinstance(sensitive_column[column[0]][column[1]], list):
                        pass
                    else:
                        sensitive_column[column[0]][column[1]] = []
                else:
                    sensitive_column[column[0]][column[1]] = []

                if sensitive_column[column[0]][column[1]].count(column[2]) > 0:
                    pass
                else:
                    sensitive_column[column[0]][column[1]].append(str(column[2]).lower())
        return sensitive_column
    # 获取元数据
    def execute_sql(self, sqlName=None, Parameter={},IsReturnData="Y"):
        n = 0
        if len(Parameter) > 0:
          for get_value in Parameter.items():
              get_keys = "##{" + get_value[0] + "}##"
              get_values = get_value[1]
              if n == 0:
                sql = str(get_sql(sqlName=sqlName)).replace(get_keys,get_values)
              else:
                sql = sql.replace(get_keys,str(get_values))
              n = n + 1
        else:
          sql = get_sql(sqlName=sqlName)
        if IsReturnData == "Y":
           data = self.session.get_all_rows(sql)
        else:
           data = self.session.execute_sql(sql)
        return data

    def get_rows_info(self,sql=None):
        return self.session.get_all_rows(sql)

#etl_meta = EtlMetadata()
#ok, get_handle = etl_meta.execute_sql(sqlName="get_handle_sql", Parameter={"handle_code": "etl_metadb"},IsReturnData="Y")
#print(get_handle[0][1])
