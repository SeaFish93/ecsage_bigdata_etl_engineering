# -*- coding: utf-8 -*-
# @Time    : 2019/1/14 14:36
# @Author  : wangsong
# @FileName: column_type.py
# @Software: PyCharm

import re

MYSQL_2_HIVE = {
    "BIGINT": "bigint",
    "BIT": "int",
    "TYPE_INT": "int",
    "DOUBLE": "decimal",
    "FLOAT": "decimal",
    "DECIMAL": "decimal",
    "DATETIME": "string",
    "TIMESTAMP": "string",
    "DATE": "string",
    "OTHER": "string"
}

ES_2_HIVE = {
    "BIGINT": "bigint",
    "BIT": "int",
    "TYPE_INT": "int",
    "DOUBLE": "decimal",
    "FLOAT": "decimal",
    "DECIMAL": "decimal",
    "DATETIME": "timestamp",
    "TIMESTAMP": "timestamp",
    "DATE": "date",
    "OTHER": "string"
}


def get_column_hive_type(column):
    c_d_type = str(column[1]).upper()
    if c_d_type in ["DOUBLE", "FLOAT", "DECIMAL"]:
        c_style = re.match(r'.*(\(.*\)).*', column[3])  ##c_style = re.match(r'.*(\(.*\)).*', column[3], re.L)
        if c_style:
            return MYSQL_2_HIVE[c_d_type] + c_style.group(1)
        return MYSQL_2_HIVE[c_d_type]+"(20,6)"
    elif c_d_type in ["BIGINT", "BIT", "DATETIME", "TIMESTAMP", "DATE"]:
        return MYSQL_2_HIVE[c_d_type]
    elif "INT" in c_d_type:
        return MYSQL_2_HIVE["TYPE_INT"]
    else:
        return MYSQL_2_HIVE["OTHER"]


def get_column_es_2_hive_type(col):
    c_d_type = str(col[1]).upper()
    if c_d_type in ["DOUBLE", "FLOAT", "DECIMAL"]:
        return ES_2_HIVE[c_d_type]+"(20,6)"
    elif c_d_type in ["BIGINT", "BIT", "DATETIME", "TIMESTAMP", "DATE"]:
        return ES_2_HIVE[c_d_type]
    elif "INT" in c_d_type:
        return ES_2_HIVE["TYPE_INT"]
    else:
        return ES_2_HIVE["OTHER"]