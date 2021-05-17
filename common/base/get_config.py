# -*- coding: utf-8 -*-
# @Time    : 2020/5/26 15:28
# @Author  : wangsong12
# @FileName: get_config.py
# @Software: PyCharm

import configparser

from os.path import abspath, dirname

class Conf:
    print(dirname(__file__))
    __CONF_FILE = "/code/bigdata_item_code/ecsage_bigdata_etl_engineering/config/config.ini"
    conf = configparser.RawConfigParser()
    try:
        conf.read(__CONF_FILE,encoding='utf-8')
    except Exception as e:
        print("**** Error: Failed to read config file: %s" % e)
        exit(1)


