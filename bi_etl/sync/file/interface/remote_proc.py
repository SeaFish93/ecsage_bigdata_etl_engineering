
import math
import requests
import paramiko
import os
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session
from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread

def exec_remote_proc(HostName="",UserName="",PassWord=""):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # 连接服务器
    ssh.connect(hostname=HostName, port=22, username=UserName, password=PassWord)
    # 执行命令
    stdin, stdout, stderr = ssh.exec_command('ls /root/bigdata_item_code/ecsage_bigdata_etl_engineering')
    # 获取命令结果
    result = stdout.read()
    # 关闭连接
    ssh.close()