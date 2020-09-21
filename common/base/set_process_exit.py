# -*- coding: utf-8 -*-
# @Time    : 2019/11/19 17:05
# @Author  : wangsong
# @FileName: set_process_exit.py
# @Software: PyCharm
#function info：退出进程



#设置退出
#LevelStatu：red、yellow、green
def set_exit(LevelStatu="",MSG=""):
    if LevelStatu == "red":
      print(MSG)
      #发送告警信息
      #push_msg
      #异常退出
      raise Exception()
    elif LevelStatu == "yellow":
      print(MSG)
      # 发送告警信息
      # push_msg
    else:
      print("Running Succ！！！")
