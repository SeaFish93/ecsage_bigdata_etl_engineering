# -*- coding: utf-8 -*-
# @Time    : 2019/1/30 16:34
# @Author  : wangsong
# @FileName: etl_thread.py
# @Software: PyCharm

import threading
exitFlag = 0


class EtlThread (threading.Thread):
    def __init__(self, thread_id, thread_name, **kwargs):
        threading.Thread.__init__(self)
        self.threadID = thread_id
        self.thread_name = thread_name
        self.arg = kwargs
        self.arg["threadName"] = self.thread_name

    def run(self):
        print("开始线程：" + self.thread_name)
        self.arg["my_run"](arg=self.arg)
        print("退出线程：" + self.thread_name)
