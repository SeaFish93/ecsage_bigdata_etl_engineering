from ecsage_bigdata_etl_engineering.common.base.etl_thread import EtlThread


def exec_remote_proc(cmd="",arg=None):
  thread_id = arg["cmd"]
  print(thread_id)

def thread():
 th = []
 for i in range(1000):
   thread_id = i
   print(thread_id)
   etl_thread = EtlThread(thread_id=thread_id, thread_name="fetch%d" % (thread_id),
                        my_run=exec_remote_proc,cmd="thread_%s"%(thread_id)
                        )
   etl_thread.start()
   th.append(etl_thread)
 etl_thread.start()
 th.append(etl_thread)

thread()