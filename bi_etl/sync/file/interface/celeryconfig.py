######### import logging
#########
import socket
import logging
#broker use rabbitmq
BROKER_URL = 'redis://:1qazXSW2@192.168.30.17:9543/0'
#backend use redis
CELERY_RESULT_BACKEND = 'redis://:1qazXSW2@192.168.30.17:9543/1'
#use json
CELERY_RESULT_SERIALIZER = 'json'
#task result expires
CELERY_TASK_RESULT_EXPIRES = 60*60*24
#task child killed after 40 times processing
#CELERY_MAX_TASKS_PER_CHILD = 40
#celery worker amount
CELERY_CONCURRENCY = 20
#the amount that a celery worker get task from broker each time
CELERY_PREFETCH_MULTIPLIER = 4
CELERY_TASK_SERIALIZER = 'pickle'
CELERY_RESULT_SERIALIZER = 'pickle'
CELERY_ACCEPT_CONTENT = ['pickle', 'json']
######import logging
######import socket
######
######from celery._state import get_current_task
######
######class Formatter(logging.Formatter):
######    """Formatter for tasks, adding the task name and id."""
######
######    def format(self, record):
######        task = get_current_task()
######        if task and task.request:
######            record.__dict__.update(task_id='%s ' % task.request.id,
######                                   task_name='%s ' % task.name)
######        else:
######            record.__dict__.setdefault('task_name', '')
######            record.__dict__.setdefault('task_id', '')
######        return logging.Formatter.format(self, record)
######
######
######
####### 将日志输出到文件
#hostname = socket.gethostname()
#fh = logging.FileHandler("""/home/ecsage_data/oceanengine/account/celery_worker.log.%s"""%(hostname)) # 这里注意不要使用TimedRotatingFileHandler，celery的每个进程都会切分，导致日志丢失
#root_logger = logging.getLogger() # 返回logging.root
#root_logger.setLevel(logging.DEBUG)
#fh.setFormatter(formatter)
#fh.setLevel(logging.DEBUG)
#root_logger.addHandler(fh)

# 将日志输出到控制台
#### sh = logging.StreamHandler()
#### formatter = Formatter('[%(task_name)s%(task_id)s%(process)s %(thread)s %(asctime)s %(pathname)s:%(lineno)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
#### sh.setFormatter(formatter)
#### sh.setLevel(logging.INFO)
#### root_logger.addHandler(sh)


    #### BROKER_URL = 'redis://:1qazXSW2@192.168.30.17:9543/0'
    #### CELERY_RESULT_BACKEND = 'redis://:1qazXSW2@192.168.30.17:9543/1'
    #### CELERY_TASK_SERIALIZER = 'pickle' # " json从4.0版本开始默认json,早期默认为pickle（可以传二进制对象）
    #### CELERY_RESULT_SERIALIZER = 'pickle'
    #### CELERY_ACCEPT_CONTENT = ['json', 'pickle']
    #### CELERY_ENABLE_UTC = True # 启用UTC时区
    #### CELERY_TIMEZONE = 'Asia/Shanghai' # 上海时区
    #### CELERYD_HIJACK_ROOT_LOGGER = False # 拦截根日志配置
    #### CELERYD_MAX_TASKS_PER_CHILD = 1 # 每个进程最多执行1个任务后释放进程（再有任务，新建进程执行，解决内存泄漏）