from kombu import Queue, Exchange
#broker use redis 测试库
BROKER_URL = 'amqp://root:1qazXSW2@192.168.30.127:9549/myvhost'
#backend use redis 测试库
CELERY_RESULT_BACKEND = 'amqp://root:1qazXSW2@192.168.30.127:9549/myvhost1'
#use json
#CELERY_RESULT_SERIALIZER = 'json'
#task result expires
CELERY_TASK_RESULT_EXPIRES = 60*60*24
#task child killed after 40 times processing
CELERYD_MAX_TASKS_PER_CHILD = 40
#celery worker amount
#CELERY_CONCURRENCY = 20
CELERYD_CONCURRENCY = 10
#the amount that a celery worker get task from broker each time
CELERYD_PREFETCH_MULTIPLIER = 4
CELERY_TASK_SERIALIZER = 'pickle'
CELERY_RESULT_SERIALIZER = 'pickle'
CELERY_ACCEPT_CONTENT = ['pickle', 'json']

