#broker use redis
BROKER_URL = 'redis://:1qazXSW2@192.168.30.17:9543/0'
#backend use redis
CELERY_RESULT_BACKEND = 'redis://:1qazXSW2@192.168.30.17:9543/1'
#use json
CELERY_RESULT_SERIALIZER = 'json'
#task result expires
CELERY_TASK_RESULT_EXPIRES = 60*60*24
#task child killed after 40 times processing
CELERYD_MAX_TASKS_PER_CHILD = 40
#celery worker amount
#CELERY_CONCURRENCY = 20
CELERYD_CONCURRENCY = 200
#the amount that a celery worker get task from broker each time
CELERYD_PREFETCH_MULTIPLIER = 4
CELERY_TASK_SERIALIZER = 'pickle'
CELERY_RESULT_SERIALIZER = 'pickle'
CELERY_ACCEPT_CONTENT = ['pickle', 'json']
