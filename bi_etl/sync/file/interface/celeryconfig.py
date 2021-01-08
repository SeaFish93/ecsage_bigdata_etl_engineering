from kombu import Queue, Exchange
#broker use redis
BROKER_URL = 'redis://:1qazXSW2@192.168.30.17:9543/0'
#backend use redis
CELERY_RESULT_BACKEND = 'redis://:1qazXSW2@192.168.30.17:9543/1'
#use json
#CELERY_RESULT_SERIALIZER = 'json'
#task result expires
CELERY_TASK_RESULT_EXPIRES = 60*60*24
#task child killed after 40 times processing
CELERYD_MAX_TASKS_PER_CHILD = 40
#celery worker amount
#CELERY_CONCURRENCY = 20
CELERYD_CONCURRENCY = 300
#the amount that a celery worker get task from broker each time
CELERYD_PREFETCH_MULTIPLIER = 4
CELERY_TASK_SERIALIZER = 'pickle'
CELERY_RESULT_SERIALIZER = 'pickle'
CELERY_ACCEPT_CONTENT = ['pickle', 'json']

CELERY_QUEUES = (
Queue('report', Exchange('report'), routing_key='report', consumer_arguments={'x-priority': 100}),
Queue('oe', Exchange('oe'), routing_key='oe', consumer_arguments={'x-priority': 10}),
Queue('tc', Exchange('tc'), routing_key='tc', consumer_arguments={'x-priority': 10}),
Queue('default', Exchange('default'), routing_key='default'),
)
CELERY_DEFAULT_QUEUE = 'default'
CELERY_DEFAULT_EXCHANGE = 'default'
CELERY_DEFAULT_ROUTING_KEY = 'default'
CELERY_ROUTES = {
# -- HIGH PRIORITY QUEUE -- #
'app.tasks.analysis_main_3': {'queue': 'for_q_type3'},
# -- LOW PRIORITY QUEUE -- #
'app.tasks.analysis_main_12': {'queue': 'for_q_type12'},
'app.tasks.analysis_main': {'queue': 'default'},
}