from kombu import Queue, Exchange

BROKER_URL = 'amqp://admin:1qazXSW2@192.168.30.130:9549//sync_broker'
#CELERY_RESULT_BACKEND = 'file:///data/ecsage_data/celery/sync_backend'
CELERY_RESULT_BACKEND = 'db+mysql://root:06D567130266EB33098B9F@192.168.30.5:13306/sync'
#use json
#CELERY_RESULT_SERIALIZER = 'json'
#task result expires
CELERY_RESULT_EXPIRES = 60*60*24
#task child killed after 40 times processing
CELERYD_MAX_TASKS_PER_CHILD = 5
#celery worker amount
#CELERY_CONCURRENCY = 20
CELERYD_CONCURRENCY = 300
#the amount that a celery worker get task from broker each time
CELERYD_PREFETCH_MULTIPLIER = 4
CELERY_TASK_SERIALIZER = 'pickle'
CELERY_RESULT_SERIALIZER = 'pickle'
CELERY_ACCEPT_CONTENT = ['pickle', 'json']

#CELERY_RESULT_DB_SHORT_LIVED_SESSIONS = False
CELERY_ENABLE_UTC = False
TIME_ZONE = 'Asia/Shanghai'
CELERY_TIMEZONE = TIME_ZONE

BROKER_HEARTBEAT = 65535
# 任务失败或超时自动确认，默认为True
CELERY_ACKS_ON_FAILURE_OR_TIMEOUT=False
# 任务完成之后再确认
CELERY_ACKS_LATE=True
# worker进程崩掉之后拒绝确认
CELERY_REJECT_ON_WORKER_LOST=True
CELERY_QUEUES = (
Queue('report', Exchange('report'), routing_key='report', consumer_arguments={'x-priority': 10}),
Queue('oe', Exchange('oe'), routing_key='oe', consumer_arguments={'x-priority': 100}),
Queue('tc', Exchange('tc'), routing_key='tc', consumer_arguments={'x-priority': 100})
)
CELERY_DEFAULT_QUEUE = 'default'
CELERY_DEFAULT_EXCHANGE = 'default'
CELERY_DEFAULT_ROUTING_KEY = 'default'
CELERY_ROUTES = {
'ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks.get_not_page_tc': {'queue': 'tc'},
'ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks.get_targeting_pages_tc': {'queue': 'tc'},
'ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks.get_pages_tc': {'queue': 'tc'},
'ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks.get_pages': {'queue': 'oe'},
'ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks.get_not_page': {'queue': 'oe'},
'ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks.get_oe_async_tasks_data_return': {'queue': 'oe'},
'ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks.get_oe_create_async_tasks': {'queue': 'oe'},
'ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks.get_oe_status_async_tasks': {'queue': 'oe'},
'ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks.get_service_page_data': {'queue': 'oe'},
'ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks.get_service_data': {'queue': 'oe'},
'ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks.get_web_interface_data': {'queue': 'report'}
}
