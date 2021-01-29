from __future__ import absolute_import, unicode_literals
from celery import Celery, platforms
import sys,os

platforms.C_FORCE_ROOT = True

sys.path.append(os.path.abspath("."))


app = Celery('ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks',include=['ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks'])
app.config_from_object('ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.celeryconfig')
app.autodiscover_tasks(packages='ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface',related_name='tasks')

if __name__ == '__main__':
    app.start()
