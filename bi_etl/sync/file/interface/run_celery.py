"""

nohup /usr/local/python3/bin/celery -A ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tylerscope worker -l info >/usr/local/python3/lib/python3.7/site-packages/airflow/logs/celery.log 2>&1 &

ps -ef|grep celery|awk '{print $2}'|while read line ;
do
 kill -9 $line
done


"""