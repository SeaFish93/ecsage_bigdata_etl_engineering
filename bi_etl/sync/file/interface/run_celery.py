"""

nohup /usr/local/python3/bin/celery -A ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tylerscope worker -l info >/usr/local/python3/lib/python3.7/site-packages/airflow/logs/celery.log 2>&1 &

ps -ef|grep celery|awk '{print $2}'|while read line ;
do
 kill -9 $line
done

nohup /usr/redis/bin/redis-server /usr/redis/bin/redis.conf >/usr/local/python3/lib/python3.7/site-packages/airflow/logs/redis.log 2>&1 &
redis-cli -h 192.168.30.17 -p 9543 -a 1qazXSW2 -n 0 ltrim transcode 0 196
"""