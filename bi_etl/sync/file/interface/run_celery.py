"""

ps -ef|grep celery|awk '{print $2}'|while read line ;
do
 kill -9 $line
done

cd /root/bigdata_item_code/ecsage_bigdata_etl_engineering
git pull

rm -f /home/airflow/logs/celery.log
nohup /usr/local/python3/bin/celery -A ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tylerscope worker -l info -n report.%h -Q report>/home/airflow/logs/report_celery.log 2>&1 &
nohup /usr/local/python3/bin/celery -A ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tylerscope worker -l info -n oe.%h -Q oe>/home/airflow/logs/oe_celery.log 2>&1 &
tail -f /home/airflow/logs/report_celery.log
tail -f /home/airflow/logs/oe_celery.log

nohup /usr/redis/bin/redis-server /usr/redis/bin/redis.conf >/home/airflow/logs/redis.log 2>&1 &
/usr/redis/bin/redis-cli -h 192.168.30.17 -p 9543 -a 1qazXSW2 -n 0 ltrim transcode 0 196

nohup /usr/local/python3/bin/flower -A ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tylerscope --broker_api=http://root:1qazXSW2@192.168.30.17:9548/api/ --port=9544 >/home/airflow/logs/flower.log 2>&1 &
nohup /usr/local/python3/bin/flower -A ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tylerscope --port=9540 >/home/airflow/logs/flower_test.log 2>&1 &

"""
