"""
#测试：
ps -ef|grep 'oe_test.%h'|awk '{print $2}'|while read line ;
do
 kill -9 $line
done
ps -ef|grep 'tc_test.%h'|awk '{print $2}'|while read line ;
do
 kill -9 $line
done

cd /code/bigdata_item_code/ecsage_bigdata_etl_engineering
git pull
 #巨量
 nohup /usr/local/python3/bin/celery -A ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tylerscope worker --concurrency=5 -l info -n oe_test.%h -Q oe_test >/tmp/oe_celery.log 2>&1 &
 #腾讯
 nohup /usr/local/python3/bin/celery -A ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tylerscope worker --concurrency=5 -l info -n tc_test.%h -Q tc_test >/tmp/tc_celery.log 2>&1 &
 #报表接口
 nohup /usr/local/python3/bin/celery -A ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tylerscope worker --concurrency=5 -l info -n report_test.%h -Q report_test >/tmp/report_celery.log 2>&1 &
 #flower
 nohup /usr/local/python3/bin/flower -A ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tylerscope --broker_api=http://admin:1qazXSW2@192.168.30.130:9548/api/ --port=9544 >/tmp/flower.log 2>&1 &
"""
