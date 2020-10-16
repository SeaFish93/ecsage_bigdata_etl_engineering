# -*- coding: utf-8 -*-
###输入有数组的json数据，返回json
###python2.7的版本
import sys
import json

reload(sys)
sys.setdefaultencoding('utf-8')

for line in sys.stdin:
   datas = line.split("##@@")
   re_data = datas[0]
   data_json = json.loads(datas[1])
   get_data = ""
   for data in data_json:
     json_data = json.dumps(data, ensure_ascii=False)
     get_data = get_data + "##@@" + str(json_data)
   print(re_data + "@@##" +str(get_data))