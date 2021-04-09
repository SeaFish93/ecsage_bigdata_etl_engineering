#构建词库
#算法n-gram：选取两个统计值：互信息、熵作为依据分词指标
#互信息：词串内部的结合紧密程度
#左右熵：词串外部的左右边界度量

import re
from collections import Counter
import numpy as np
import os
import datetime
from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.tasks import PMI

def ngram_words(file, ngram_cont):
    words = []
    for i in range(1, ngram_cont):
        #添加指定的n元词数
        words.extend([file[j:j + i] for j in range(len(file) - i + 1)])
    #统计词频（1...n字）
    words_fre = dict(Counter(words))
    return words_fre


def PMI_test(words_fre, pmi_threshold):
    """
    凝固度：min{P(abc)/P(ab)P(c),P(abc)/P(a)P(bc)}
    """
    #os.system(""" echo "总pmi词：%s">>/root/wangsong/data.pmi.log """ % (len(words_fre)))
    for i in words_fre:
        celery_task_id = PMI.delay(words_fre=words_fre,
                                     pmi_threshold=pmi_threshold,i=i)
        os.system(""" echo "%s">>/root/wangsong/celery_task_id.log """ % (celery_task_id))


def calculate_entropy(list):
    """
    左熵计算方法:对一个候选词左边所有可能的词以及词频，计算信息熵，然后求和
    右熵计算方法:对一个候选词左边所有可能的词以及词频，计算信息熵，然后求和
    """
    entropy_dic = dict(Counter(list))  # 统计词频
    entropy = (-1) * sum([entropy_dic.get(i) / len(list) * np.log2(entropy_dic.get(i) / len(list)) for i in entropy_dic])
    return entropy

def Entropy_left_right(words, text, ent_threshold):
    for word in words:
        try:
            #获取新词在文章中的前后位置的字并计算其左右熵
            left_right_words = re.findall('(.)%s(.)' % word, text)
            left_words = [i[0] for i in left_right_words]
            left_entropy = calculate_entropy(left_words)
            right_words = [i[1] for i in left_right_words]
            right_entropy = calculate_entropy(right_words)
            if min(left_entropy, right_entropy) > ent_threshold:
                #符合规则，落地文件
                os.system(""" echo "%s,%s,%s">>/root/wangsong/data.data.log """ % (word,min(left_entropy, right_entropy),ent_threshold))
        except:
            pass

if __name__ == '__main__':
   os.system(""" rm -rf /root/wangsong/data.pmi.log """)
   os.system(""" rm -rf /root/wangsong/celery_task_id.log """)
   os.system(""" echo "%s">/root/wangsong/data.data.log """ % ("开始时间：%s" % (datetime.datetime.now().strftime('%F %T'))))
   stop_word = ['（','）','【', '】', ')', '(', '、', '，', '“', '”', '。', '\n', '《', '》', ' ', '-', '！', '？', '.', '\'', '[', ']', '：',
                '/', '.', '"', '\u3000', '’', '．', ',', '…', '?']
   #获取停顿词列表
   stop_word_file = """/root/wangsong/stop_words.txt"""
   stop_word_list = []
   with open(stop_word_file) as lines:
       array = lines.readlines()
       for data in array:
           stop_word_list.append(data.strip('\n'))
   stop_word_list.extend(stop_word)
   stop_word_list = list(set(stop_word_list))

   #语料
   with open("/root/wangsong/tian.txt", 'r', encoding='utf8') as f:
       text = f.read()
   for i in stop_word_list:
       text = text.replace(i, "")
   ngram = 5
   PMI_threshold = 0.0005
   ent_threshold = 1
   print("总字数：%s"%(len(text)))
   os.system(""" echo "%s">>/root/wangsong/data.data.log """%("总字数：%s"%(len(text))))
   #词频
   words_fre = ngram_words(text, ngram)
   #凝固度
   PMI_test(words_fre, PMI_threshold)
   #左右熵
   #Entropy_left_right(new_words, text, ent_threshold)
   #print(result)
   #os.system(""" echo "%s">>/root/wangsong/data.data.log """ % ("结束时间：%s" % (datetime.datetime.now().strftime('%F %T'))))
