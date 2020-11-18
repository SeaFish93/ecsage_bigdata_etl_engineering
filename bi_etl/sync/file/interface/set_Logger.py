
import logging
from logging import handlers

class Logger(object):
    level_relations = {
        'info':logging.INFO
    }#日志级别关系映射
    def __init__(self,filename,level='info',when='D',backCount=3,fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
        self.logger = logging.getLogger(filename)
        format_str = logging.Formatter(fmt)#设置日志格式
        self.logger.setLevel(self.level_relations.get(level))#设置日志级别
        th = handlers.TimedRotatingFileHandler(filename=filename,when=when,backupCount=backCount,encoding='utf-8')#往文件里写入#指定间隔时间自动生成文件的处理器
        #实例化TimedRotatingFileHandler
        th.setFormatter(format_str)#设置文件里写入的格式
        self.logger.addHandler(th)
#####if __name__ == '__main__':
#####    log = Logger('all.log',level='info')
#####    log.logger.info('info')
#####    #Logger('error.log', level='error').logger.error('error')