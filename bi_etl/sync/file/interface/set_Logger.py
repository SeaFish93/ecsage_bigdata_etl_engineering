# coding=utf8

import os
import unittest
import logging
from logging.handlers import RotatingFileHandler

if os.name == 'posix':
  from cloghandler import ConcurrentRotatingFileHandler

format_dict = {
  1: logging.Formatter('%(message)s'),
  2: logging.Formatter('%(message)s'),
  3: logging.Formatter('%(message)s'),
  4: logging.Formatter('%(message)s'),
  5: logging.Formatter('%(message)s'),
}


class LogLevelException(Exception):
  def __init__(self, log_level):
    err = '设置的日志级别是 {0}， 设置错误，请设置为1 2 3 4 5 范围的数字'.format(log_level)
    Exception.__init__(self, err)


class LogManager(object):
  """
  一个日志类，用于创建和捕获日志，支持将日志打印到控制台打印和写入日志文件。
  """

  def __init__(self, logger_name=None):
    """
    :param logger_name: 日志名称，当为None时候打印所有日志
    """
    self.logger = logging.getLogger(logger_name)
    self._logger_level = None
    self._is_add_stream_handler = None
    self._log_path = None
    self._log_filename = None
    self._log_file_size = None
    self._formatter = None

  def get_logger_and_add_handlers(self, log_level_int=1, is_add_stream_handler=True, log_path=None, log_filename=None,
                                  log_file_size=8000):
    """
   :param log_level_int: 日志输出级别，设置为 1 2 3 4 5，分别对应输出DEBUG，INFO，WARNING，ERROR,CRITICAL日志
   :param is_add_stream_handler: 是否打印日志到控制台
   :param log_path: 设置存放日志的文件夹路径
   :param log_filename: 日志的名字，仅当log_path和log_filename都不为None时候才写入到日志文件。
   :param log_file_size :日志大小，单位M，默认10M
   :type logger_name :str
   :type log_level_int :int
   :type is_add_stream_handler :bool
   :type log_path :str
   :type log_filename :str
   :type log_file_size :int
   """
    self.__check_log_level(log_level_int)
    self._logger_level = self.__transform_logger_level(log_level_int)
    self._is_add_stream_handler = is_add_stream_handler
    self._log_path = log_path
    self._log_filename = log_filename
    self._log_file_size = log_file_size
    self._formatter = format_dict[log_level_int]
    self.__set_logger_level()
    self.__add_handlers()
    return self.logger

  def get_logger_without_handlers(self):
    """返回一个不带hanlers的logger"""
    return self.logger

  def __set_logger_level(self):
    self.logger.setLevel(self._logger_level)

  @staticmethod
  def __check_log_level(log_level_int):
    if log_level_int not in [1, 2, 3, 4, 5]:
      raise LogLevelException(log_level_int)

  @staticmethod
  def __transform_logger_level(log_level_int):
    logger_level = None
    if log_level_int == 1:
      logger_level = logging.DEBUG
    elif log_level_int == 2:
      logger_level = logging.INFO
    elif log_level_int == 3:
      logger_level = logging.WARNING
    elif log_level_int == 4:
      logger_level = logging.ERROR
    elif log_level_int == 5:
      logger_level = logging.CRITICAL
    return logger_level

  def __add_handlers(self):
    if self._is_add_stream_handler:
      for h in self.logger.handlers:
        if isinstance(h, logging.StreamHandler):
          break
      else:
        self.__add_stream_handler()
    if all([self._log_path, self._log_filename]):
      for h in self.logger.handlers:
        if os.name == 'nt':
          if isinstance(h, RotatingFileHandler):
            break
        if os.name == 'posix':
          if isinstance(h, (RotatingFileHandler, ConcurrentRotatingFileHandler)):
            break

      else:
        self.__add_file_handler()

  def __add_stream_handler(self):
    """
    日志显示到控制台
    """
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(self._logger_level)
    stream_handler.setFormatter(self._formatter)
    self.logger.addHandler(stream_handler)
    self.logger.fatal = False

  def __add_file_handler(self):
    """
    日志写入日志文件
    """
    if not os.path.exists(self._log_path):
      os.makedirs(self._log_path)
    log_file = os.path.join(self._log_path, self._log_filename)
    os_name = os.name
    rotate_file_handler = None
    if os_name == 'nt':
      # windows下用这个，非进程安全
      rotate_file_handler = RotatingFileHandler(log_file, mode="a", maxBytes=self._log_file_size * 1024 * 1024,
                                                backupCount=10,
                                                encoding="utf-8")
    if os_name == 'posix':
      # linux下可以使用ConcurrentRotatingFileHandler，进程安全的日志方式
      rotate_file_handler = ConcurrentRotatingFileHandler(log_file, mode="a",
                                                          maxBytes=self._log_file_size * 1024 * 1024,
                                                          backupCount=10, encoding="utf-8")
    rotate_file_handler.setLevel(self._logger_level)
    rotate_file_handler.setFormatter(self._formatter)
    self.logger.addHandler(rotate_file_handler)
    self.logger.propagate = True
