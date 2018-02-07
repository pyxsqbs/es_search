#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ConfigParser import SafeConfigParser
import os
from utils import logger

""" 获取环境配置 """

__author__ = '秦宝帅'


class GetEnvConf(object):
    __slots__ = ('__host', '__port', '__db_host', '__db_user', '__db_pwd', '__db_db')  # 用tuple定义允许绑定的属性名称
    config = SafeConfigParser()
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config', 'app.conf')
    config.read(path)
    env_dict = dict()
    env_dict['host'] = config.get('server', 'host')
    env_dict['port'] = config.get('server', 'port')
    env_dict['db_host'] = config.get('database', 'db_host')
    env_dict['db_user'] = config.get('database', 'db_user')
    env_dict['db_pwd'] = config.get('database', 'db_pwd')
    env_dict['db_db'] = config.get('database', 'db_db')

    if not os.path.exists("logs"):
        os.mkdir("logs")
    log_name = os.path.join(os.getcwd(), "logs", config.get("log", "log_name"))
    # In debug mode, we want to keep the stream handler
    logger.set_file_handler(log_name,
                            config.getint("log", "max_size"),
                            config.getint("log", "backup_count"),
                            config.getboolean("log", "debug"))
    logger.set_level(config.get("log", "log_level"))

    def __init__(self, host=env_dict['host'], port=int(env_dict['port']), db_host=env_dict['db_host'],
                 db_user=env_dict['db_user'], db_pwd=env_dict['db_pwd'], db_db=env_dict['db_db']):
        self.__host = host
        self.__port = port
        self.__db_host = db_host
        self.__db_user = db_user
        self.__db_pwd = db_pwd
        self.__db_db = db_db

    @property
    def host(self):
        return self.__host

    @property
    def port(self):
        return self.__port

    @property
    def db_host(self):
        return self.__db_host

    @property
    def db_user(self):
        return self.__db_user

    @property
    def db_pwd(self):
        return self.__db_pwd

    @property
    def db_db(self):
        return self.__db_db


if __name__ == '__main__':
    env = GetEnvConf()
    print env.host, env.port
