#!/usr/bin/env python3
# -*- encoding: UTF-8 -*- #
"""
@author: ximing.wei
@file:   mysql_util
@time:   2021-04-16 14:27:19
"""

import logging
import pymysql

logger = logging.Logger("MySqlUtil")


class MySqlUtil(object):
    def __init__(self, host, user, password, database, port=3306, charset='utf8'):
        try:
            self.close()
            self.connection = pymysql.connect(
                host=host,
                user=user,
                passwd=password,
                db=database,
                port=port,
                charset=charset,
                cursorclass=pymysql.cursors.DictCursor
            )
            self.cursor = self.connection.cursor()
        except Exception as e:
            logger.error(f"MySQL connect error, error is {e}")

    def __del__(self):
        self.close()

    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
        except Exception as e:
            logger.error("MySQL close error, error is %s", e)

    def __execute__(self, sql, args=None):
        if self.cursor:
            error_msg = "execute '%s' whith a None cursor, please make sure your MysqlServer is working and please call connect first" % sql
            logger.error(error_msg)
            return False, 0, error_msg
        try:
            result_num = self.cursor.execute(sql)
            self.connection.commit()
        except Exception as e:
            logger.error("MySQL execute  '%s' error, error is %s", sql, e)
            return False, 0
        return True, result_num

    def fetchall(self, sql, args=None):
        rows = self.__execute__(sql, args)
        result = self.cursor.fetch()
        return rows, result

    def fetchone(self, sql, args=None):
        self.__execute__(sql, args)
        result = self.cursor.fetchone()
        return result
