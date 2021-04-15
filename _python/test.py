# a = []
# for i in range(10):
#     a.append(i)
#     print(a)
#
# a.reverse()
#
# while len(a):
#     if len(a) == 5:
#         break
#     print(a.pop())


# coding=utf-8
# import re
# import os
#
# content = '''''
# <script>var images = [
# { "big":"http://i-2.yxdown.com/2015/3/18/KDkwMHgp/6381ccc0-ed65-4422-8671-b3158d6ad23e.jpg",
#   "thumb":"http://i-2.yxdown.com/2015/3/18/KHgxMjAp/6381ccc0-ed65-4422-8671-b3158d6ad23e.jpg",
#   "original":"http://i-2.yxdown.com/2015/3/18/6381ccc0-ed65-4422-8671-b3158d6ad23e.jpg",
#   "title":"","descript":"","id":75109},
# { "big":"http://i-2.yxdown.com/2015/3/18/KDkwMHgp/fec26de9-8727-424a-b272-f2827669a320.jpg",
#   "thumb":"http://i-2.yxdown.com/2015/3/18/KHgxMjAp/fec26de9-8727-424a-b272-f2827669a320.jpg",
#   "original":"http://i-2.yxdown.com/2015/3/18/fec26de9-8727-424a-b272-f2827669a320.jpg",
#   "title":"","descript":"","id":75110},
# </script>
# '''
#
# html_script = r'<script>(.*?)</script>'
# m_script = re.findall(html_script, content, re.S | re.M)
# for script in m_script:
#     res_original = r'"original":"(.*?)"'  # 原图
#     m_original = re.findall(res_original, script)
#     for pic_url in m_original:
#         print(pic_url)
#         filename = os.path.basename(pic_url)  # 去掉目录路径,返回文件名
#         print(filename)


# ss = '    '
# print(ss.replace(' ', 'a'))

""" 获取当前目录 """
# base_dir = os.path.abspath(os.path.join(os.getcwd(), "."))
# print(f'{base_dir}')

''' 获取目录下的所有子文件及子文件夹 '''
# for dirs in os.walk(base_dir):
#     print(dirs)

''' 获取目录下的所有文件及文件夹 '''
# for dirs in os.listdir(base_dir):
#     print(dirs)

# print(f'{base_dir}/python_xlwings')
# print(os.getcwd())
# print(os.path.abspath(os.path.dirname(__file__)))
# print(os.path.abspath(os.path.join(os.getcwd(), ".")))  # 主用
''' 获取上级目录 '''
# print(os.path.abspath(os.path.join(os.getcwd(), "../..")))  # 主用

# import xlwings as xw
# import pandas as pd
# import numpy as np

# df = pd.DataFrame(np.random.rand(10, 4), columns=['a', 'b', 'c', 'd'])
# xw.view(df)


# sql = "CREATE TABLE IF NOT EXISTS `dim_new.dim_encrypt_info`(\
# `dim_type`                      string        COMMENT '数据类型',\
# `dim_encrypt`                   string        COMMENT '加密字段',\
# `dim_decrypt`                   string        COMMENT '明文字段',\
# `create_time`                   timestamp     COMMENT '创建时间（yyyy—MM—dd HH:mm:ss）',\
# `update_time`                   timestamp     COMMENT '更新时间（yyyy—MM—dd HH:mm:ss）'\
# ) COMMENT '加密信息表'\
# PARTITIONED BY (`product_id` string COMMENT ')产 品,编号', aa int)\
# STORED AS PARQUET;"

# 1.分割SQL
# stmts = sqlparse.split(sql)
# for stmt in stmts:
#     # 2.format格式化
#     # print(sqlparse.format(stmt, reindent=True, keyword_case="upper"))
#     # 3.解析SQL
#     stmt_parsed = sqlparse.parse(stmt)[0].tokens
#     for token in stmt_parsed:
#         print(type(token), token.ttype, token.value)

# sql_str = "CREATE TABLE IF NOT EXISTS `dim_new.dim_encrypt_info`(\n" \
#           "  `dim_type`    string    COMMENT '数据类型',\n" \
#           "  `dim_encrypt` string    COMMENT '加密字段',\n" \
#           "  `dim_decrypt` string    COMMENT '明文字段',\n" \
#           "  `create_time` timestamp COMMENT '创建时间（yyyy—MM—dd HH:mm:ss）',\n" \
#           "  `update_time` timestamp COMMENT '更新时间（yyyy—MM—dd HH:mm:ss）'\n" \
#           ") COMMENT '加密信息表'\n" \
#           "PARTITIONED BY (`product_id` string COMMENT '产品编号')\n" \
#           "STORED AS PARQUET;"
#
# print(sql_str)
#
# q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)
# print(q)
# for line in q.splitlines():
#     if not re.match("^\s*(--|#)", line):
#         print(line)
# lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]
# print(lines)
# switch = {
#     "a": lambda x: x * 2,
#     "b": lambda x: x * 3,
#     "c": lambda x: x ** x
# }
#
# try:
#     print(switch["c"](6))
# except KeyError as e:
#     pass


# import sqlparse
# sql = 'delete from someschema'
#
# parsed = sqlparse.parse(sql)
# result = sqlparse.sql.Statement(parsed[0].tokens)
# print(result.get_type())

# name, addr = ['a', '哈哈哈']
# print(name, addr)

# cmd = 'ls'
# text_list = os.popen(cmd).readlines()
# os.system('pwd')
# print(text_list)
print(ord('a'))
print(ord('m'))

print(len('打怪者'), len('abc'))  # 3 3
print(len(u'打怪者'), len(u'abc'))  # 3 3
print(len(u'打怪者'.encode('gbk')), len(u'abc'.encode('gbk')))  # 6 3
print(len(u'打怪者'.encode()), len(u'abc'.encode()))  # 9 3
print(len(u'打怪者'.encode('utf-8')), len(u'abc'.encode('utf-8')))  # 9 3
