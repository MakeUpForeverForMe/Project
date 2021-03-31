#!/usr/bin/env python3
# -*- encoding: UTF-8 -*- #
"""
@author: ximing.wei
@file:   create_table.py
@time:   2021-03-01 15:01:55
"""
import os
import sys

while True:
    try:
        import pymysql

        print('已检测到 pymysql 模块           ok')
        break
    except ModuleNotFoundError:
        print('检测到未安装 pymysql 模块,现在开始安装......')
        os.system('pip install pymysql')
        continue


def case(type_string):
    if 'bigint' in type_string or 'datetime' in type_string:
        return 'bigint'
    elif 'int' in type_string or 'tinyint' in type_string or 'smallint' in type_string or 'mediumint' in type_string or 'integer' in type_string:
        return "int"
    elif 'double' in type_string or 'decimal' in type_string:
        return type_string
    elif 'float' in type_string:
        return 'decimal(30,10)'
    else:
        return "string"


args = sys.argv

args_length = len(args) - 1
if args_length != 8:
    print(f'参数个数不符合条件（8）：${args_length}！')
    sys.exit(1)

schemadb = 'information_schema'

hive_db = args[1]
hive_tb = args[2]
hive_db_tb = hive_db + '.' + hive_tb

hostname = args[3]
username = args[4]
password = args[5]

database = args[6]
tablname = args[7]

is_partitioned = args[8]

if is_partitioned == 'SY':
    partition = "PARTITIONED BY(d_date string comment '快照日期', p_type string comment '数据类型')\n"
elif is_partitioned == 'Y':
    partition = "PARTITIONED BY(d_date string comment '快照日期')\n"
else:
    partition = ""

sql_tbl = 'select TABLE_COMMENT from TABLES WHERE TABLE_SCHEMA = %s and TABLE_NAME = %s'
sql_col = 'select COLUMN_NAME,COLUMN_TYPE,COLUMN_COMMENT from COLUMNS where TABLE_SCHEMA = %s and TABLE_NAME = %s order by ORDINAL_POSITION;'

connection = pymysql.connect(host=hostname, port=3306, user=username, password=password, database=schemadb, charset='utf8')
cursor = connection.cursor()

cursor.execute(sql_tbl, (database, tablname))  # 返回执行行数（1行）
tbl_comm = cursor.fetchone()[0]  # cursor.fetchone() 返回 tuple 类型数据，因为只有一条数据，取第一个

col_rows = cursor.execute(sql_col, (database, tablname))
# cursor.fetchall() 返回 List 格式数据。每一行为 Tuple 数据
# a 字段名，b 字段类型，c 字段注释
data_all = [((a, len(a)), (case(b), len(case(b))), (c, len(c))) for a, b, c in cursor.fetchall()]

col_name = max(data_all, key=lambda tup: tup[0][1])[0][1]
col_type = max(data_all, key=lambda tup: tup[1][1])[1][1]
col_comm = max(data_all, key=lambda tup: tup[2][1])[2][1]

i = 1
ddl = f'-- DROP TABLE IF EXISTS `{hive_db_tb}`;\n' \
      f'CREATE EXTERNAL TABLE IF NOT EXISTS `{hive_db_tb}`(\n'
for (column_name, tup_name), (column_type, tup_type), (column_comment, tup_comment) in data_all:
    ddl += f'  %-{col_name + 2}s %-{col_type}s COMMENT %-s\n' % (
        '`' + column_name + '`', case(column_type), '\'' + column_comment + '\'' + ('' if i == col_rows else ','))
    i += 1

ddl += f') COMMENT \'{tbl_comm}\'\n' \
       f'{partition}' \
       f'STORED as PARQUET;'

print(ddl)

cursor.close()
connection.close()

file_name = f'/tmp/{hive_db_tb}.hql'
file = open(file_name, mode="w")
file.write(ddl)
file.close()

os.system(f'hdfs dfs -put -f {file_name} cosn://bigdata-center-prod-1253824322/user/hadoop/create_table')
os.system(f'beeline -n hadoop -u "jdbc:hive2://10.80.0.46:2181,10.80.0.255:2181,10.80.1.113:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -f {file_name}')
