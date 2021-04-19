#!/usr/bin/env python3
# -*- encoding: UTF-8 -*- #
"""
@author: ximing.wei
@file:   HiveMetastoreToExcel
@time:   2021-04-16 10:34:54
"""
import logging

import pymysql

# NOTSET   NOTSET
# DEBUG    DEBUG
# INFO     INFO
# WARNING  WARNING
# ERROR    ERROR
# CRITICAL CRITICAL
import xlwings

logger = logging.Logger("MySqlUtil", level=logging.INFO)


def tb_type_enum(table_type):
    if table_type == 'MANAGED_TABLE':
        return 'table'
    elif table_type == 'EXTERNAL_TABLE':
        return 'external table'
    elif table_type == 'VIRTUAL_VIEW':
        return 'view'
    else:
        return 'table'


class MySqlUtil(object):
    def __init__(self, host, user, password, database, port=3306, charset='utf8'):
        try:
            self.connection = pymysql.connect(
                host=host,
                user=user,
                passwd=password,
                db=database,
                port=port,
                charset=charset,
                cursorclass=pymysql.cursors.DictCursor
            )
            logger.info('MySQL 连接 创建成功！')
            self.cursor = self.connection.cursor()
            logger.info('MySQL 游标 创建成功！')
        except Exception as e:
            logger.error(f"MySQL connect error, error is {e}")

    def __del__(self):
        self.close()

    def close(self):
        try:
            if self.cursor is None:
                self.cursor.close()
                logger.info('MySQL 游标 关闭成功！')
            if self.connection is None:
                self.connection.close()
                logger.info('MySQL 连接 关闭成功！')
        except Exception as e:
            logger.error("MySQL close error, error is %s", e)

    def __execute__(self, query, args=None):
        if self.cursor is None:
            error_msg = 'MySQL 游标 未曾创建！'
            logger.error(error_msg)
            return 0
        try:
            result_num = self.cursor.execute(query, args)
        except Exception as e:
            logger.error("MySQL execute  '%s'  '%s'  error, error is %s", query, args, e)
            return 0
        return result_num

    def fetch(self, query, args=None, one_all=None):
        self.__execute__(query, args)
        if one_all is None:
            result = self.cursor.fetchall()
        else:
            result = self.cursor.fetchone()
        return result


class ExcelWings(object):
    def __init__(self, excel_name):
        """ 初始化 Excel """
        """ 打开 Excel """
        self.excel = xlwings.Book(excel_name)
        """ 获取 Excel 程序对象 """
        self.app = self.excel.app
        """ 设置 App 对象属性 """
        self.app.visible = False
        self.app.display_alerts = False
        self.app.screen_updating = False
        """ 初始化相关属性 """
        self.sheet: xlwings.main.Sheet = None

    def __del__(self):
        self.close()

    def close(self, excel_book: xlwings.Book = None, excel_app: xlwings.App = None):
        """ 关闭 Excel 文档 """
        """ 无参数时，关闭程序初始化进程 """
        if excel_book is None and excel_app is None:
            self.excel.close()
            self.app.quit()
        if excel_book is not None:
            excel_book.close()
        if excel_app is not None:
            excel_app.quit()

    def object_sheet(self, sheet_name):
        """ 获取 Sheet 对象 """
        self.sheet = self.excel.sheets[sheet_name]

    def object_sheet_row(self):
        """ 获取 Sheet 对象中最大行数 """
        return self.sheet.used_range.last_cell.row

    def object_sheet_range_value(self, sheet_range: str):
        """ 获取 Sheet 对象中某区域的值 """
        return self.sheet.range(sheet_range).value


# mysql_host = '10.80.1.104'
# mysql_user = 'root'
# mysql_pass = 'Ws@ProEmr1QSC@'
# mysql_db = 'hivemetastore'

mysql_host = '10.83.16.32'
mysql_user = 'bgp_admin'
mysql_pass = '3Mt%JjE#WJIt'
mysql_db = 'metastore'

db_name = 'db_name'
db_comm = 'db_comm'
db_set = set()

tb_name = 'tb_name'
tb_type = 'tb_type'
tb_comm = 'tb_comm'
tb_map = {}
tb_set = set()
tb_list = {}

col_id = 'col'
col_index = 'col_index'
col_name = 'col_name'
col_type = 'col_type'
col_comm = 'col_comm'
col_map = {}
part_map = {}

dt_map = {}

sql = """
select
  db_name,
  db_comm,
  tb_type,
  tb_name,
  tb_comm,
  col_name,
  col_type,
  col_comm,
  col_index,
  col
from (
  select
    db.name           as db_name,
    db.desc           as db_comm,
    tb.tbl_type       as tb_type,
    tb.tbl_name       as tb_name,
    tbl_param.comment as tb_comm,
    col.column_name   as col_name,
    col.type_name     as col_type,
    col.comment       as col_comm,
    col.integer_idx   as col_index,
    0                 as col
  from (
    select
      `db_id`,
      `name`,
      `desc`
    from DBS
    where name in (
      'dim',
      'stage',
      'ods',     'ods_cps',
      'dw',      'dw_cps',
      'dm_eagle','dm_eagle_cps',
      ''
    )
  ) as db
  join (
    select
      `db_id`,
      `tbl_id`,
      `sd_id`,
      `tbl_name`,
      `tbl_type`
    from TBLS
  ) as tb
  on db.db_id = tb.db_id
  left join (
    select
      `tbl_id`,
      max(if(`param_key` = 'comment',`param_value`,null)) as comment
    from TABLE_PARAMS
    group by tbl_id
  ) as tbl_param
  on tb.tbl_id = tbl_param.tbl_id
  left join (
    select
      `sd_id`,
      `cd_id`
    from SDS
  ) as sds
  on tb.sd_id = sds.sd_id
  left join (
    select
      `cd_id`,
      `column_name`,
      `type_name`,
      `comment`,
      `integer_idx`
    from COLUMNS_V2
  ) as col
  on sds.cd_id = col.cd_id
  union all
  select
    db.name           as db_name,
    db.desc           as db_comm,
    tb.tbl_type       as tb_type,
    tb.tbl_name       as tb_name,
    tbl_param.comment as tb_comm,
    part.pkey_name    as col_name,
    part.pkey_type    as col_type,
    part.pkey_comment as col_comm,
    part.integer_idx  as col_index,
    1                 as col
  from (
    select
      `db_id`,
      `name`,
      `desc`
    from DBS
    where name in (
      'dim',
      'stage',
      'ods',     'ods_cps',
      'dw',      'dw_cps',
      'dm_eagle','dm_eagle_cps',
      ''
    )
  ) as db
  join (
    select
      `db_id`,
      `tbl_id`,
      `sd_id`,
      `tbl_name`,
      `tbl_type`
    from TBLS
  ) as tb
  on db.db_id = tb.db_id
  left join (
    select
      `tbl_id`,
      max(if(`param_key` = 'comment',`param_value`,null)) as comment
    from TABLE_PARAMS
    group by tbl_id
  ) as tbl_param
  on tb.tbl_id = tbl_param.tbl_id
  join (
    select
      `tbl_id`,
      `pkey_name`,
      `pkey_type`,
      `pkey_comment`,
      `integer_idx`
    from PARTITION_KEYS
  ) as part
  on tb.tbl_id = part.tbl_id
) as tmp
where 1 > 0
  and db_name like %s and tb_name like %s
order by db_name,tb_name,col,col_index;
"""

mysql = MySqlUtil(mysql_host, mysql_user, mysql_pass, mysql_db)

# results = mysql.fetch(sql, ('dm_eagle', 'abs_asset_distribution_day'))
# results = mysql.fetch(sql, ('dm_eagle', '%%'))
# results = mysql.fetch(sql, ('dim', '%%'))
results = mysql.fetch(sql, ('%%', '%%'))

# print(results)

db_list_index = -1
tb_list_index = -1
for line in results:
    line = dict(line)

    if line[db_name] not in db_set:
        db_list_index += 1
        tb_list_index = -1

        dt_map = {
            db_name: line[db_name],
            db_comm: line[db_comm] if line[db_comm] is not None else '',
        }
    db_set.add(line[db_name])

    if line[db_name] + line[tb_name] not in tb_set:
        tb_list_index += 1
        col_map = {}
        part_map = {}

        tb_map = {
            tb_name: line[tb_name],
            tb_type: tb_type_enum(line[tb_type]),
            tb_comm: line[tb_comm] if line[tb_comm] is not None else '',
        }
    tb_set.add(line[db_name] + line[tb_name])

    if line[col_id] == 0:
        col_map[line[col_index]] = {
            col_name: line[col_name],
            col_type: line[col_type],
            col_comm: line[col_comm] if line[col_comm] is not None else '',
        }
        tb_map[0] = col_map
    elif line[col_id] == 1:
        part_map[line[col_index]] = {
            col_name: line[col_name],
            col_type: line[col_type],
            col_comm: line[col_comm] if line[col_comm] is not None else '',
        }
        tb_map[1] = part_map

    tb_list[tb_list_index] = tb_map
    dt_map[db_list_index] = tb_list

dt_db_map = {}
for dt_key, dt_val in dt_map.items():
    if dt_key == db_name:
        dt_db_map[db_name] = dt_val
    elif dt_key == db_comm:
        dt_db_map[db_comm] = dt_val
    elif type(dt_key) is int:
        for tb_key, tb_val in dt_val.items():
            print(dt_key, dt_db_map, tb_key, tb_val)

# print(dt_map)


file_path = "D:\\Users\\ximing.wei\\Desktop\\手动维护表.xlsx"
sheets = [
    # ('biz_conf 表', 3, 'a', 'u', 'ac'),
    ('星云项目', 2, 'b', 'm', 'pp'),
    # ('投资人信息表', 3, 'a', 'q', 'ai'),
]

excel = ExcelWings(file_path)
sheet_range_value = excel.object_sheet_range_value

""" 关闭 Excel 文档 """
excel.close()
