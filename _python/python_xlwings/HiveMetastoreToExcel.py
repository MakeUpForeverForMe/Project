#!/usr/bin/env python3
# -*- encoding: UTF-8 -*- #
"""
@author: ximing.wei
@file:   HiveMetastoreToExcel
@time:   2021-04-16 10:34:54
"""
import logging

import pymysql
import xlwings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger("MySqlUtil")


def tb_type_enum(table_type):
    if table_type == 'MANAGED_TABLE':
        return 'table'
    elif table_type == 'EXTERNAL_TABLE':
        return 'external table'
    elif table_type == 'VIRTUAL_VIEW':
        return 'view'
    else:
        return 'table'


def rgb2hex(rgb: tuple):
    return int('%2x%2x%02x' % (rgb[2], rgb[1], rgb[0]), 16)


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
            if self.cursor is not None:
                self.cursor.close()
                logger.info('MySQL 游标 关闭成功！')
            if self.connection is not None:
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
            logger.info(f'执行语句完成，返回{result_num}行数据！')
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
        logger.info('抓取数据完成！')
        return result


# mysql_host = '10.80.1.104'
# mysql_user = 'root'
# mysql_pass = 'Ws@ProEmr1QSC@'
# mysql_db = 'hivemetastore'

mysql_host = '10.83.16.32'
mysql_user = 'bgp_admin'
mysql_pass = '3Mt%JjE#WJIt'
mysql_db = 'metastore'

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

db_name = 'db_name'
db_comm = 'db_comm'

tb_name = 'tb_name'
tb_type = 'tb_type'
tb_comm = 'tb_comm'

col_id = 'col'
col_index = 'col_index'
col_name = 'col_name'
col_type = 'col_type'
col_comm = 'col_comm'

db_set = set()
tb_set = set()

col_map = {}
par_map = {}

dt_map = {}

db_index = -1
tb_index = -1

for line in results:
    line = dict(line)

    if line[db_name] not in db_set:
        db_index += 1
        tb_index = -1

        dt_map[db_index] = {
            db_name: line[db_name],
            db_comm: line[db_comm] if line[db_comm] is not None else '',
        }
    db_set.add(line[db_name])

    if line[db_name] + line[tb_name] not in tb_set:
        tb_index += 1
        col_map = {}
        par_map = {}

        dt_map[db_index][tb_index] = {
            tb_name: line[tb_name],
            tb_type: tb_type_enum(line[tb_type]),
            tb_comm: line[tb_comm] if line[tb_comm] is not None else '',
        }
    tb_set.add(line[db_name] + line[tb_name])

    tb_map = col_map if line[col_id] == 0 else par_map

    tb_map[line[col_index]] = {
        col_name: line[col_name],
        col_type: line[col_type],
        col_comm: line[col_comm] if line[col_comm] is not None else '',
    }

    dt_map[db_index][tb_index][line[col_id]] = tb_map

# print(dt_map)


file_path = "D:\\Users\\ximing.wei\\Desktop\\数仓 表设计.xlsx"
after = '字段规范与解释'
""" 打开 Excel """
excel = xlwings.Book(file_path)
""" 获取 Excel 程序对象 """
app = excel.app
""" 设置 App 对象属性 """
app.visible = False  # visible 决定App是否可见
app.display_alerts = False  # 关闭一些提示信息，可以加快运行速度。 默认为 True
app.screen_updating = False  # 关掉屏幕刷新。默认为 True

sheet_names = [sheet.name for sheet in excel.sheets]

try:
    for dt_val in dt_map.values():
        tb_count = len(dt_val.keys()) - 2

        if dt_val[db_name] in sheet_names:
            excel.sheets[dt_val[db_name]].delete()
            logger.info(f'删除 Sheet {dt_val[db_name]} 成功！')
        excel.sheets.add(dt_val[db_name], after=after)
        logger.info(f'添加 Sheet {dt_val[db_name]} 成功！')

        sheet = excel.sheets[dt_val[db_name]]
        sheet_name = sheet.name
        logger.info(f'获取到 Sheet {sheet_name}！')

        row_height = 27.75
        sheet['A1'].row_height = row_height  # 设置第1行 行高
        logger.info(f'设置 {sheet_name} A1 行高为 {row_height}！')
        col_width = (10.75, 61.38, 15.5, 82.25, 8.25, 8.25, 10.75, 10.75, 32)
        for table_index, width in enumerate(col_width):
            sheet[f'{chr(97 + table_index)}1'].column_width = width  # 设置 列宽
            logger.info(f'设置 {sheet_name} {chr(97 + table_index)} 列宽为 {width}！')

        """ 标题 设置 """
        sheet['A1'].value = f'{dt_val[db_name]}    {dt_val[db_comm]}'
        logger.info(f'设置 {sheet_name} A1 值为 {sheet["A1"].value}！')
        """ 合并 单元格 """
        a1i1 = sheet['A1:I1']
        a1i1.api.merge()
        logger.info(f'设置 {sheet_name} A1:I1 合并！')
        """ 设置 字体 """
        font_color = (5, 99, 193)
        a1i1.api.Font.Color = rgb2hex(font_color)  # 设置字体颜色
        logger.info(f'设置 {sheet_name} A1 字体颜色为 {font_color}！')
        font_size = 22
        a1i1.api.Font.Size = font_size  # 设置字体大小
        logger.info(f'设置 {sheet_name} A1 字体大小为 {font_size}！')
        is_bold = True
        a1i1.api.Font.Bold = is_bold  # 设置为粗体
        logger.info(f'设置 {sheet_name} A1 字体粗体 {is_bold}！')
        """ 设置 单元格边框 """
        line_style = 1
        a1i1.api.Borders.LineStyle = line_style  # 全框实线
        logger.info(f'设置 {sheet_name} A1 单元格边框线型 {line_style}！')
        line_weight = 3
        a1i1.api.Borders.Weight = line_weight  # 边框宽度
        logger.info(f'设置 {sheet_name} A1 单元格边框宽度 {line_weight}！')
        line_color = (255, 192, 0)
        a1i1.api.Borders.Color = rgb2hex(line_color)  # 边框颜色
        logger.info(f'设置 {sheet_name} A1 单元格边框颜色 {line_color}！')

        table_name_index = 3
        sheet[f'B{table_name_index}'].value = '表名'
        sheet[f'C{table_name_index}'].value = '类型'
        sheet[f'D{table_name_index}'].value = '表注释'
        logger.info(f'设置 {sheet_name} 页面头部 表头信息！')

        b3d3 = sheet['B3:D3']
        b3d3.api.HorizontalAlignment = -4108
        logger.info(f'设置 {sheet_name} 页面头部 字段居中！')

        for table_index in range(tb_count):
            tb_map = dict(dt_val[table_index])
            logger.info(f'获取 {sheet_name} 库中的表 {tb_map[tb_name]}！')

            table_name_index += 1
            sheet[f'B{table_name_index}'].value = tb_map[tb_name]
            sheet[f'C{table_name_index}'].value = tb_map[tb_type]
            sheet[f'D{table_name_index}'].value = tb_map[tb_comm]
            logger.info(f'设置 {sheet_name} 页面头部 表信息 {tb_map[tb_name]} {tb_map[tb_type]} {tb_map[tb_comm]}！')

            sheet[f'{table_name_index + 1}:{table_name_index + 1}'].api.Insert()
            logger.info('在 {sheet_name} 页面头部 表信息 插入新行！')

            """ 取最大行数是为了在最后添加新的表 """
            """ 最大 行数 """
            table_col_index_max = sheet.api.UsedRange.Rows.count
            logger.info(f'获取当前页面 {sheet_name} 的最大行数 {table_col_index_max}！')

            table_col_index = table_col_index_max + 2
            sheet[f'A{table_col_index}'].value = '名称信息'
            sheet[f'B{table_col_index}'].value = '英文名称'
            sheet[f'C{table_col_index}'].value = '类型'
            sheet[f'D{table_col_index}'].value = '汉语注释'
            sheet[f'E{table_col_index}'].value = '来源库'
            sheet[f'F{table_col_index}'].value = '来源表'
            sheet[f'G{table_col_index}'].value = '来源字段'
            sheet[f'H{table_col_index}'].value = '计算逻辑'
            sheet[f'I{table_col_index}'].value = '备注'
            logger.info(f'设置 {sheet_name} 表信息 表头！')

            ai3 = sheet[f'A{table_col_index}:I{table_col_index}']
            ai3.api.Font.Size = 14
            ai3.api.HorizontalAlignment = -4108
            ai3.api.Borders.LineStyle = 1
            ai3.api.Borders.Weight = 3
            ai3.api.Borders(11).Weight = 2
            logger.info(f'设置 {sheet_name} 表信息 表头 边框、字体大小、单元格居中！')

            table_col_index += 1
            sheet[f'A{table_col_index}'].value = '表名信息'
            sheet[f'B{table_col_index}'].value = tb_map[tb_name]
            sheet[f'C{table_col_index}'].value = tb_map[tb_type]
            sheet[f'D{table_col_index}'].value = tb_map[tb_comm]
            logger.info(f'设置 {sheet_name} 表信息 {tb_map[tb_name]}！')

            part_index_start = table_col_index

            if len(tb_map.keys()) - 3 == 2:
                logger.info(f'{sheet_name} 中表 {tb_map[tb_name]} 有分区，设置 表信息 的 分区信息！')
                for col_index, col_val in tb_map[1].items():
                    table_col_index += 1
                    sheet[f'A{table_col_index}'].value = '分区信息'
                    sheet[f'B{table_col_index}'].value = col_val[col_name]
                    sheet[f'C{table_col_index}'].value = col_val[col_type]
                    sheet[f'D{table_col_index}'].value = col_val[col_comm]
                    logger.info(f'设置 {sheet_name} 表信息 {tb_map[tb_name]} 的第 {col_index} 个分区的分区信息 {col_val[col_name]}！')
                logger.info(f'设置 {sheet_name} 表信息 {tb_map[tb_name]} 的分区信息完成！')

            ai3_partition = sheet[f'A{part_index_start}:I{table_col_index}']
            ai3_partition.api.Font.Size = 14
            ai3_partition.api.Borders.LineStyle = 1
            ai3_partition.api.Borders.Weight = 3
            ai3_partition.api.Borders(11).Weight = 2
            ai3_partition.api.Borders(12).Weight = 2
            logger.info(f'设置 {sheet_name} 表信息 {tb_map[tb_name]} 分区部分的 边框、字体大小、单元格居中！')

            col_index_start = table_col_index + 1

            for col_index, col_val in tb_map[0].items():
                table_col_index += 1
                sheet[f'A{table_col_index}'].value = '字段信息'
                sheet[f'B{table_col_index}'].value = col_val[col_name]
                sheet[f'C{table_col_index}'].value = col_val[col_type]
                sheet[f'D{table_col_index}'].value = col_val[col_comm]
                logger.info(f'设置 {sheet_name} 表信息 {tb_map[tb_name]} 的第 {col_index} 个字段的字段信息 {col_val[col_name]}！')
            logger.info(f'设置 {sheet_name} 表信息 {tb_map[tb_name]} 的字段信息完成！')

            ai3_col = sheet[f'A{col_index_start}:I{table_col_index}']
            ai3_col.api.Font.Size = 14
            ai3_col.api.Borders.LineStyle = 1
            ai3_col.api.Borders.Weight = 3
            ai3_col.api.Borders(11).Weight = 2
            ai3_col.api.Borders(12).Weight = 2
            logger.info(f'设置 {sheet_name} 表信息 {tb_map[tb_name]} 字段部分的 边框、字体大小、单元格居中！')
        logger.info(f'设置 {sheet_name} 页面的表信息完成！')

        b3d3.api.Borders.LineStyle = 1
        b3d3.api.Borders.Weight = 3
        b3d3.api.Borders(11).Weight = 2
        b3d3.api.Font.Size = 14
        logger.info(f'设置 {sheet_name} 页面头部 表头 部分的 边框、字体大小、单元格居中！')

        b4di = sheet[f'B4:D{table_name_index}']
        b4di.api.Borders.LineStyle = 1
        b4di.api.Borders.Weight = 3
        b4di.api.Borders(11).Weight = 2
        b4di.api.Borders(12).Weight = 2
        b4di.api.Font.Size = 14
        logger.info(f'设置 {sheet_name} 页面头部 表信息 部分的 边框、字体大小、单元格居中！')

        logger.info(f'设置 {sheet_name} 页面完成！')
finally:
    """ 关闭 Excel 文档 """
    excel.save()
    logger.info('Excel 保存成功！')
    excel.close()
    logger.info('Excel 关闭成功！')
    app.quit()
    logger.info('Excel 退出成功！')
