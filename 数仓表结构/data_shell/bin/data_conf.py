#!/usr/bin/env python3
# -*- encoding: UTF-8 -*- #
"""
@author: ximing.wei
@file:   data_conf
@time:   2021-04-13 16:39:50
"""
import xlwings


def len_str(string):
    len_zh = 0
    len_en = 0
    for s in string:
        if '\u4e00' <= s <= '\u9fff':
            len_zh += 1
        else:
            len_en += 1
    return len_zh, len_en


class ExcelWings:
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


file_path = "D:\\Users\\ximing.wei\\Desktop\\手动维护表.xlsx"
sheets = [
    ('biz_conf 表', 3, 'a', 'u'),
    # ('星云项目', 2, 'a', 'm'),
    # ('投资人信息表', 3, 'a', 'q'),
]

excel = ExcelWings(file_path)
sheet_range_value = excel.object_sheet_range_value

for biz_sheet, row_start, col_s, col_e in sheets:
    excel.object_sheet(biz_sheet)
    col_list = []
    map_key_first = 0
    map_key_len = 'len'

    for asc_w in range(ord(col_s), ord(col_e) + 1):
        col_id = chr(asc_w)
        col_map = {}

        for row_num in range(row_start + 1, excel.object_sheet_row() + 1):
            col_val = sheet_range_value(f'{col_id}{row_num}')
            col_val = col_val if col_val else 'null'
            col_map[row_num] = (col_val, len_str(col_val))

        col_map[map_key_len] = max(col_map.values(), key=lambda map_tup: map_tup[1][0] * 2 + map_tup[1][1])[1]

        col_first = sheet_range_value(f'{col_id}{row_start}')
        col_map[map_key_first] = (col_first, len_str(col_first))

        col_list.append(col_map)

    sql_list = []
    for map_key in range(row_start + 1, excel.object_sheet_row() + 1):
        line_list = []
        for map_line in col_list:
            column = map_line[map_key][0]

            col_len_zh = map_line[map_key][1][0]
            col_len_en = map_line[map_key][1][1]

            len_zh_max = map_line[map_key_len][0]
            len_en_max = map_line[map_key_len][1]
            col_len = col_len_zh + col_len_en + (len_zh_max - col_len_zh) * 2 + (len_en_max - col_len_en)

            line_list.append(f"""%-{col_len + 2}s as {map_line[map_key_first][0]}""" % ('\'' + column + '\''))
        sql_list.append('select ' + ','.join(line_list))
    sql = ' union all\n'.join(sql_list) + ';'

    print(sql, '\n')

""" 关闭 Excel 文档 """
excel.close()
