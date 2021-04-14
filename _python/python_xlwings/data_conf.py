#!/usr/bin/env python3
# -*- encoding: UTF-8 -*- #
"""
@author: ximing.wei
@file:   data_conf
@time:   2021-04-13 16:39:50
"""
import xlwings

file_path = "D:\\Users\\ximing.wei\\Desktop\\手动维护表.xlsx"
# biz_sheet, row_start, col_s, col_e = ('biz_conf 表', 3, 'a', 'u')
biz_sheet, row_start, col_s, col_e = ('星云项目', 2, 'a', 'm')

excel = xlwings.Book(file_path)

""" 通过打开的 Excel 文档获取 应用程序 """
app = excel.app

""" 设置 App """
app.display_alerts = False  # 关闭一些提示信息，可以加快运行速度。 默认为 True
app.screen_updating = True  # 关掉屏幕刷新。默认为 True。关闭它也可以提升运行速度。运行脚本结束后，改回 True

""" 获取 所有分页 """
sheets = excel.sheets

""" 引用 工作表 """
sheet = sheets[biz_sheet]

""" 获取 最大 行数 """
rows = sheet.used_range.last_cell.row

""" 获取 工作表 工作区域 """
sheet_range = sheet.range

""" 字段名 """
columns: list = sheet_range(f'{col_s}{row_start}:{col_e}{row_start}').value

# print(columns, len(columns), rows)

sql_list = []
for i in range(row_start + 1, rows + 1):
    values: list = sheet_range(f'{col_s}{i}:{col_e}{i}').value
    line_list = []
    for j in range(len(columns)):
        value = str(values[j]) if values[j] is not None else 'null'
        line_list.append(f"""'{value}' as {columns[j]}""")

    sql_list.append('select ' + ','.join(line_list))

sql = ' union all\n'.join(sql_list)

print(sql)

""" 保存 Excel 文档 """
# excel.save()
""" 关闭 Excel 文档 """
# excel.close()
""" 退出 应用程序 """
# app.quit()
# app.kill()
