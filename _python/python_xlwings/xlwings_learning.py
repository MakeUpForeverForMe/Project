import xlwings as xw
import os

""" 颜色索引 """
# 无色   = -4142
# 自动   = -4105
# 黑色   = 1
# 白色   = 2
# 红色   = 3
# 鲜绿   = 4
# 蓝色   = 5
# 黄色   = 6
# 粉红   = 7
# 青绿   = 8
# 深红   = 9
# 绿色   = 10
# 深蓝   = 11
# 深黄   = 12
# 紫罗兰 = 13
# 青色   = 14
# 灰色25 = 15
# 褐色   = 53
# 橄榄   = 52
# 深绿   = 51
# 深青   = 49
# 靛蓝   = 55
# 灰色80 = 56
# 橙色   = 46
# 蓝灰   = 47
# 灰色50 = 16
# 浅橙色 = 45
# 酸橙色 = 43
# 海绿   = 50
# 水绿色 = 42
# 浅蓝   = 41
# 灰色40 = 48
# 金色   = 44
# 天蓝   = 33
# 梅红   = 54
# 玫瑰红 = 38
# 茶色   = 40
# 浅黄   = 36
# 浅绿   = 35
# 浅青绿 = 34
# 淡蓝   = 37
# 淡紫   = 39

""" 获取 当前目录 """
base_dir = os.path.abspath(os.path.join(os.getcwd(), "."))

""" 获取 App """
# app = xw.App(add_book=False)  # visible决定App是否可见（对象不存在时设置为True），add_book 参数为是否生成一个新的工作表,默认True

""" 获取 文档 """
# excel = app.books.open(f'{base_dir}/python_xlwings.xlsx')
excel = xw.Book(f'{base_dir}/python_xlwings.xlsx')

""" 通过打开的 Excel 文档获取 应用程序 """
app = excel.app

""" 设置 App """
app.display_alerts = False  # 关闭一些提示信息，可以加快运行速度。 默认为 True
app.screen_updating = True  # 关掉屏幕刷新。默认为 True。关闭它也可以提升运行速度。运行脚本结束后，改回 True

""" 获取 所有分页 """
sheets = excel.sheets

""" 根据 分页索引 获取 分页名称 """
# sheets_name = []  # 创建一个列表用于存储分页名称
# for i in range(len(sheets)):
#     sheets_name.append(sheets[i].name)
# print(sheets_name)  # ['databases', 'tables', 'Python 练习']

""" 添加 分页 """
sheet_add_name = 'Python 练习'
# if sheet_add_name not in sheets_name:
#     sheets.add(sheet_add_name, after=sheets_name[-1])

""" 引用 工作表 """
sheet = sheets[sheet_add_name]

""" 获取 工作表 工作区域 """
sheet_range = sheet.range

""" 选定 单元格 """
a1 = sheet_range('a1')
b2d4 = sheet_range('b2:d4')

""" 获取 单元格数据 """
# print(a1.value)
# print(sheet_range('A1:B5').value)

""" 写入 单元格数据 """
# a1.value = 'sublime'

""" 设置 字体 """
# a1.api.NumberFormat = "0.00"        # 设置单元格的数字格式
# a1.api.Font.ColorIndex = 3          # 设置字体的颜色，具体颜色索引见最上方
# a1.api.Font.Bold = True             # 设置为粗体。
# a1.api.Font.FontStyle = '加粗'      # 设置为粗体
# a1.api.Font.FontStyle = '倾斜'      # 设置为斜体
# a1.api.Font.FontStyle = '加粗倾斜'  # 设置为加粗斜体
# a1.api.Font.Underline = 2           # 设置字体 下划线 1：无，2：有
# a1.api.HorizontalAlignment = -4108  # -4108 水平居中。 -4131 靠左，-4152 靠右
# a1.api.VerticalAlignment = -4108    # -4108 垂直居中（默认）。 -4160 靠上，-4107 靠下， -4130 自动换行对齐
# a1.color = 255, 153, 255            # 设置单元格的填充颜色

""" 操作 单元格 """
""" 操作 单元格名称 """
""" 添加 单元格名称 """
# excel.names.add('bbb', "='Python 练习'!$A$2")
# print(excel.names)
""" 删除 单元格名称 """
# excel.names['aaa'].delete()
# print(excel.names['bbb'].name)
# print(excel.names['bbb'].refers_to)
""" 添加 超链接 """
# a1.add_hyperlink("#'Python 练习'!b2", '超链接测试', '提示：超链接')


""" 插入 一行 """
# sheet.api.Rows(1).Insert()  # 会在第1行插入一行，原来的第1行下移
""" 删除 一行 """
# sheet.api.Rows(1).Delete()  # 删除第1行
# a1.api.EntireRow.Delete()  # 会删除 a1 单元格所在的行
""" 插入 一列 """
# sheet.api.Columns(1).Insert()  # 会在第1列插入一列，原来的第1列右移。(也可以用列的字母表示)
""" 删除 一列 """
# sheet.api.Columns(1).Delete()  # 删除第1列
# a1.api.EntireColumn.Delete()  # 会删除 a1 单元格所在的列


""" 设置 单元格大小 """
# sheet.autofit()                     # 自动调整单元格大小。注：此方法是在单元格写入内容后，再使用，才有效。
# sheet.range(1, 4).column_width = 5  # 设置第4列 列宽。（1,4）为第1行第4列的单元格
# sheet.range(1, 4).row_height = 20   # 设置第1行 行高


""" 设置 单元格边框 """
# Borders（边框类型）最多12种
# LineStyle（线型，最多10种）值说明：
# 0：透明
# 1：实线
# 2：小虚线
# 3：大虚线
# 4：点划线
# 5：双点划线
# 6：粗划线
# 7：粗实线
# 8：小虚线
# 9：双实线

""" Borders 设置全框线 """
# b2d4.api.Borders.LineStyle = 1
# b2d4.api.Borders.Weight = 1  # 不建议如此操作

""" Borders(1) 单元格所有左边框 """
# b2d4.api.Borders(1).LineStyle = 1
# b2d4.api.Borders(1).Weight = 3

""" Borders(2) 单元格所有右边框 """
# b2d4.api.Borders(2).LineStyle = 1
# b2d4.api.Borders(2).Weight = 3

""" Borders(3) 单元格所有上边框 """
# b2d4.api.Borders(3).LineStyle = 1
# b2d4.api.Borders(3).Weight = 3

""" Borders(4) 单元格所有下边框 """
# b2d4.api.Borders(4).LineStyle = 1
# b2d4.api.Borders(4).Weight = 3

""" Borders(5) 单元格 内部斜线（左上到右下） """
# b2d4.api.Borders(5).LineStyle = 1
# b2d4.api.Borders(5).Weight = 3

""" Borders(6) 单元格 内部斜线（左下到右上） """
# b2d4.api.Borders(6).LineStyle = 1
# b2d4.api.Borders(6).Weight = 3

""" Borders(7) 单元格整体左边框 """
# b2d4.api.Borders(7).LineStyle = 1
# b2d4.api.Borders(7).Weight = 3

""" Borders(8) 单元格整体顶部边框 """
# b2d4.api.Borders(8).LineStyle = 1
# b2d4.api.Borders(8).Weight = 3

""" Borders(9) 单元格整体底部边框 """
# b2d4.api.Borders(9).LineStyle = 1
# b2d4.api.Borders(9).Weight = 3

""" Borders(10) 单元格整体右边框 """
# b2d4.api.Borders(10).LineStyle = 1
# b2d4.api.Borders(10).Weight = 3

""" Borders(11) 单元格内部垂直线 """
# b2d4.api.Borders(11).LineStyle = 1
# b2d4.api.Borders(11).Weight = 3

""" Borders(12) 单元格内部水平线 """
# b2d4.api.Borders(12).LineStyle = 1
# b2d4.api.Borders(12).Weight = 3


""" 合并 单元格 """
# b2d4.api.merge()
""" 拆分 单元格 """
# b2d4.api.unmerge()


""" 获取 最大行列数 """
# cell = sheet.used_range.last_cell
""" 最大 行数 """
# rows = cell.row
""" 最大 列数 """
# columns = cell.column
# print(rows, columns)


""" 插入、读取公式 """
# sheet_range('b1').formula = '=sum(c1 + d1)'  # 插入公式
# print(sheet_range('b1').formula)

"""排序，删除重复值"""
# 排序使用方法：
# 1、选择需要排序的区域。这里用 'a2' 是因为排序的数据送从第二行开始的，第一行是标题，不应该参与排序。
# 2、选择按那一列进行排序 Key1=sht.range('c2').api， 这里选择的是按 第 C 列排序，所以这里选择 c1 和 c2 都可以。
# 3、Order1=1 为升序，2为降序。
# sht1.range('a2', (rows, columns)).api.Sort(Key1=sht.range('c2').api, Order1=1)

# 删除重复值使用方法：
# RemoveDuplicates(3) 为按第3列内容进行删除重复项。
# sht1.range('a2', (rows, columns)).api.RemoveDuplicates(3)


"""同个表格复制、粘贴"""
# 复制 a2 到 a6 之间单元格的值，粘贴到'a15'中
# sht.range('a2', 'a6').api.Copy(sht.range('a15').api)

"""跨表格复制、粘贴"""
# my_values = sht_1.range('a2：d4').options(ndim=2).value  # 读取二维的数据
# sht_2.range('a1').value = my_values


""" 保存 Excel 文档 """
excel.save()
""" 关闭 Excel 文档 """
excel.close()
""" 退出 应用程序 """
app.quit()
# app.kill()
