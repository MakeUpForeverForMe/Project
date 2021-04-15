import os

import xlwings


class ExcelWings:
    def __init__(self, excel_name):
        """ 初始化 Excel """
        """ 打开 Excel """
        self.excel = xlwings.Book(excel_name)
        """ 获取 Excel 程序对象 """
        self.app = self.excel.app
        """ 设置 App 对象属性 """
        self.app.display_alerts = False
        self.app.screen_updating = False

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


if __name__ == '__main__':
    base_dir = os.path.abspath(os.path.join(os.getcwd(), "."))
    ew = ExcelWings(f'{base_dir}/python_xlwings.xlsx')
    ew.close()
