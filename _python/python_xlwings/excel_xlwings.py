import os

import xlwings as xw


class ExcelWings:

    def __init__(self, excel_name):
        """ 操作 Excel 文档 """
        self.excel = xw.Book(excel_name)
        self.app = self.excel.app
        self.app.display_alerts = False
        self.app.screen_updating = False

    def close(self, excel: xw.Book = None, app: xw.App = None):
        """ 关闭 Excel 文档 """
        if excel is None and app is None:
            self.excel.close()
            self.app.quit()
        if excel is not None:
            excel.close()
        if app is not None:
            app.quit()


if __name__ == '__main__':
    base_dir = os.path.abspath(os.path.join(os.getcwd(), "."))
    ew = ExcelWings(f'{base_dir}/python_xlwings.xlsx')
    ew.close()
