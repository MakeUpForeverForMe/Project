import os
import re

import sqlparse
from sqlparse.sql import Parenthesis, Identifier, TokenList
from sqlparse.tokens import Whitespace, Keyword, Token, Punctuation

base_dir = os.path.abspath(os.path.join(os.getcwd(), "."))

''' 获取文件并解析 '''
with open(f'{base_dir}/ods_new_s.hql', encoding='utf-8') as file:
    flag = False
    lines = []
    sql_str = ''

    for line in file.readlines():
        if not flag:
            if re.match(r'^create[\w\s]+table[\s\w`.]*\(', line.strip(),
                        re.I) and not re.match(r'.*(as|like).*', line, re.I):
                sql_str, flag = '', True

        if flag:
            sql_str += line.replace('\n', ' ')

            if re.match(r'.*;$', sql_str.strip(), re.I):
                flag = False
                lines.append(re.sub(r'\s+', ' ', sql_str.strip()))

data = {}

for line in lines:
    for token in sqlparse.parse(line)[0].tokens:
        if token.is_whitespace or token.is_keyword or token.ttype is Punctuation:  # 过滤空行
            continue

        # print(type(token), token.ttype, token)

        if isinstance(token, Identifier) and token.value != 'PARTITIONED':  # 获取库名、表名
            if not re.search(r'STORED AS', token.value, re.I):
                data['db_name'] = token.get_parent_name()
                data['tb_name'] = token.get_name()
            else:
                partition_stored = re.split(r'STORED AS', token.value, re.I)
                data['stored_as'] = partition_stored[1].strip()
                print('````````', partition_stored[0])

        if isinstance(token, Parenthesis):  # 获取字段名
            print('-------------------------------------------')
            for identifier in sqlparse.sql.IdentifierList(token).get_identifiers():
                print(identifier)
            # sqlparse.sql.IdentifierList(token).get_identifiers()
            # print(sqlparse.sql.IdentifierList(token).get_identifiers())
            columns = []
            # for sublist in token.flatten():
            #     if sublist.ttype is Whitespace or sublist.ttype is Punctuation or re.match(r'COMMENT', sublist.value,
            #                                                                                re.I):
            #         continue
            #     # sqlparse.sql.TokenList
            #     print(sublist)
            #     # columns += {}
            #     print(type(sublist), sublist.ttype, sublist)
            print('--------', token)

        if token.ttype is Token.Literal.String.Single:  # 获取表注释
            data['tb_comment'] = token.value.strip('\'')

    print(data)
    # print(line)
    # index_1 = line.index('(')
    # table_name = line[:index_1]
    # other = line[index_1 + 1:].strip()
    # print(table_name)
    # print(other)
    # find_db_tb_name = line.find('(')
    # get_column_key(line[find_db_tb_name + 1:].strip())
    # db_tb_name = line[0:find_db_tb_name].split(' ')[-1].strip('`').split('.')
    # data['db_name'] = db_tb_name[0]
    # data['tb_name'] = db_tb_name[1]
    # print(data)
    # for index, column in enumerate(line.split(' ')):
    #     print(index, column)

''' 打开文档 '''
# excel = xw.Book(f'{base_dir}/python_xlwings.xlsx')
# app = excel.app
# app.display_alerts = False
# app.screen_updating = False

# sheets = excel.sheets

''' 关闭文档 '''
# excel.save()
# excel.close()
# app.quit()
