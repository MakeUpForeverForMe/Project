# -*- coding: UTF-8 -*-
import re
import sys

import pymysql
import requests
from lxml import etree

# one_url = 'http://www.mca.gov.cn/article/sj/xzqh/2020/?2'
one_url = 'http://www.mca.gov.cn/article/sj/xzqh/1980/?3'
headers = {'User-Agent': 'Mozilla/5.0'}

# connection = pymysql.connect('localhost', 'root', '000000', 'government', charset='utf8')
connection = pymysql.connect('192.168.18.100', 'root', '000000', 'government', charset='utf8')
cursor = connection.cursor()

# xpath: //a[@class="artitlelist"]
html = requests.get(url=one_url, headers=headers).content.decode('utf-8')
# print(html)
# sys.exit()
# 解析
url_list = []
sql = 'insert ignore idno_city value (%s,%s,%s)'
for xpath in etree.HTML(html).xpath('//a[@class="artitlelist"]'):
    url = 'http://www.mca.gov.cn' + xpath.get('href')
    # print(a.attrib)  # 打印标签内容
    # title=a.xpath('./@title')[0]
    title = xpath.get('title')
    data_year = str(title).strip()[0:4]
    print(data_year, '-- 原始 --', title, url)

    if re.findall('^2020年.*', title, re.S) and not re.findall('.*县以上.*', title, re.S):
        continue
    elif url == 'http://www.mca.gov.cn/article/sj/xzqh/1980/201507/20150715854849.shtml':
        break
    elif re.findall('^2020年.*', title, re.S) and re.findall('.*县以上.*', title, re.S):
        html_false = requests.get(url=url, headers=headers).text
        # 打印响应内容，查看真实链接的跳转，匹配出真实链接
        url = re.compile(r'window.location.href="(.*?)"', re.S).findall(html_false)[0]
    elif re.findall('^201[1-9]年中华人民共和国行政区划代码.*', title, re.S):
        html_false = requests.get(url=url, headers=headers).text
        div = etree.HTML(html_false).xpath('//div[@class="content"]')[0]
        div_name = div.xpath('./p//a/text()')[0]
        url = div.xpath('./p//a')[0].get('href')
    else:
        html_false = requests.get(url=url, headers=headers).text
        url = re.compile(r'window.location.href="(.*?)"', re.S).findall(html_false)[0]

    print('-- title --', title, url)

    real_html = requests.get(url=url, headers=headers).text

    # 基准xpath: //tr[@height="19"]
    # tr_list = etree.HTML(real_html).xpath('//tr[@height="17"]')
    # tr_list = etree.HTML(real_html).xpath('//tr[@height="19"]')
    tr_list = etree.HTML(real_html).xpath('//tr[@height="20"]')
    id_no_city = []
    for tr in tr_list:
        td_list = tr.xpath('./td/text()')
        if len(td_list) != 2:
            continue
        # print(td_list)

        # try:
        #     code = tr.xpath('./td[2]/text()')[0]
        # except IndexError:
        #     code = tr.xpath('./td[2]//span[@lang="EN-US"]/text()')[0]
        # name = tr.xpath('./td[3]/text()')[0]
        code = td_list[0]
        name = td_list[1]
        id_no_city.append((code, name, data_year))
        print([data_year, code, name])
    cursor.executemany(sql, id_no_city)
    connection.commit()

sys.exit()
