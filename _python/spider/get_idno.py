# -*- coding: UTF-8 -*-

import requests
from lxml import etree
import pymysql
import re


class Government(object):
    def __init__(self):
        self.one_url = 'http://www.mca.gov.cn/article/sj/xzqh/2020/'
        self.headers = {'User-Agent': 'Mozilla/5.0'}
        self.connection = pymysql.connect(
            # 'localhost', 'root', '000000', 'government', charset='utf8'
            '192.168.18.100', 'root', '000000', 'government', charset='utf8'
        )
        self.cursor = self.connection.cursor()

    # 提取二级页面链接（假链接）- 一定是最新的哪个链接
    def get_false_link(self):

        # xpath: //a[@class="artitlelist"]
        html = requests.get(
            url=self.one_url,
            headers=self.headers
        ).content.decode('utf-8', 'ignore')
        # 解析
        parse_html = etree.HTML(html)
        a_list = parse_html.xpath('//a[@class="artitlelist"]')
        for a in a_list:
            # print(a.attrib)  # 打印标签内容
            # title=a.xpath('./@title')[0]
            title = a.get('title')
            if re.findall('.*以上行政区划代码', title, re.S):
                return 'http://www.mca.gov.cn' + a.get('href')

    # 提取真实二级页面链接（返回数据）
    def get_true_link(self):
        # 获取响应内容
        false_link = self.get_false_link()
        html = requests.get(url=false_link, headers=self.headers).text
        # 打印响应内容，查看真实链接的跳转，匹配出真实链接
        pattern = re.compile(r'window.location.href="(.*?)"', re.S)
        real_link = pattern.findall(html)[0]

        # 实现增量爬取
        # 先抓数据
        self.get_data(real_link)

    # 真正提取数据函数
    def get_data(self, real_link):
        html = requests.get(
            url=real_link,
            headers=self.headers
        ).text
        # 基准xpath: //tr[@height="19"]
        parse_html = etree.HTML(html)
        tr_list = parse_html.xpath('//tr[@height="19"]')
        id_no_city = []
        for tr in tr_list:
            try:
                code = tr.xpath('./td[2]/text()')[0]
            except IndexError:
                code = tr.xpath('./td[2]//span[@lang="EN-US"]/text()')[0]
            name = tr.xpath('./td[3]/text()')[0]
            id_no_city.append((code, name))
            # print(code, name)
        sql = 'insert into idno_city value (%s,%s)'
        self.cursor.executemany(sql, id_no_city)
        self.connection.commit()
        # print(id_no_city)
        # sys.exit(0)

    # 主函数
    def main(self):
        self.get_true_link()


if __name__ == '__main__':
    spider = Government()
    spider.main()
