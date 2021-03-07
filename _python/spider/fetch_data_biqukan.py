import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


class Downloader(object):
    def __init__(self):
        self.nums = 0
        self.urls = []
        self.names = []

    def __get_soup__(self, url):
        req = requests.get(url)
        req.encoding = 'gbk'
        return BeautifulSoup(req.text, "html.parser")

    def get_downloader_url(self, url):
        a = self.__get_soup__(url).find('div', class_='listmain').find_all('a')
        a15 = a[15:]
        self.nums = len(a15)
        for data in a15:
            self.names.append(data.text)
            self.urls.append(urljoin(url, data.get('href')))

    def get_contents(self, url):
        return self.__get_soup__(url).find('div', class_='showtxt').text.replace('\xa0' * 8, '\n\n')

    def writer(self, name, path, text):
        with open(path, 'a', encoding='utf-8') as r:
            r.write(name + '\n')
            r.writelines(text)


if __name__ == '__main__':
    target = 'http://www.biqukan.com/1_1094/'

    dl = Downloader()
    dl.get_downloader_url(target)
    for i in range(dl.nums):
        dl.writer(dl.names[i], '一念永恒.txt', dl.get_contents(dl.urls[i]))
        sys.stdout.write("  已下载:%.3f%%" % float(i / dl.nums) + '\r')
        sys.stdout.flush()
    print('《一年永恒》下载完成')
