from urllib import request
from urllib.parse import urljoin

from bs4 import BeautifulSoup


class Download(object):

    def __init__(self):
        self.urls = []
        self.data = set()

    def add_urls(self, urls):
        if urls is None or len(urls) == 0:
            return
        for url in urls:
            self.urls.append(url)

    def has_url(self):
        return len(self.urls) != 0

    def get_url(self):
        return self.urls.pop()

    def parse(self, url):
        if url is None:
            return None
        response = request.urlopen(url)
        if response.getcode() != 200:
            return None
        links = []
        soup = BeautifulSoup(response.read(), 'html.parser', from_encoding='utf-8')

        # if len(links) == 0:
        #     tds = soup.find('div')
        #     if tds is not None:
        #         print(url)
        #         tds = tds.find_all('tr')
        #         for td in tds:
        #             text = td.text.replace(u'\xa0', '').replace(' ', '').replace('\t', '').replace('\n', ',').strip(',')
        #             if len(text) != 0:
        #                 pattern = re.compile('^\d{6}.*').search(text)
        #                 if pattern is not None:
        #                     self.data.add(pattern.group(0))
        #
        # if len(links) == 0:
        #     pattern = re.compile(r'window.location.href=.*')
        #     links = soup.find_all('script', text=pattern)
        #     if len(links) != 0:
        #         self.parse(pattern.search(str(links[0])).group(0).split('"')[1])

        if len(links) == 0:
            links = soup.find_all('div', class_='content')
            if len(links) != 0:
                for link in links:
                    href = link.find('a')['href']
                    print(href)
                    # self.parse(href)

        if len(links) == 0:
            links = soup.find_all('a', class_='artitlelist')
            if len(links) != 0:
                for link in links:
                    self.parse(urljoin(url, link['href']))

    def write(self, path):
        pass


if __name__ == '__main__':
    count = 1
    path = ''
    download = Download()
    urls = [
        # 'http://www.mca.gov.cn/article/sj/xzqh/2020/',
        'http://www.mca.gov.cn/article/sj/xzqh/1980/',
    ]
    download.add_urls(urls)
    while download.has_url():
        url = download.get_url()
        print('download %d : %s' % (count, url))  # unquote(new_url)
        download.parse(url)
        count += 1
    download.write(path)
