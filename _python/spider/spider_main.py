from spider import url_manager, html_downloader, html_parser, html_outputer
from urllib.parse import unquote


class SpiderMain(object):
    def __init__(self):
        self.urls = url_manager.UrlManager()
        self.downloader = html_downloader.HtmlDownloader()
        self.parser = html_parser.HtmlParser()
        self.outputer = html_outputer.HtmlOutputer()

    def craw(self, root_url):
        count_s = 1
        count_e = 1
        self.urls.add_new_url(root_url)
        while self.urls.has_new_url():
            try:
                new_url = self.urls.get_new_url()
                print('craw %d : %s' % (count_s, unquote(new_url)))
                html_cont = self.downloader.download(new_url)
                new_urls, new_data = self.parser.parse(unquote(new_url), html_cont)
                self.urls.add_new_urls(new_urls)
                self.outputer.collect_data(new_data)

                if count_s == count_e:
                    break
            except:
                print('craw failed')

            count_s += 1

        self.outputer.output_html()


if __name__ == "__main__":
    root_url = 'https://baike.baidu.com/item/Python/407313'
    obj_spider = SpiderMain()
    obj_spider.craw(root_url)
