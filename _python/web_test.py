#!/usr/bin/env python3
# -*- encoding: UTF-8 -*- #
"""
@author: ximing.wei
@file:   web_test.py
@time:   2021-03-05 17:58:50
"""
from http.server import HTTPServer, BaseHTTPRequestHandler


class RequestHandler(BaseHTTPRequestHandler):
    """
    请求路径合法则返回相应处理
    否则返回错误页面
    """

    @staticmethod
    def error(msg):
        return str({"msgCode": 400, "errorMethod": msg})

    @staticmethod
    def robot(msg):
        import send_robot
        send_robot.send_robot(msg)
        return str({"msgCode": 200, "msgSucc": 'ok'})

    def invoke(self, do_type: str):
        from urllib.parse import urlparse, parse_qsl

        data = None
        real_path = None
        if do_type == 'post':
            real_path = self.path
            data = self.rfile.read(int(self.headers['content-length'])).decode()
        elif do_type == 'get':
            parse_result = urlparse(self.path)
            real_path = parse_result.path
            data = parse_result.query

        message = dict(parse_qsl(data))['msg']
        method_name = real_path.strip('/')

        try:
            return getattr(self, method_name)(message)
        except AttributeError:
            return getattr(self, 'error')(method_name)

    def do_GET(self):
        self.send_content(self.invoke('get'))

    def do_POST(self):
        self.send_content(self.invoke('post'))

    # 发送数据到客户端
    def send_content(self, content, status=200):
        # 开始回复
        self.send_response(status)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.end_headers()
        self.wfile.write(content.encode(encoding="utf-8"))


if __name__ == '__main__':
    serverAddress = ('', 8080)
    service = HTTPServer(serverAddress, RequestHandler)
    print('程序已启动...')
    service.serve_forever()
