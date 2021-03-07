#!/usr/bin/env python3
# -*- encoding: UTF-8 -*- #
"""
@author: ximing.wei
@file:   web_test.py
@time:   2021-03-05 17:58:50
"""
from http.server import HTTPServer, BaseHTTPRequestHandler

import py_utils


class Invoke(object):
    @staticmethod
    def error(method, msg):
        return str(
            {
                "errorCode": 400,
                "errorMethod": method,
                "errorMessage": msg
            }
        )

    @staticmethod
    def robot(msg):
        from send_robot import send_robot
        send_robot(msg)
        return str(
            {
                "msgCode": 200,
                "msgSucc": 'ok'
            }
        )


class RequestHandler(BaseHTTPRequestHandler):
    """
    请求路径合法则返回相应处理
    否则返回错误页面
    """

    def invoke(self, do_type: str):
        from urllib.parse import urlparse, parse_qsl

        parse = urlparse(self.path)
        real_path = parse.path.strip('/')

        if do_type.lower() == 'post':
            data = self.rfile.read(int(self.headers['content-length'])).decode()
        elif do_type.lower() == 'get':
            data = parse.query
        else:
            data = None

        message = dict(parse_qsl(data))['msg']

        try:
            return getattr(Invoke, real_path)(message)
        except AttributeError:
            return getattr(Invoke, 'error')(real_path, message)

    def do_GET(self):
        self.send_content(self.invoke(self.command))

    def do_POST(self):
        self.send_content(self.invoke(self.command))

    # 发送数据到客户端
    def send_content(self, content, status=200):
        # 开始回复
        self.send_response(status)
        self.send_header('Content-Type', 'text/json; charset=utf-8')
        self.end_headers()
        self.wfile.write(str(content).encode(encoding="utf-8"))


if __name__ == '__main__':
    serverAddress = (py_utils.get_ip(), 60000)
    print(serverAddress)
    service = HTTPServer(serverAddress, RequestHandler)
    print('程序已启动...')
    service.serve_forever()
