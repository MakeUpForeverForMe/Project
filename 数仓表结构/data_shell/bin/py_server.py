#!/usr/bin/env python3
# -*- encoding: UTF-8 -*- #
"""
@author: ximing.wei
@file:   py_server
@time:   2021-03-05 17:58:50
"""
import socket
from http.server import HTTPServer, BaseHTTPRequestHandler

from .send_robot import send_robot, Error, NoKeyError, NotDictError, NotListError


class Invoke(object):
    @staticmethod
    def error(code=400, msg='error', method=''):
        return str(
            {
                "errorCode": code,
                "errorMethod": method,
                "errorMessage": msg
            }
        )

    @staticmethod
    def robot(msg):
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
        error_attr = getattr(Invoke, 'error')
        from urllib.parse import urlparse, parse_qsl, unquote

        url_parser = urlparse(self.path)
        real_path = url_parser.path.strip('/')

        global data
        if do_type.lower() == 'post':
            content_length = self.headers['content-length']
            if content_length:
                data = unquote(self.rfile.read(int(content_length)).decode(), encoding='utf-8')
            else:
                return error_attr(method=real_path, msg="""未传参数！""")
        elif do_type.lower() == 'get':
            query = unquote(url_parser.query)
            if query:
                data = dict(parse_qsl(query)).get('msg', None)
                if not data:
                    return error_attr(method=real_path, msg="""参数 Key 值【msg】未获取到！""")
        else:
            data = None

        try:
            return getattr(Invoke, real_path)(data)
        except AttributeError:
            return error_attr(real_path, """路径错误，没有这个函数！""")
        except ValueError as e:
            return error_attr(real_path, f"""输入的参数值，不符合 '{real_path}' 的规范！{e}""")
        except NoKeyError as e:
            return error_attr(real_path, e.msg)
        except NotDictError as e:
            return error_attr(real_path, e.msg)
        except NotListError as e:
            return error_attr(real_path, e.msg)
        except Error as e:
            return error_attr(real_path, e.msg)

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


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        return s.getsockname()[0]
    finally:
        s.close()


def run_server(handler_class=RequestHandler, port=60000):
    server_address = (get_ip(), port)
    service = HTTPServer(server_address, handler_class)
    print(f'{server_address} 程序已启动...', flush=True)
    service.serve_forever()


if __name__ == '__main__':
    run_server()
