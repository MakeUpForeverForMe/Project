#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import ast
import sys
from datetime import datetime

import requests

robot_help = """参数应为 json 数据
{
    "key": "b1499656-602e-45c9-8722-2032e85aa8d0", // 机器人 Hook 的 Key（必有）
    "at": ["18812345678"],                         // 要提醒人的手机号（(逗号),分隔，可选）
    "msg": {                                       // 要发送消息（必有）
        "title": "这是测试数据",                     // 要发送消息的头部信息（必有）
        "k1": "v1",                                // 要发送消息的信息内容1（可选）
        "k2": "v2",                                // 要发送消息的信息内容2（可选）
        "k3": "v3"                                 // 要发送消息的信息内容3（可选）
    }
}
"""


class WWXRobot(object):
    def __init__(self, key: str):
        self.url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=' + key
        self.headers = {
            'Content-Type': 'application/json'
        }

    def _send(self, body: dict):
        response = requests.post(url=self.url, headers=self.headers, json=body)
        code = response.status_code
        msg = response.json().get('errmsg')
        assert code == 200, f'机器人 请求 返回状态 {code} 有错误，即机器人 Hook 的 Key 错误！'
        assert msg == 'ok', f'机器人 请求 返回结果 {msg} 有错误，即发送的数据非 Json 格式！'
        print('机器人 消息 发送成功！')
        return True

    def send_at(self, mentioned: list = None):
        self._send(
            {
                'msgtype': 'text',
                'text': {
                    'content': '',
                    'mentioned_mobile_list': mentioned,  # ['@all']
                }
            }
        )

    def send_markdown(self, content: str):
        """
        Markdown 仅支持以下格式
        1、标题 （支持1至6级标题，注意#与文字中间要有空格）
            # 标题一
            ## 标题二
            ### 标题三
            #### 标题四
            ##### 标题五
            ###### 标题六
        2、加粗
            **bold**
        3、链接
            [这是一个链接](http://work.weixin.qq.com/api/doc)
        4、行内代码段（暂不支持跨行）
            `code`
        5、引用
            > 引用文字
        6、字体颜色（只支持3种内置颜色）
            <font color="info">绿色</font>
            <font color="comment">灰色</font>
            <font color="warning">橙红色</font>
        """
        self._send(
            {
                'msgtype': 'markdown',
                'markdown': {
                    'content': content,
                }
            }
        )


def send_robot(json: str):
    json = ast.literal_eval(json.strip())

    robot = WWXRobot(json['key'])

    msg = dict(json['msg'])
    data = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n' \
           f'<font color="warning">{msg.pop("title")}</font>'

    for key, val in msg.items():
        data += f'\n>{key}: <font color="comment">{val}</font>'

    # print(str(data))

    robot.send_markdown(data)

    at_someone = list(json.get('at', None))
    if at_someone and len(at_someone) != 0:
        for i, someone in enumerate(at_someone):
            if isinstance(someone, int):
                someone = str(someone)
            at_someone[i] = someone

        robot.send_at(at_someone)


if __name__ == '__main__':
    args = sys.argv[1:]

    # args = ["""
    # {
    #     "key": "b1499656-602e-45c9-8722-2032e85aa8d0",
    #     "at": [17645774457, "18812345678"],
    #     "msg": {
    #         "title": "这是测试数据",
    #         "k1": "v1",
    #         "k2": "v2",
    #         "k3": "v3"
    #     }
    # }
    # """.strip()]

    args_length = len(args)

    if args_length != 1:
        print(f'输入的参数数量（要求 1 个）不正确（实际 {args_length} 个）{args}！\n{robot_help}')
        exit(1)

    send_robot(args[0])
