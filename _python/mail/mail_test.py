#!/usr/bin/python
# -*- coding: UTF-8 -*-

import smtplib
import sys
from email.mime.text import MIMEText
from email.header import Header
from configparser import ConfigParser
from email.utils import formataddr

""" 设置邮件消息 """
msg = MIMEText('message', 'plain', _charset='utf-8')  # 根据邮件内容，获取邮件
msg['From'] = Header('魏喜明<664651151@qq.com>', charset='utf-8')
msg['To'] = Header(
    ','.join([formataddr(('魏喜明', '664651151@qq.com')), formataddr(('魏喜明', 'ximing.wei@weshareholdings.com'))]),
    charset='utf-8')
msg['Subject'] = Header('{}'.format('测试'), charset='utf-8')

""" 发送邮件 """
server = smtplib.SMTP('smtp.qq.com', 25)  # 25 为 SMTP 端口号
server.login('664651151@qq.com', 'qonjgkozycnjbfhg')
server.sendmail('664651151@qq.com', ['664651151@qq.com', 'ximing.wei@weshareholdings.com'],
                msg.as_string())  # 发件人、收件人、消息
server.quit()  # 退出对话
server.close()  # 关闭连接
