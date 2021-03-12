#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import os
import re
import sys
from configparser import ConfigParser
from email.mime.text import MIMEText
from email.header import Header
from email.utils import parseaddr, formataddr
from smtplib import SMTP


class Mail(object):
    """ 属性设置 """
    ip = str(os.popen("ifconfig | grep -Po 'inet[ ]\K[^ ]+' | grep -v '127'").readlines()[0]).strip()

    prod_send_host = '10.80.0.133'
    prod_send_user = '生产告警邮箱<DataCenter-Alert@services.weshreholdings.com>'

    test_send_host = '10.83.0.44'
    test_send_user = '测试告警邮箱<DataCenter-Alert-sit@weshreholdings.com.cn>'

    default_send_host = ''
    default_send_user = ''

    if re.match(r'10.80.*', ip):
        default_send_host = prod_send_host
        default_send_user = prod_send_user
    elif re.match(r'10.83.*', ip):
        default_send_host = test_send_host
        default_send_user = test_send_user
    else:
        default_send_host = test_send_host
        default_send_user = test_send_user

    default_send_pass = ''
    default_receivers = '郭超<chao.guo@weshareholdings.com>' \
                        ',' \
                        '檀剑<jian.tan@weshareholdings.com>' \
                        ',' \
                        '刘焕<huan.liu@weshareholdings.com>' \
                        ',' \
                        '魏喜明<ximing.wei@weshareholdings.com>' \
                        ',' \
                        '王禹衡<yuheng.wang@weshareholdings.com>' \
                        ',' \
                        '黄育楠<yunan.huang@weshareholdings.com>'

    def __init__(self, path=None):

        self.charset = 'utf-8'
        self.str_split = ','
        self.end_with_mail = 'qq.com'

        self.sender = 'sender'
        self.sender_host = 'host'
        self.sender_user = 'user'
        self.sender_pass = 'pass'

        self.receiver = 'receiver'
        self.receiver_users = 'users'

        self.conf = ConfigParser(allow_no_value=True)  # 创建 ConfigParser 解析对象
        self.conf.read(path, encoding=self.charset)

        self.server = None

    def __formataddr_list__(self, addresses):
        addr_list = set()
        for address in str(addresses).split(self.str_split):
            name, addr = parseaddr(address)
            addr_list.add(formataddr((Header(name, 'utf-8').encode(), addr)))

        return addr_list

    def sender_check(self, host=default_send_host, user=default_send_user, password=default_send_pass):
        # 检查指定节点是否存在，如果不存在则创建
        if not self.conf.has_section(self.sender):
            self.conf.add_section(self.sender)

        if not self.conf.has_option(self.sender, self.sender_host):
            self.conf.set(self.sender, self.sender_host, host)

        if not self.conf.has_option(self.sender, self.sender_user):
            self.conf.set(self.sender, self.sender_user, user)

        if not self.conf.has_option(self.sender, self.sender_pass):
            self.conf.set(self.sender, self.sender_pass, password)

    def receiver_check(self, receiver_users=default_receivers):
        # 检查指定节点是否存在，如果不存在则创建
        if not self.conf.has_section(self.receiver):
            self.conf.add_section(self.receiver)

        if not self.conf.has_option(self.receiver, self.receiver_users):
            self.conf.set(self.receiver, self.receiver_users, receiver_users)

    def send_mail(self, sub, msg):
        """ 设置邮件消息 """
        mail_msg = MIMEText(msg, 'plain', _charset=self.charset)  # 根据邮件内容，获取邮件
        mail_msg['From'] = self.str_split.join(self.__formataddr_list__(self.conf.get(self.sender, self.sender_user)))
        mail_msg['To'] = self.str_split.join(self.__formataddr_list__(self.conf.get(self.receiver, self.receiver_users)))
        mail_msg['Subject'] = Header('{}'.format(sub), charset=self.charset)

        # print(mail_msg.items())

        self.server = SMTP(self.conf.get(self.sender, self.sender_host), 25)  # 25 为 SMTP 端口号

        if self.conf.get(self.sender, self.sender_host).endswith(self.end_with_mail):
            self.server.login(re.findall(r'<(.*?)>', self.conf.get(self.sender, self.sender_user))[0],
                              self.conf.get(self.sender, self.sender_pass))

        self.server.sendmail(self.conf.get(self.sender, self.sender_user),
                             self.conf.get(self.receiver, self.receiver_users).split(self.str_split),
                             mail_msg.as_string())  # 发件人、收件人、消息

    def close(self):
        self.server.quit()  # 退出对话
        self.server.close()  # 关闭连接


if __name__ == '__main__':

    """ 获取参数 """
    args_length = len(sys.argv[1:])
    if args_length == 2:
        in_path = None
        subject = sys.argv[1]
        message = sys.argv[2]
    elif args_length == 3:
        in_path = sys.argv[1]
        subject = sys.argv[2]
        message = sys.argv[3]
    else:
        print("Error：请指定：[配置文件、]主题、消息内容", sys.argv)
        sys.exit(1)

    if args_length == 3 and not os.path.exists(sys.argv[1]):
        print("Error：配置文件 未找到！")
        sys.exit(1)

    mail = Mail(in_path)
    mail.sender_check()
    mail.receiver_check()
    mail.send_mail(subject, message)
    mail.close()
