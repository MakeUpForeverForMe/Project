#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os
import re
from configparser import ConfigParser

config = ConfigParser(allow_no_value=True)  # 创建对象

config.read('configparser_learning.ini', encoding='utf-8')  # 读取配置文件，如果配置文件不存在则创建
# secs = config.sections()  # 获取所有的节点名称
# print(secs)  # ['section1', 'section2']
#
# options = config.options('section1')  # 获取指定节点的所有 key
# print(options)  # ['name', 'age']
#
# item_list = config.items('section1')  # 获取指定节点的键值对
# print(item_list)  # [('name', 'wang'), ('age', '18')]

val = str(config['section1']['name']).strip().split('\n')  # 获取节点 section1 的 name 属性
print('section1', 'name', val)  # wang

default_receivers = [
    '郭超<chao.guo@weshareholdings.com>',
    '檀剑<jian.tan@weshareholdings.com>',
    '刘焕<huan.liu@weshareholdings.com>',
    '魏喜明<ximing.wei@weshareholdings.com>',
    '王禹衡<yuheng.wang@weshareholdings.com>',
    '黄育楠<yunan.huang@weshareholdings.com>',
]

print(default_receivers)
# config.read('mail_qq.ini', encoding='utf-8')  # 读取配置文件，如果配置文件不存在则创建
# val = config['receiver']['users']  # 获取节点 section1 的 name 属性
# print('receiver', 'users', val)  # wang

""" 判断是哪种操作系统 """
system = os.name
default_send_host = ''
default_send_user = ''
ip = ''
""" 属性设置 """
if system == 'nt':
    ip = re.findall(r'[\d.]+', str(os.popen('ipconfig | find /i "IPv4 地址"').readlines()[0]))[-1].strip()
    pass
elif system == 'posix':
    ip = str(os.popen("ifconfig | grep -Po 'inet[ ]\K[^ ]+' | grep -v '127'").readlines()[0]).strip()

print(ip)

# val = config.getint('section1', 'age')  # 获取节点 section1 的 age 属性，属性需要是int型，否则ValueError
# print(val)  # 18
#
# val = config.has_section('section1')  # 检查指定节点是否存在，返回True或False
# print(val)  # True
#
# val = config.has_option('section1', 'age')  # 检查指定节点中是否存在某个key，返回True或False
# print(val)  # True
#
# # 增删改
# # config.add_section("node")  # 添加一个节点，节点名为node, 此时添加的节点node尚未写入文件
# # config.write(open('configparser_learning.ini', "w"))  # 将添加的节点node写入配置文件
#
# # config.remove_section("node")  # 删除一个节点，节点名为node, 删掉了内存中的节点node
# # config.write(open("configparser_learning.ini", "w"))  # 将删除节点node后的文件内容回写到配置文件
#
# config.set("node", "k1", "v1")  # 在已存在的节点中添加一个键值对k1 = v1 ，如果key已经存在，则修改value
# # 如果该节点不存在则报错  # configparser.NoSectionError: No section: 'section'
#
# config.write(open("configparser_learning.ini", "w"))
