#!/usr/bin/env python3
# -*- encoding: UTF-8 -*- #
"""
@author: ximing.wei
@file:   read_json_2_hadoop_conf
@time:   2021-06-04 16:36:45
"""
import json

input_file_name = "D:\\Users\\ximing.wei\\Desktop\\emr-hs0wld0a-2021-6-4.json"
write_file_name = "D:\\Users\\ximing.wei\\Desktop\\hive-site.xml"
conf = ""
with open(input_file_name, 'r') as file:
    lines = dict(json.loads(file.read()))
    conf += "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
    conf += "<configuration>\n"
    for (key, val) in lines.items():
        conf += "  <property>\n"
        conf += f"    <name>{key}</name>\n"
        conf += f"    <value>{val}</value>\n"
        conf += "  </property>\n"
    conf += "</configuration>"

    print(conf)

with open(write_file_name, 'w') as file:
    file.write(conf)
