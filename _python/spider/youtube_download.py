#!/usr/bin/env python3
# -*- encoding: UTF-8 -*- #
"""
@author: 魏喜明
@file:   youtube_download
@time:   2021-05-04 12:22:37
"""
import requests

url = 'https://www.youtube.com/playlist?list=PLmOn9nNkQxJHrxjtTBM_WI1Lu2N2_fpfR'

html = requests.get(url=url).content.decode('utf-8', 'ignore')

print(html)