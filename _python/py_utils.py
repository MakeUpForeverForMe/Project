#!/usr/bin/env python3
# -*- encoding: UTF-8 -*- #
"""
@author: 魏喜明
@file:   py_utils.py
@time:   2021-03-07 15:05:45
"""
import socket


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        return s.getsockname()[0]
    finally:
        s.close()
