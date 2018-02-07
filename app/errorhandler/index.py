#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from flask import request

""" 响应处理 """

__author__ = '秦宝帅'


def res_success(data, msg="query success"):
    return json.dumps({"status": 1000, "data": data, "msg": msg})


def res_failure(msg="query failure"):
    return json.dumps({"status": 1001, "data": None, "msg": msg})


def req_params_required(data, req_json):
    for i in data:
        if i not in req_json.keys():
            return i
    return ''

if __name__ == '__main__':
    pass
