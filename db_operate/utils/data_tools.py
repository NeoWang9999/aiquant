#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: dataprocess.py
@time: 2021/1/31 18:39
"""
import decimal
import json
from datetime import datetime, date


class SQLEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)

        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")

        if isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")

        return super(SQLEncoder, self).default(obj)

