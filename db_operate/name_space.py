#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: name_space.py
@time: 2021/1/31 18:17
"""

class JQNameSpace:
    schema = "jq_data"
    all_trade_days = "all_trade_days"
    securities = "securities"
    index_stocks = "index_stocks"
    index_daily = "index_daily"
    moneyflow_hsgt = "moneyflow_hsgt"
    fund_daily = "fund_daily"
    stock_daily = "stock_daily"

    @staticmethod
    def full_table_name(table_name):
        tbn = getattr(JQNameSpace, table_name)
        return "{schema_name}.{table_name}".format(schema_name=JQNameSpace.schema, table_name=tbn)


class TSNameSpace:
    schema = "ts_data"
    trade_cal = "trade_cal"
    stock_basic = "stock_basic"
    moneyflow_hsgt = "moneyflow_hsgt"
    index_basic = "index_basic"

    @staticmethod
    def full_table_name(table_name):
        tbn = getattr(TSNameSpace, table_name)
        return "{schema_name}.{table_name}".format(schema_name=TSNameSpace.schema, table_name=tbn)


class AKNameSpace:
    schema = "ak_data"
    moneyflow_hsgt = "moneyflow_hsgt"

    @staticmethod
    def full_table_name(table_name):
        tbn = getattr(AKNameSpace, table_name)
        return "{schema_name}.{table_name}".format(schema_name=TSNameSpace.schema, table_name=tbn)

