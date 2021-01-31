#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: sycn_tushare.py
@time: 2021/1/22 21:05
"""
from datetime import datetime, timedelta

import tushare as ts

from config.config import TS_TOKEN, TS_SCHEMA_NAME
from db_operate.sqls.utils import pg_insert_on_conflict_batch

# 初始化全局变量
TODAY = datetime.now().strftime("%Y%m%d")
ZERO_DATE = "19000101"
pro = ts.pro_api(token=TS_TOKEN)


def trade_cal():
    table = "{schema_name}.trade_cal".format(schema_name=TS_SCHEMA_NAME)
    fields = ["cal_date", 'exchange', 'is_open']
    conflict_cols = ["cal_date"]
    update_cols = ['exchange', 'is_open']

    df = pro.trade_cal(end_date=TODAY, fields=fields)
    cols = df.columns.tolist()
    lines = df.to_records(index=False).tolist()
    pg_insert_on_conflict_batch(table=table, cols=cols, lines=lines,
                                      conflict_cols=conflict_cols, update_cols=update_cols)
    print(len(lines))


def index_basic():
    table = "{schema_name}.index_basic".format(schema_name=TS_SCHEMA_NAME)
    fields = ['ts_code', 'name', 'fullname', 'market', 'publisher', 'index_type', 'category', 'base_date', 'base_point',
              'list_date', 'weight_rule', 'desc', 'exp_date']
    conflict_cols = ["ts_code"]
    update_cols = ['name', 'fullname', 'market', 'publisher', 'index_type', 'category', 'base_date', 'base_point',
                   'list_date', 'weight_rule', 'description', 'exp_date']

    df = pro.index_basic(fields=fields)
    df.rename(columns={"desc": "description"}, inplace=True)
    cols = df.columns.tolist()
    lines = df.to_records(index=False).tolist()
    pg_insert_on_conflict_batch(table=table, cols=cols, lines=lines,
                                      conflict_cols=conflict_cols, update_cols=update_cols)
    print(len(lines))


def stock_basic():
    table = "{schema_name}.stock_basic".format(schema_name=TS_SCHEMA_NAME)
    fields = ['ts_code', 'symbol', 'name', 'area', 'industry', 'fullname', 'enname', 'market', 'exchange', 'curr_type',
              'list_status', 'list_date', 'delist_date', 'is_hs']
    conflict_cols = ["ts_code"]
    update_cols = ['symbol', 'name', 'area', 'industry', 'fullname', 'enname', 'market', 'exchange', 'curr_type',
                   'list_status', 'list_date', 'delist_date', 'is_hs']

    df = pro.stock_basic(fields=fields)
    cols = df.columns.tolist()
    lines = df.to_records(index=False).tolist()
    pg_insert_on_conflict_batch(table=table, cols=cols, lines=lines,
                                      conflict_cols=conflict_cols, update_cols=update_cols)
    print(len(lines))


def moneyflow_hsgt():
    table = "{schema_name}.moneyflow_hsgt".format(schema_name=TS_SCHEMA_NAME)
    fields = ['trade_date', 'ggt_ss', 'ggt_sz', 'hgt', 'sgt', 'north_money', 'south_money']
    conflict_cols = ['trade_date']
    update_cols = ['ggt_ss', 'ggt_sz', 'hgt', 'sgt', 'north_money', 'south_money']

    df = pro.moneyflow_hsgt(start_date=ZERO_DATE, end_date=TODAY, fields=fields)
    while len(df) > 0:
        # sleep(1)
        df = df.where(df.notnull(), None)

        cols = df.columns.tolist()
        lines = df.to_records(index=False).tolist()
        pg_insert_on_conflict_batch(table=table, cols=cols, lines=lines,
                                          conflict_cols=conflict_cols, update_cols=update_cols)

        next_end_date = datetime.strptime(sorted(df["trade_date"].tolist())[0], '%Y%m%d') - timedelta(days=1)
        next_end_date = next_end_date.strftime("%Y%m%d")

        df = pro.moneyflow_hsgt(start_date=ZERO_DATE, end_date=next_end_date, fields=fields)

        print(len(lines))


if __name__ == '__main__':
    trade_cal()
    index_basic()
    stock_basic()
    moneyflow_hsgt()
