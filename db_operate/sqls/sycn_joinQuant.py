#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: sycn_jq.py
@time: 2021/1/27 17:11

数据详情参考 https://www.joinquant.com/help/api/help#JQData
"""
from datetime import datetime

import jqdatasdk as jqd

from config.config import JQ_USER, JQ_PASSWD, JQ_SCHEMA_NAME
from db_operate.sqls.utils import pg_insert_on_conflict_batch

# 初始化全局变量
from db_operate.utils.time import time_cost

TODAY = datetime.now().strftime("%Y-%m-%d")
ZERO_DATE = "1900-01-01"
jqd.auth(JQ_USER, JQ_PASSWD)

@time_cost
def securities():
    table = "{schema_name}.securities".format(schema_name=JQ_SCHEMA_NAME)
    conflict_cols = ['code']
    update_cols = ['display_name', 'name', 'start_date', 'end_date', 'type', 'updated_at']

    df = jqd.get_all_securities(
        types=['stock', 'fund', 'index', 'futures', 'options', 'etf', 'lof', 'fja', 'fjb', 'open_fund', 'bond_fund',
               'stock_fund', 'QDII_fund', 'money_market_fund', 'mixture_fund'], date=None)
    df = df.where(df.notnull(), None)
    df.reset_index(inplace=True)
    df.rename(columns={"index": "code"}, inplace=True)
    df = df.astype({"start_date": str, "end_date": str})

    cols = df.columns.tolist()
    lines = df.to_records(index=False).tolist()
    pg_insert_on_conflict_batch(table=table, cols=cols, lines=lines, conflict_cols=conflict_cols,
                                update_cols=update_cols)

    print("完成更新，获取数据：", len(lines))


def index_daily():
    fields = ['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit', 'low_limit', 'avg', 'pre_close', 'paused']
    r = jqd.get_price("000001.XSHG", start_date=ZERO_DATE, end_date=TODAY, frequency='daily', fields=fields,
                      skip_paused=False, fq='pre')
    print(r)


if __name__ == '__main__':
    securities()
    # index_daily()
