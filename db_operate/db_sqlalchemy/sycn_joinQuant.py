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
from db_operate.db_sqlalchemy.jq_sqlalchemy import JQ_Session, Security

from db_operate.utils.time import time_cost

"""
效率比直接用 SQL 慢 6倍，弃用。。
"""


# 初始化全局变量
TODAY = datetime.now().strftime("%Y-%m-%d")
ZERO_DATE = "1900-01-01"
jqd.auth(JQ_USER, JQ_PASSWD)

@time_cost
def securities():
    table = "{schema_name}.securities".format(schema_name=JQ_SCHEMA_NAME)
    conflict_cols = ['code']
    update_cols = ['display_name', 'name', 'start_date', 'end_date', 'type', 'update_at']

    df = jqd.get_all_securities(
        types=['stock', 'fund', 'index', 'futures', 'options', 'etf', 'lof', 'fja', 'fjb', 'open_fund', 'bond_fund',
               'stock_fund', 'QDII_fund', 'money_market_fund', 'mixture_fund'], date=None)
    df = df.where(df.notnull(), None)
    df.reset_index(inplace=True)
    df.rename(columns={"index": "code"}, inplace=True)
    # df = df.astype({"start_date": str, "end_date": str})
    lines = df.to_dict(orient="records")
    print("获取数据：", len(df))

    # tb = jq_metadata.tables.get(table)
    session = JQ_Session()
    session.bulk_insert_mappings(Security, lines)
    session.commit()

    print("完成更新：", len(df))


def index_daily():
    fields = ['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit', 'low_limit', 'avg', 'pre_close', 'paused']
    r = jqd.get_price("000001.XSHG", start_date=ZERO_DATE, end_date=TODAY, frequency='daily', fields=fields,
                      skip_paused=False, fq='pre')
    print(r)


if __name__ == '__main__':
    securities()
    # index_daily()
