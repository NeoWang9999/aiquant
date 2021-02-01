#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: init_tables.py
@time: 2021/1/22 21:06
"""
from db_operate.db_sqls.pg_sqls import TSTableCreateSQL, JQTableCreateSQL
from db_operate.db_sqls.utils import pg_execute


def ts_init():
    pg_execute(TSTableCreateSQL.schema)
    pg_execute(TSTableCreateSQL.trade_cal)
    pg_execute(TSTableCreateSQL.index_basic)
    pg_execute(TSTableCreateSQL.stock_basic)
    pg_execute(TSTableCreateSQL.moneyflow_hsgt)


def jq_init():
    # pg_execute(JQTableCreateSQL.schema)
    # pg_execute(JQTableCreateSQL.securities)
    # pg_execute(JQTableCreateSQL.index_stocks)
    pg_execute(JQTableCreateSQL.index_daily)



if __name__ == '__main__':
    # ts_init()
    jq_init()
