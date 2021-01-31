#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: NorthFlow.py
@time: 2021/1/22 19:03
"""
import tushare as ts

from config.config import TS_TOKEN
from db_operate.sqls.utils import pg_fetchall


def north_flow_avg_last_252(start_date, end_date):
    """
    获取时间段内北向资金的均值
    Args:
        start_date: yyyymmdd
        end_date: yyyymmdd

    Returns: DataFrame

    """
    pro = ts.pro_api(token=TS_TOKEN)
    df = pro.moneyflow_hsgt(start_date=start_date, end_date=end_date)
    return df


if __name__ == '__main__':
    r = pg_fetchall("aiquant.moneyflow_hsgt", "*")
    print(r)
