#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: utils.py
@time: 2021/1/22 17:32
"""
from datetime import datetime, timedelta
from typing import Union

import numpy as np
import pandas_market_calendars as mcal

from strategy_lab.lab_config.config import ZERO_DATE, LAST_DATE


xshg = mcal.get_calendar('XSHG')  # XSHG = 上交所
trade_days_df = xshg.schedule(ZERO_DATE, LAST_DATE, tz=xshg.tz.zone)
trade_days_df.drop(["market_open", "market_close"], axis=1, inplace=True)
trade_days_df.reset_index(inplace=True)
trade_days_df.rename(columns={"index": "trade_date"}, inplace=True)
trade_days_df = trade_days_df.astype(str)


def first_true(iterable, default=False, pred=None):
    """Returns the first true value in the iterable.

    If no true value is found, returns *default*

    If *pred* is not None, returns the first item
    for which pred(item) is true.

    """
    # first_true([a,b,c], x) --> a or b or c or x
    # first_true([a,b], x, f) --> a if f(a) else b if f(b) else x
    return next(filter(pred, iterable), default)


def datetime2str(date: Union[str, datetime], fmt: str = "%Y-%m-%d") -> str:

    if isinstance(date, str):
        d = date
    elif isinstance(date, datetime):
        d = date.strftime(fmt)
    else:
        raise TypeError("Unsupport type for date: {}".format(type(date)))
    return d


def str2datetime(date: Union[str, datetime], fmt: str = "%Y-%m-%d") -> datetime:

    if isinstance(date, str):
        d = date
    elif isinstance(date, datetime):
        d = date.strftime(fmt)
    else:
        raise TypeError("Unsupport type for date: {}".format(type(date)))
    return d


def date_calc(date: Union[str, datetime], add_days: int, fmt: str = "%Y-%m-%d") -> str:
    """
    Args:
        date:
        add_days:
        fmt:

    Returns:
        date: string
    """
    d = str2datetime(date, fmt)
    d += timedelta(days=add_days)
    return d.strftime(fmt)


def trade_date_calc(date: Union[str, datetime], add_days: int, fmt: str = "%Y-%m-%d") -> str:
    """
    Args:
        date:
        add_days:
        fmt:

    Returns:
        date: string
    """
    d = datetime2str(date, fmt)
    d_row = trade_days_df.loc[trade_days_df["trade_date"] == d]
    if d_row.empty:
        raise ValueError("{} is not a trade date!".format(d))
    tar_idx = d_row.index[0] + add_days
    if tar_idx < trade_days_df.index.min() or tar_idx > trade_days_df.index.max():
        raise ValueError("{} target trade date is unknown!")

    tar_row = trade_days_df.iloc[tar_idx]
    tar_d = tar_row["trade_date"]

    return tar_d


def argmin(a, default=-1):
    """
    当 a 为空时, 返回 default, 其他同 numpy.argmin
    Args:
        a:
        default:

    Returns:

    """
    if len(a) > 0:
        return np.argmin(a)
    else:
        return default


def argmax(a, default=-1):
    """
    当 a 为空时, 返回 default, 其他同 numpy.argmin
    Args:
        a:
        default:

    Returns:

    """
    if len(a) > 0:
        return np.argmax(a)
    else:
        return default


def argvalmin(a):
    """
    同时返回索引和值，其他同 numpy.argmin
    Args:
        a:
    Returns:

    """
    idx = np.argmin(a)
    return idx, a[idx]


def argvalmax(a):
    """
    同时返回索引和值，其他同 numpy.argmin
    Args:
        a:
    Returns:

    """
    idx = np.argmax(a)
    return idx, a[idx]



def yearly_profit_rate(start_date: Union[str, datetime], end_date: Union[str, datetime], capital_before: float, capital_after: float, D: int = 250) -> float:
    """
    计算年化收益率 Y
    Y = (V/C)^N - 1
    Y = (V/C)^(D/T) - 1
    其中N=D/T表示投资人一年内重复投资的次数。D 表示一年的有效投资时间，对银行存款、票据、债券等D=360日，对于股票、期货等市场D=250日，对于房地产和实业等D=365日。
    Args:
        start_date:
        end_date:
        capital_before:
        capital_after:
        D:

    Returns:

    """
    start_date = datetime2str(start_date)
    end_date = datetime2str(end_date)

    tds = trade_days_df.loc[(trade_days_df["trade_date"] < end_date) & (trade_days_df["trade_date"] > start_date)]
    T = len(tds)
    N = D / T
    Y = (capital_after/capital_before)**N - 1
    return Y


if __name__ == '__main__':
    # r = trade_date_calc("2020-01-03", 1)
    # print(r)

    r = yearly_profit_rate(datetime(2020, 1, 1), datetime(2020, 2, 1), 10000, 11000)
    print(r)

