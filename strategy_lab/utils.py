#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: utils.py
@time: 2021/1/22 17:32
"""
from datetime import datetime, timedelta

import numpy as np


def first_true(iterable, default=False, pred=None):
    """Returns the first true value in the iterable.

    If no true value is found, returns *default*

    If *pred* is not None, returns the first item
    for which pred(item) is true.

    """
    # first_true([a,b,c], x) --> a or b or c or x
    # first_true([a,b], x, f) --> a if f(a) else b if f(b) else x
    return next(filter(pred, iterable), default)


def date_calc(date: [str, datetime], add_days: int, fmt: str = "%Y-%m-%d"):
    """
    Args:
        date:
        add_days:
        fmt:

    Returns:
        date: string
    """
    if isinstance(date, str):
        d = datetime.strptime(date, fmt)
    elif isinstance(date, datetime):
        d = date
    else:
        raise TypeError("Unsupport type for date: {}".format(type(date)))
    d += timedelta(days=add_days)
    return d.strftime(fmt)


def trade_date_calc(date: [str, datetime], add_days: int, fmt: str = "%Y-%m-%d"):
    """
    Args:
        date:
        add_days:
        fmt:

    Returns:
        date: string
    """
    if isinstance(date, str):
        d = date
    elif isinstance(date, datetime):
        d = date.strftime(fmt)
    else:
        raise TypeError("Unsupport type for date: {}".format(type(date)))

    from db_operate.db_sqls.pg_sqls import JQQuerySQL
    from db_operate.db_sqls.utils import pg_execute
    import pandas as pd

    s_ret = pg_execute(JQQuerySQL.all_trade_days, returning=True)
    df = pd.DataFrame(s_ret)
    df.sort_values(by="trade_date", inplace=True)
    d_row = df.loc[df["trade_date"] == d]
    if d_row.empty:
        raise ValueError("{} is not a known trade date!".format(d))
    tar_idx = d_row.index[0] + add_days
    if tar_idx < df.index.min() or tar_idx > df.index.max():
        raise ValueError("{} target trade date is unknown!")

    tar_row = df.iloc[tar_idx]
    tar_d = tar_row["trade_date"]

    return tar_d


def argmin(a, default=-1):
    """
    当 a 为空时，返回 default
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
    当 a 为空时，返回 default
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
    当 a 为空时，返回 default
    Args:
        a:
        default:

    Returns:

    """
    idx = np.argmin(a)
    return idx, a[idx]


def argvalmax(a):
    """
    当 a 为空时，返回 default
    Args:
        a:
        default:

    Returns:

    """
    idx = np.argmax(a)
    return idx, a[idx]


if __name__ == '__main__':
    trade_date_calc("2020-01-06", add_days=1)
