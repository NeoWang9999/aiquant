#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: time.py
@time: 2021/1/30 17:57
"""
import functools
import time


def time_cost(print_func=print):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            s = time.time()
            func(*args, **kwargs)
            cost = (time.time() - s) * 1000
            print_func('{func_name} cost time: {time_cost} s'.format(func_name=func.__name__, time_cost=round(cost / 1000, 3)))
            return
        return wrapper
    return decorator
