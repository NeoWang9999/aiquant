#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: sycn_jq.py
@time: 2021/1/27 17:11
@desc: 数据详情参考 https://www.akshare.xyz/zh_CN/latest/introduction.html
"""
import json
from datetime import datetime

import akshare as ak

from db_operate.db_logger.logger import logger
from db_operate.utils.time import time_cost

# 初始化全局变量
TODAY = datetime.now().strftime("%Y-%m-%d")
ZERO_DATE = "1900-01-01"

@time_cost(logger.info)
def moneyflow_hsgt():
    logger.info("开始获取 moneyflow_hsgt 数据...")
    fields = ["沪股通", "深股通", "港股通沪", "港股通深"]

    df = ak.stock_em_hsgt_hist(symbol="沪股通")



if __name__ == '__main__':
    moneyflow_hsgt()
