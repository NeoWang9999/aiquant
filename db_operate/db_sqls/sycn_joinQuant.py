#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: sycn_jq.py
@time: 2021/1/27 17:11
@desc: 数据详情参考 https://www.joinquant.com/help/api/help#JQData
"""
import json
from datetime import datetime

import jqdatasdk as jqd

from config.config import JQ_USER, JQ_PASSWD
from db_operate.db_logger.logger import logger
from db_operate.db_sqls.utils import pg_insert_on_conflict_batch, pg_execute
from db_operate.name_space import JQNameSpace
from db_operate.utils.data_tools import SQLEncoder
from db_operate.utils.time import time_cost

# 初始化全局变量

TODAY = datetime.now().strftime("%Y-%m-%d")
ZERO_DATE = "1900-01-01"
jqd.auth(JQ_USER, JQ_PASSWD)


@time_cost(logger.info)
def securities():
    logger.info("开始获取 securities 数据...")
    conflict_cols = ['code']
    update_cols = ['display_name', 'name', 'start_date', 'end_date', 'type']

    df = jqd.get_all_securities(
        types=['stock', 'fund', 'index', 'futures', 'options', 'etf', 'lof', 'fja', 'fjb', 'open_fund', 'bond_fund',
               'stock_fund', 'QDII_fund', 'money_market_fund', 'mixture_fund'], date=None)
    df = df.where(df.notnull(), None)
    df.reset_index(inplace=True)
    df.rename(columns={"index": "code"}, inplace=True)
    df = df.astype({"start_date": str, "end_date": str})

    cols = df.columns.tolist()
    lines = df.to_records(index=False).tolist()

    logger.info("开始插入数据库...")
    pg_insert_on_conflict_batch(table=JQNameSpace.full_table_name("securities"),
                                cols=cols, lines=lines, conflict_cols=conflict_cols, update_cols=update_cols)

    logger.info("securities 完成更新，获取数据：{}".format(len(lines)))


@time_cost(logger.info)
def index_stocks():
    logger.info("开始获取 index_stocks 数据...")
    # match 示例： select index_code, jsonb_array_elements(stocks)->'display_name' from jq_data.index_stocks is2 where index_code in ('000001.XSHG')

    cols = ['index_code', 'display_name', 'name', 'start_date', 'end_date', 'stocks']
    conflict_cols = ['index_code']
    update_cols = ['display_name', 'name', 'start_date', 'end_date', 'stocks']
    fetch_command = """
        SELECT 
            code, display_name, name, start_date, end_date
        FROM
            {table_name}
        where
            type = 'index'
        ;
 """.format(table_name=JQNameSpace.full_table_name("securities"))
    fetch_list_temp = """
        SELECT 
            *
        FROM
            {table_name}
        where
            code in {tar_codes}
        ;
    """

    db_return = pg_execute(command=fetch_command, returning=True)

    lines = []
    for index_item in db_return:
        line = [
            index_item["code"],  # code
            index_item["display_name"],  # display_name
            index_item["name"],  # name
            index_item["start_date"],  # start_date
            index_item["end_date"],  # end_date
            None,  # stocks
        ]
        stock_list = jqd.get_index_stocks(index_symbol=index_item["code"], date=TODAY)
        logger.info("{}, {} 成分股数量: {}".format(line[0], line[1], len(stock_list)))
        if not stock_list:
            continue
        command = fetch_list_temp.format(table_name=JQNameSpace.full_table_name("securities"),
                                         tar_codes=tuple(stock_list))
        db_return = pg_execute(command=command, returning=True)
        line[-1] = json.dumps(db_return, ensure_ascii=False, cls=SQLEncoder)
        lines.append(line)

    logger.info("开始插入数据库...")
    pg_insert_on_conflict_batch(table=JQNameSpace.full_table_name("index_stocks"),
                                cols=cols, lines=lines, conflict_cols=conflict_cols, update_cols=update_cols)

    logger.info("index_stocks 完成更新，获取数据：{}".format(len(lines)))


def index_daily():
    fields = ['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit', 'low_limit', 'avg',
              'pre_close', 'paused']
    r = jqd.get_price("000001.XSHG", start_date=ZERO_DATE, end_date=TODAY, frequency='daily', fields=fields,
                      skip_paused=False, fq='pre')
    logger(r)


if __name__ == '__main__':
    # securities()
    index_stocks()
    # index_daily()
