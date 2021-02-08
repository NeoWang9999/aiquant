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
from jqdatasdk import finance
from sqlalchemy import desc

from config.config import JQ_USER, JQ_PASSWD
from db_operate.db_logger.logger import logger
from db_operate.db_sqls.pg_sqls import JQQuerySQL
from db_operate.db_sqls.utils import pg_insert_on_conflict_batch, pg_execute
from db_operate.name_space import JQNameSpace
from db_operate.utils.data_tools import SQLEncoder
from db_operate.utils.time import time_cost

# 初始化全局变量
DATE_FMT = "%Y-%m-%d"
TODAY = datetime.now().strftime(DATE_FMT)
ZERO_DATE = "1900-01-01"
jqd.auth(JQ_USER, JQ_PASSWD)


@time_cost(logger.info)
def all_trade_days():
    logger.info("开始获取 all_trade_days 数据...")
    cols = ["trade_date", "pre_trade_date", "next_trade_date"]
    conflict_cols = ['trade_date']
    update_cols = ["pre_trade_date", "next_trade_date"]

    a = [d.strftime(DATE_FMT) for d in jqd.get_all_trade_days()]
    trade_date = a[1:-1]
    pre_trade_date = a[:-2]
    next_trade_date = a[2:]

    lines = [(td, ptd, ntd) for td, ptd, ntd in zip(trade_date, pre_trade_date, next_trade_date)]

    logger.info("开始更新数据库...")
    pg_insert_on_conflict_batch(table=JQNameSpace.full_table_name("all_trade_days"),
                                cols=cols, lines=lines, conflict_cols=conflict_cols, update_cols=update_cols)

    logger.info("all_trade_days 完成更新，获取数据：{}".format(len(lines)))


@time_cost(logger.info)
def securities():
    logger.info("开始获取 securities 数据...")
    cols = ["code", 'display_name', 'name', 'start_date', 'end_date', 'type']
    conflict_cols = ['code']
    update_cols = ['display_name', 'name', 'start_date', 'end_date', 'type']

    df = jqd.get_all_securities(
        types=['stock', 'fund', 'index', 'futures', 'options', 'etf', 'lof', 'fja', 'fjb', 'open_fund', 'bond_fund',
               'stock_fund', 'QDII_fund', 'money_market_fund', 'mixture_fund'], date=None)
    df = df.where(df.notnull(), None)
    df.reset_index(inplace=True)
    df.rename(columns={"index": "code"}, inplace=True)
    df = df.astype({"start_date": str, "end_date": str})
    df = df[cols]

    lines = df.to_records(index=False).tolist()

    logger.info("开始更新数据库...")
    pg_insert_on_conflict_batch(table=JQNameSpace.full_table_name("securities"),
                                cols=cols, lines=lines, conflict_cols=conflict_cols, update_cols=update_cols)

    logger.info("securities 完成更新，获取数据：{}".format(len(lines)))


@time_cost(logger.info)
def index_stocks():
    logger.info("开始获取 index_stocks 数据...")
    # match 示例： select index_code, jsonb_array_elements(stocks)->'display_name' from jq_data.index_stocks is2 where index_code in ('000001.XSHG')

    cols = ['code', 'display_name', 'name', 'start_date', 'end_date', 'stocks']
    conflict_cols = ['code']
    update_cols = ['display_name', 'name', 'start_date', 'end_date', 'stocks']

    fetch_list_temp = """
        SELECT 
            *
        FROM
            {table_name}
        where
            code in {tar_codes}
        ;
    """
    db_return = pg_execute(command=JQQuerySQL.securities__w_type, returning=True)

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

    logger.info("开始更新数据库...")
    pg_insert_on_conflict_batch(table=JQNameSpace.full_table_name("index_stocks"),
                                cols=cols, lines=lines, conflict_cols=conflict_cols, update_cols=update_cols)

    logger.info("index_stocks 完成更新，获取数据：{}".format(len(lines)))


@time_cost(logger.info)
def index_daily():
    logger.info("开始获取 index_daily 数据...")
    cols = ['code', 'date', 'open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit', 'low_limit',
            'avg', 'pre_close', 'paused', 'open_interest']
    conflict_cols = ['code', 'date']
    update_cols = ['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit', 'low_limit', 'avg',
                   'pre_close', 'paused', 'open_interest']
    fields = update_cols

    total_lines_count = 0
    s_return = pg_execute(command=JQQuerySQL.securities__w_type, returning=True)
    for index_item in s_return:
        # 初始化模式，如果库里有则跳过，不更新
        id_return = pg_execute(command=JQQuerySQL.index_daily__i_code.format(code=index_item["code"]), returning=True)
        if id_return:
            logger.info("[已存在] {} 的行情 ...".format(index_item["code"]))
            continue

        df = jqd.get_price(index_item["code"], start_date=ZERO_DATE, end_date=TODAY, frequency='daily', fields=fields,
                           skip_paused=False, fq='pre')
        if df.empty:
            continue
        df.dropna(how="all", inplace=True)
        df = df.where(df.notnull(), None)
        df.reset_index(inplace=True)
        df.rename(columns={"index": "date"}, inplace=True)
        df = df.astype({"paused": int, "date": str})
        df = df.astype({"paused": bool})
        df["code"] = index_item["code"]
        df = df[cols]

        lines = df.to_records(index=False).tolist()
        total_lines_count += len(lines)

        logger.info("[开始更新] {} 的行情，共计 {} 条 ...".format(index_item["code"], len(lines)))
        pg_insert_on_conflict_batch(table=JQNameSpace.full_table_name("index_daily"),
                                    cols=cols, lines=lines, conflict_cols=conflict_cols, update_cols=update_cols)
        logger.info("[完成更新] {} 的行情，共计 {} 条 ...".format(index_item["code"], len(lines)))

    logger.info("index_daily 完成更新，获取数据：{}".format(total_lines_count))


@time_cost(logger.info)
def moneyflow_hsgt():
    logger.info("开始获取 moneyflow_hsgt 数据...")
    cols = ["date", "link_id", "link_name", "currency_id", "currency_name", "net_buy", "net_flow", "buy_amount", "buy_volume", "sell_amount", "sell_volume", "sum_amount", "sum_volume", "quota", "quota_balance", "quota_daily", "quota_daily_balance"]
    conflict_cols = ["date", "link_id"]
    update_cols = ["link_name", "currency_id", "currency_name", "net_buy", "net_flow", "buy_amount", "buy_volume", "sell_amount", "sell_volume", "sum_amount", "sum_volume", "quota", "quota_balance", "quota_daily", "quota_daily_balance"]

    # 一次获取 2000 条，太多返回结果会乱
    batch_size = 2000
    q = jqd.query(finance.STK_ML_QUOTA).filter(finance.STK_ML_QUOTA.day < TODAY).order_by(desc(finance.STK_ML_QUOTA.day)).limit(batch_size)
    df = finance.run_query(q)
    while not df.empty:
        df.dropna(how="all", inplace=True)
        df.rename(columns={"day": "date"}, inplace=True)
        df["net_buy"] = df.apply(lambda x: x["buy_amount"] - x["sell_amount"], axis=1)
        df["net_flow"] = df.apply(lambda x: x["quota_daily"] - x["quota_daily_balance"], axis=1)
        df = df[cols]

        date_list = sorted(df["date"].tolist())
        oldest = date_list[0]
        latest = date_list[-1]
        # 计算北向资金（沪股通 + 深股通）
        north_df = df.loc[(df["link_id"] == 310001) | (df["link_id"] == 310002)].groupby(by="date").sum()
        north_df.reset_index(inplace=True)
        north_df.rename(columns={"index": "date"}, inplace=True)
        north_df["link_id"] = 310005
        north_df["link_name"] = "北向资金"
        north_df["currency_id"] = 110001
        north_df["currency_name"] = "人民币"
        north_df["quota"] = None
        north_df["quota_balance"] = None
        df = df.append(north_df, ignore_index=True)

        # 计算南向资金（港股通(沪) + 港股通(深)）
        south_df = df.loc[(df["link_id"] == 310003) | (df["link_id"] == 310004)].groupby(by="date").sum()
        south_df.reset_index(inplace=True)
        south_df.rename(columns={"index": "date"}, inplace=True)
        south_df["link_id"] = 310006
        south_df["link_name"] = "南向资金"
        south_df["currency_id"] = 110003
        south_df["currency_name"] = "港元"
        south_df["quota"] = None
        south_df["quota_balance"] = None
        df = df.append(south_df, ignore_index=True)

        lines = df.to_records(index=False).tolist()

        logger.info("[开始更新] moneyflow_hsgt，{} 到 {}，共计 {} 条 ...".format(oldest, latest, len(lines)))
        pg_insert_on_conflict_batch(table=JQNameSpace.full_table_name("moneyflow_hsgt"),
                                    cols=cols, lines=lines, conflict_cols=conflict_cols, update_cols=update_cols)
        logger.info("[完成更新] moneyflow_hsgt，{} 到 {}，共计 {} 条 ...".format(oldest, latest, len(lines)))

        q = jqd.query(finance.STK_ML_QUOTA).filter(finance.STK_ML_QUOTA.day < oldest).order_by(desc(finance.STK_ML_QUOTA.day)).limit(batch_size)
        df = finance.run_query(q)


if __name__ == '__main__':
    all_trade_days()
    # securities()
    # index_stocks()
    # index_daily()
    # moneyflow_hsgt()
