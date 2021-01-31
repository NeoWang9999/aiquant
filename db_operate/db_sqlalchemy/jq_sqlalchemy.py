#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: pg_sqlalchemy.py
@time: 2021/1/28 21:04
"""
import datetime

import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, DateTime, String, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from config.config import DBConfig
from db_operate.name_space import JQNameSpace

engine = create_engine(DBConfig.uri,
                       echo=True,
                       pool_size=DBConfig.pool_size,
                       max_overflow=DBConfig.max_overflow,
                       pool_recycle=DBConfig.pool_recycle
                       )

jq_metadata = MetaData(schema=JQNameSpace.schema)
JQ_Session = sessionmaker(bind=engine)
JQ_Base = declarative_base(engine, metadata=jq_metadata, name="JQ_Base")


class CommonColsMixin:
    """ mixin 公用字段 """
    created_at = Column(DateTime, nullable=False, default=datetime.datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.datetime.now, onupdate=datetime.datetime.now, index=True)
    deleted_at = Column(DateTime)  # 可以为空, 如果非空, 则为软删


class Security(JQ_Base, CommonColsMixin):
    __tablename__ = 'securities'
    __table_args__ = {
        "comment": """
                获取所有标的信息
        
        交易市场	代码后缀	示例代码	证券简称
        上海证券交易所	.XSHG	600519.XSHG	贵州茅台
        深圳证券交易所	.XSHE	000001.XSHE	平安银行
        中金所	.CCFX	IC9999.CCFX	中证500主力合约
        大商所	.XDCE	A9999.XDCE	豆一主力合约
        上期所	.XSGE	AU9999.XSGE	黄金主力合约
        郑商所	.XZCE	CY8888.XZCE	棉纱期货指数
        上海国际能源期货交易所	.XINE	SC9999.XINE	原油主力合约
        """
    }
    code = Column(String(128), primary_key=True, index=True, comment="证券代码")
    display_name = Column(String(128), comment="中文名称")
    name = Column(String(128), comment="缩写简称")
    start_date = Column(String(128), comment="上市日期")
    end_date = Column(String(128), comment="退市日期，如果没有退市则为2200-01-01")
    type = Column(String(128), comment="类型 : stock(股票)，index(指数)，etf(ETF基金)，fja（分级A），fjb（分级B），fjm（分级母基金），mmf（场内交易的货币基金）, open_fund（开放式基金）, bond_fund（债券基金）, stock_fund（股票型基金） , QDII_fund（QDII 基金）, money_market_fund（场外交易的货币基金）, mixture_fund（混合型基金）, options(期权)")
