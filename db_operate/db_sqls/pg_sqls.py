#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: pg_sqls.py
@time: 2021/1/22 22:28
"""
from db_operate.name_space import JQNameSpace, TSNameSpace

class TSTableCreateSQL(object):
    """
    建表语句
    """

    schema = """
    CREATE SCHEMA {schema_name};
    """.format(schema_name=TSNameSpace.schema)

    trade_cal = """
        CREATE TABLE {schema_name}.{table_name} (
            cal_date varchar(128),
            exchange varchar(128),
            is_open smallint,
            create_at timestamp not null default now(),
            update_at timestamp not null default now(),
        PRIMARY KEY (cal_date)
        );
        comment on TABLE {schema_name}.{table_name} is '各大交易所交易日历数据';
        comment on column {schema_name}.{table_name}.cal_date is '日历日期';
        comment on column {schema_name}.{table_name}.exchange is '交易所：SSE 上交所, SZSE 深交所, CFFEX 中金所, SHFE 上期所, CZCE 郑商所, DCE 大商所, INE 上能源';
        comment on column {schema_name}.{table_name}.is_open is '是否交易。 0-休市，1-交易';
    """.format(schema_name=TSNameSpace.schema, table_name=TSNameSpace.trade_cal)

    stock_basic = """
        CREATE TABLE {schema_name}.{table_name} (
            ts_code varchar(128),
            symbol varchar(128),
            name varchar(128),
            area varchar(128),
            industry varchar(128),
            fullname varchar(128),
            enname varchar(128),
            market varchar(128),
            exchange varchar(128),
            curr_type varchar(128),
            list_status varchar(128),
            list_date varchar(128),
            delist_date varchar(128),
            is_hs varchar(128),
            create_at timestamp not null default now(),
            update_at timestamp not null default now(),
        PRIMARY KEY (ts_code)
        );
        CREATE INDEX ix_{schema_name}_{table_name}_ts_code on {schema_name}.{table_name} (ts_code); 
        
        comment on TABLE {schema_name}.{table_name} is '基础信息数据表，包括股票代码、名称、上市日期、退市日期等';
        comment on column {schema_name}.{table_name}.ts_code is 'TS代码';
        comment on column {schema_name}.{table_name}.symbol is '股票代码';
        comment on column {schema_name}.{table_name}.name is '股票名称';
        comment on column {schema_name}.{table_name}.area is '所在地域';
        comment on column {schema_name}.{table_name}.industry is '所属行业';
        comment on column {schema_name}.{table_name}.fullname is '股票全称';
        comment on column {schema_name}.{table_name}.enname is '英文全称';
        comment on column {schema_name}.{table_name}.market is '市场类型 （主板/中小板/创业板/科创板/CDR）';
        comment on column {schema_name}.{table_name}.exchange is '交易所代码';
        comment on column {schema_name}.{table_name}.curr_type is '交易货币';
        comment on column {schema_name}.{table_name}.list_status is '上市状态： L上市 D退市 P暂停上市';
        comment on column {schema_name}.{table_name}.list_date is '上市日期';
        comment on column {schema_name}.{table_name}.delist_date is '退市日期';
        comment on column {schema_name}.{table_name}.is_hs is '是否沪深港通标的，N否 H沪股通 S深股通';
    """.format(schema_name=TSNameSpace.schema, table_name=TSNameSpace.stock_basic)

    moneyflow_hsgt = """
        CREATE TABLE {schema_name}.{table_name} (
            trade_date	varchar(128),
            ggt_ss	numeric(16, 2),
            ggt_sz	numeric(16, 2),
            hgt	numeric(16, 2),
            sgt	numeric(16, 2),
            north_money	numeric(16, 2),
            south_money	numeric(16, 2),
            create_at timestamp not null default now(),
            update_at timestamp not null default now(),
        PRIMARY KEY (trade_date)
        );
        CREATE INDEX ix_{schema_name}_{table_name}_trade_date on {schema_name}.{table_name} (trade_date); 
        
        comment on TABLE {schema_name}.{table_name} is '港深股通资金流向';
        comment on column {schema_name}.{table_name}.trade_date is '交易日期';
        comment on column {schema_name}.{table_name}.ggt_ss is '港股通（上海）';
        comment on column {schema_name}.{table_name}.ggt_sz is '港股通（深圳）';
        comment on column {schema_name}.{table_name}.hgt is '沪股通（百万元）';
        comment on column {schema_name}.{table_name}.sgt is '深股通（百万元）';
        comment on column {schema_name}.{table_name}.north_money is '北向资金（百万元）';
        comment on column {schema_name}.{table_name}.south_money is '南向资金（百万元）';
    """.format(schema_name=TSNameSpace.schema, table_name=TSNameSpace.moneyflow_hsgt)

    index_basic = """
        CREATE TABLE {schema_name}.{table_name} (
            ts_code varchar(128),
            name varchar(128),
            fullname varchar(128),
            market varchar(128),
            publisher varchar(128),
            index_type varchar(128),
            category varchar(128),
            base_date varchar(128),
            base_point numeric(10, 2),
            list_date varchar(128),
            weight_rule varchar(128),
            description varchar(2048),
            exp_date varchar(128),
            create_at timestamp not null default now(),
            update_at timestamp not null default now(),
        PRIMARY KEY (ts_code)
        );
        CREATE INDEX INDEX ix_{schema_name}_{table_name}_ts_code on {schema_name}.{table_name} (ts_code); 

        comment on TABLE {schema_name}.{table_name} is \
            '''
            指数基础信息表
            指数列表：[主题指数，规模指数，策略指数，风格指数，综合指数，成长指数，价值指数，有色指数，化工指数，能源指数，其他指数，外汇指数，基金指数，商品指数，债券指数，行业指数，贵金属指数，农副产品指数，软商品指数，油脂油料指数，非金属建材指数，煤焦钢矿指数，谷物指数]
            ''';
        comment on column {schema_name}.{table_name}.ts_code is 'TS代码';
        comment on column {schema_name}.{table_name}.name is '简称';
        comment on column {schema_name}.{table_name}.fullname is '指数全称';
        comment on column {schema_name}.{table_name}.market is '市场；市场代码: 说明 \nMSCI: MSCI指数 \nCSI: 中证指数 \nSSE: 上交所指数 \nSZSE: 深交所指数 \nCICC: 中金指数 \nSW: 申万指数 \nOTH: 其他指数';
        comment on column {schema_name}.{table_name}.publisher is '发布方';
        comment on column {schema_name}.{table_name}.index_type is '指数风格';
        comment on column {schema_name}.{table_name}.category is '指数类别';
        comment on column {schema_name}.{table_name}.base_date is '基期';
        comment on column {schema_name}.{table_name}.base_point is '基点';
        comment on column {schema_name}.{table_name}.list_date is '发布日期';
        comment on column {schema_name}.{table_name}.weight_rule is '加权方式';
        comment on column {schema_name}.{table_name}.description is '描述';
        comment on column {schema_name}.{table_name}.exp_date is '终止日期';
    """.format(schema_name=TSNameSpace.schema, table_name=TSNameSpace.moneyflow_hsgt)



class JQTableCreateSQL:
    schema = """
    CREATE SCHEMA {schema_name};
    """.format(schema_name=JQNameSpace.schema)

    securities = """
        CREATE TABLE {schema_name}.{table_name} (
            code varchar(128),
            display_name varchar(128),
            name varchar(128),
            start_date varchar(128),
            end_date varchar(128),
            type varchar(128),
            created_at timestamp not null default now(),
            updated_at timestamp not null default now(),
            deleted_at timestamp default null ,
        PRIMARY KEY (code)
        );
        CREATE INDEX ix_{schema_name}_{table_name}_code on {schema_name}.{table_name} (code); 

        comment on TABLE {schema_name}.{table_name} is \
            '
            获取所有标的信息
            
            交易市场	代码后缀	示例代码	证券简称
            上海证券交易所	.XSHG	600519.XSHG	贵州茅台
            深圳证券交易所	.XSHE	000001.XSHE	平安银行
            中金所	.CCFX	IC9999.CCFX	中证500主力合约
            大商所	.XDCE	A9999.XDCE	豆一主力合约
            上期所	.XSGE	AU9999.XSGE	黄金主力合约
            郑商所	.XZCE	CY8888.XZCE	棉纱期货指数
            上海国际能源期货交易所	.XINE	SC9999.XINE	原油主力合约
            ';
        comment on column {schema_name}.{table_name}.code is ' 证券代码';
        comment on column {schema_name}.{table_name}.display_name is ' 中文名称';
        comment on column {schema_name}.{table_name}.name is ' 缩写简称';
        comment on column {schema_name}.{table_name}.start_date is ' 上市日期';
        comment on column {schema_name}.{table_name}.end_date is ' 退市日期，如果没有退市则为2200-01-01';
        comment on column {schema_name}.{table_name}.type is ' 类型 : stock(股票)，index(指数)，etf(ETF基金)，fja（分级A），fjb（分级B），fjm（分级母基金），mmf（场内交易的货币基金）, open_fund（开放式基金）, bond_fund（债券基金）, stock_fund（股票型基金） , QDII_fund（QDII 基金）, money_market_fund（场外交易的货币基金）, mixture_fund（混合型基金）, options(期权)';
    """.format(schema_name=JQNameSpace.schema, table_name=JQNameSpace.securities)

    index_stocks = """
        CREATE TABLE {schema_name}.index_stocks (
            index_code varchar(128),
            display_name varchar(128),
            name varchar(128),
            start_date varchar(128),
            end_date varchar(128),
            stocks jsonb,
            created_at timestamp not null default now(),
            updated_at timestamp not null default now(),
            deleted_at timestamp default null ,
        PRIMARY KEY (index_code)
        );
        CREATE INDEX ix_{schema_name}_{table_name}_index_code on {schema_name}.index_stocks (index_code); 

        comment on TABLE {schema_name}.{table_name} is '指数给定日期在平台可交易的成分股列表';
        comment on column {schema_name}.{table_name}.index_code is ' 指数代码';
        comment on column {schema_name}.{table_name}.display_name is ' 中文名称';
        comment on column {schema_name}.{table_name}.name is ' 缩写简称';
        comment on column {schema_name}.{table_name}.start_date is ' 上市日期';
        comment on column {schema_name}.{table_name}.end_date is ' 退市日期，如果没有退市则为2200-01-01';
        comment on column {schema_name}.{table_name}.stocks is '成分股列表及成分股详情';
    """.format(schema_name=JQNameSpace.schema, table_name=JQNameSpace.index_stocks)

