#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: pg_sqls.py
@time: 2021/1/22 22:28
"""
from db_operate.name_space import JQNameSpace, TSNameSpace, AKNameSpace


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
            created_at timestamp not null default now(),
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
            created_at timestamp not null default now(),
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
            created_at timestamp not null default now(),
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
            created_at timestamp not null default now(),
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


    all_trade_days = """
        CREATE TABLE {schema_name}.{table_name} (
            trade_date varchar(128),
            pre_trade_date varchar(128),
            next_trade_date varchar(128),
            created_at timestamp not null default now(),
            update_at timestamp not null default now(),
            deleted_at timestamp default null ,
        PRIMARY KEY (trade_date)
        );
        comment on TABLE {schema_name}.{table_name} is '所有交易日';
        comment on column {schema_name}.{table_name}.trade_date is '交易日';
        comment on column {schema_name}.{table_name}.pre_trade_date is '前一个交易日';
        comment on column {schema_name}.{table_name}.next_trade_date is '后一个交易日';
    """.format(schema_name=JQNameSpace.schema, table_name=JQNameSpace.all_trade_days)


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
        CREATE TABLE {schema_name}.{table_name} (
            code varchar(128),
            display_name varchar(128),
            name varchar(128),
            start_date varchar(128),
            end_date varchar(128),
            stocks jsonb,
            created_at timestamp not null default now(),
            updated_at timestamp not null default now(),
            deleted_at timestamp default null ,
        PRIMARY KEY (code)
        );
        CREATE INDEX ix_{schema_name}_{table_name}_code on {schema_name}.{table_name} (code); 

        comment on TABLE {schema_name}.{table_name} is '指数给定日期在平台可交易的成分股列表';
        comment on column {schema_name}.{table_name}.code is ' 指数代码';
        comment on column {schema_name}.{table_name}.display_name is ' 中文名称';
        comment on column {schema_name}.{table_name}.name is ' 缩写简称';
        comment on column {schema_name}.{table_name}.start_date is ' 上市日期';
        comment on column {schema_name}.{table_name}.end_date is ' 退市日期，如果没有退市则为2200-01-01';
        comment on column {schema_name}.{table_name}.stocks is '成分股列表及成分股详情';
    """.format(schema_name=JQNameSpace.schema, table_name=JQNameSpace.index_stocks)


    index_daily = """
        CREATE TABLE {schema_name}.{table_name} (
            code varchar(128),
            date varchar(128),
            open numeric(20,4),
            close numeric(20,4),
            low numeric(20,4),
            high numeric(20,4),
            volume numeric(20,4),
            money numeric(16, 0),
            factor numeric(8,4),
            high_limit numeric(20,4),
            low_limit numeric(20,4),
            avg numeric(20,4),
            pre_close numeric(20,4),
            paused boolean,
            open_interest numeric(20,4),
            created_at timestamp not null default now(),
            updated_at timestamp not null default now(),
            deleted_at timestamp default null ,
        PRIMARY KEY (code, date)
        );
        CREATE INDEX ix_{schema_name}_{table_name}_code__date on {schema_name}.{table_name} (code, date); 
        
        comment on TABLE {schema_name}.{table_name} is '指数行情数据';
        comment on column {schema_name}.{table_name}.open is '时间段开始时价格';
        comment on column {schema_name}.{table_name}.close is '时间段结束时价格';
        comment on column {schema_name}.{table_name}.low is '时间段中的最低价';
        comment on column {schema_name}.{table_name}.high is '时间段中的最高价';
        comment on column {schema_name}.{table_name}.volume is '时间段中的成交的股票数量';
        comment on column {schema_name}.{table_name}.money is '时间段中的成交的金额';
        comment on column {schema_name}.{table_name}.factor is '前复权因子, 我们提供的价格都是前复权后的, 但是利用这个值可以算出原始价格, 方法是价格除以factor, 比如 close/factor';
        comment on column {schema_name}.{table_name}.high_limit is '时间段中的涨停价';
        comment on column {schema_name}.{table_name}.low_limit is '时间段中的跌停价';
        comment on column {schema_name}.{table_name}.avg is '这段时间的平均价。计算方法（1）天级别：股票是成交额除以成交量；期货是直接从CTP行情获取的，计算方法为成交额除以成交量再除以合约乘数；（2）分钟级别：用该分钟所有tick的现价乘以该tick的成交量加起来之后，再除以该分钟的成交量。';
        comment on column {schema_name}.{table_name}.pre_close is '前一个单位时间结束时的价格, 按天则是前一天的收盘价（期货中pre_close是前前一天结算价，建议使用get_extras获取结算价）, 注意：在分钟频率下pre_close=open；';
        comment on column {schema_name}.{table_name}.paused is 'bool值, 这只股票是否停牌, 停牌时open/close/low/high/pre_close依然有值,都等于停牌前的收盘价, volume=money=0';
        comment on column {schema_name}.{table_name}.open_interest is '期货持仓量';
    """.format(schema_name=JQNameSpace.schema, table_name=JQNameSpace.index_daily)


    moneyflow_hsgt = """
        CREATE TABLE {schema_name}.{table_name} (
            date varchar(128),
            link_id int,
            link_name varchar(32),
            currency_id int,
            currency_name varchar(16),
            net_buy numeric(20, 4),
            net_flow numeric(20, 4),
            buy_amount numeric(20, 4),
            buy_volume numeric(20, 4),
            sell_amount numeric(20, 4),
            sell_volume numeric(20, 4),
            sum_amount numeric(20, 4),
            sum_volume numeric(20, 4),
            quota numeric(20, 4),
            quota_balance numeric(20, 4),
            quota_daily numeric(20, 4),
            quota_daily_balance numeric(20, 4),
            created_at timestamp not null default now(),
            updated_at timestamp not null default now(),
            deleted_at timestamp default null,
        PRIMARY KEY (date, link_id)
        );
        CREATE INDEX ix_{schema_name}_{table_name}_date__link_id on {schema_name}.{table_name} (date, link_id); 
        
        comment on column {schema_name}.{table_name}.date is '交易日期';
        comment on column {schema_name}.{table_name}.link_id is '市场通编码
            市场通编码	市场通名称
            310001	沪股通
            310002	深股通
            310003	港股通（沪）
            310004	港股通（深）
            310005	北向资金
            310006	南向资金
        ';
        comment on column {schema_name}.{table_name}.link_name is '市场通名称。包括以下四个名称： 沪股通，深股通，港股通(沪）,港股通(深）;其中沪股通和深股通属于北向资金，港股通（沪）和港股通（深）属于南向资金。';
        comment on column {schema_name}.{table_name}.currency_id is '货币编码
            货币编码	货币名称
            110001	人民币
            110003	港元
        ';
        comment on column {schema_name}.{table_name}.currency_name is '货币名称';
        comment on column {schema_name}.{table_name}.net_buy is '净买入=买入额-卖出额。单位：亿';
        comment on column {schema_name}.{table_name}.net_flow is '净流入=每日额度-每日额度余额。单位：亿';
        comment on column {schema_name}.{table_name}.buy_amount is '买入额。单位：亿';
        comment on column {schema_name}.{table_name}.buy_volume is '买入数';
        comment on column {schema_name}.{table_name}.sell_amount is '卖出额。单位：亿';
        comment on column {schema_name}.{table_name}.sell_volume is '卖出数';
        comment on column {schema_name}.{table_name}.sum_amount is '累计额。买入额+卖出额。单位：亿';
        comment on column {schema_name}.{table_name}.sum_volume is '累计数目。买入量+卖出量';
        comment on column {schema_name}.{table_name}.quota is '总额度。2016-08-16号起，沪港通和深港通不再设总额度限制';
        comment on column {schema_name}.{table_name}.quota_balance is '总额度余额';
        comment on column {schema_name}.{table_name}.quota_daily is '每日额度';
        comment on column {schema_name}.{table_name}.quota_daily_balance is '每日额度余额。单位：亿';

    """.format(schema_name=JQNameSpace.schema, table_name=JQNameSpace.moneyflow_hsgt)

    fund_daily = """
        CREATE TABLE {schema_name}.{table_name} (
            code varchar(128),
            date varchar(128),
            type varchar(128),
            open numeric(20,4),
            close numeric(20,4),
            low numeric(20,4),
            high numeric(20,4),
            volume numeric(20,4),
            money numeric(16, 0),
            factor numeric(8,4),
            high_limit numeric(20,4),
            low_limit numeric(20,4),
            avg numeric(20,4),
            pre_close numeric(20,4),
            paused boolean,
            open_interest numeric(20,4),
            created_at timestamp not null default now(),
            updated_at timestamp not null default now(),
            deleted_at timestamp default null,
        PRIMARY KEY (code, date)
        );
        CREATE INDEX ix_{schema_name}_{table_name}_code__date on {schema_name}.{table_name} (code, date); 

        comment on TABLE {schema_name}.{table_name} is '基金行情数据';
        comment on column {schema_name}.{table_name}.type is '基金类型: etf(ETF基金), fja(分级A), fjb(分级B), fjm(分级母基金), mmf(场内交易的货币基金), open_fund(开放式基金), bond_fund(债券基金), stock_fund(股票型基金) , QDII_fund(QDII 基金), money_market_fund(场外交易的货币基金), mixture_fund(混合型基金)';
        comment on column {schema_name}.{table_name}.open is '时间段开始时价格';
        comment on column {schema_name}.{table_name}.close is '时间段结束时价格';
        comment on column {schema_name}.{table_name}.low is '时间段中的最低价';
        comment on column {schema_name}.{table_name}.high is '时间段中的最高价';
        comment on column {schema_name}.{table_name}.volume is '时间段中的成交的股票数量';
        comment on column {schema_name}.{table_name}.money is '时间段中的成交的金额';
        comment on column {schema_name}.{table_name}.factor is '前复权因子, 我们提供的价格都是前复权后的, 但是利用这个值可以算出原始价格, 方法是价格除以factor, 比如 close/factor';
        comment on column {schema_name}.{table_name}.high_limit is '时间段中的涨停价';
        comment on column {schema_name}.{table_name}.low_limit is '时间段中的跌停价';
        comment on column {schema_name}.{table_name}.avg is '这段时间的平均价。计算方法（1）天级别：股票是成交额除以成交量；期货是直接从CTP行情获取的，计算方法为成交额除以成交量再除以合约乘数；（2）分钟级别：用该分钟所有tick的现价乘以该tick的成交量加起来之后，再除以该分钟的成交量。';
        comment on column {schema_name}.{table_name}.pre_close is '前一个单位时间结束时的价格, 按天则是前一天的收盘价（期货中pre_close是前前一天结算价，建议使用get_extras获取结算价）, 注意：在分钟频率下pre_close=open；';
        comment on column {schema_name}.{table_name}.paused is 'bool值, 这只股票是否停牌, 停牌时open/close/low/high/pre_close依然有值,都等于停牌前的收盘价, volume=money=0';
        comment on column {schema_name}.{table_name}.open_interest is '期货持仓量';
    """.format(schema_name=JQNameSpace.schema, table_name=JQNameSpace.fund_daily)


class JQQuerySQL:
    """
    {table_name}__{[w-where__column, i-input__column, r-return__column]}
    """
    all_trade_days = """
        SELECT 
            *
        FROM
            {table_name}
        ;
    """.format(table_name=JQNameSpace.full_table_name("all_trade_days"))

    securities__w_type_index = """
        SELECT 
            *
        FROM
            {table_name}
        where
            type = 'index'
        ;
    """.format(table_name=JQNameSpace.full_table_name("securities"))

    securities__w_type_funds = """
        SELECT 
            *
        FROM
            {table_name}
        where
            type in ('etf')
        ;
    """.format(table_name=JQNameSpace.full_table_name("securities"))

    fund_daily__i_code = """
        SELECT 
            *
        FROM
            {table_name}
        where
            code = '{{code}}'
        ;
    """.format(table_name=JQNameSpace.full_table_name("fund_daily"))

    index_daily__i_code = """
        SELECT 
            *
        FROM
            {table_name}
        where
            code = '{{code}}'
        ;
    """.format(table_name=JQNameSpace.full_table_name("index_daily"))

    index_daily__i_code__r_date__close = """
        SELECT 
            date, close
        FROM
            {table_name}
        where
            code = '{{code}}'
        ;
    """.format(table_name=JQNameSpace.full_table_name("index_daily"))

    index_daily__i_code__r_date__open__close = """
        SELECT 
            date, open, close
        FROM
            {table_name}
        where
            code = '{{code}}'
        ;
    """.format(table_name=JQNameSpace.full_table_name("index_daily"))

    moneyflow_hsgt__i_link_id = """
        SELECT 
            *
        FROM
            {table_name}
        where
            link_id = '{{link_id}}'
        ;
    """.format(table_name=JQNameSpace.full_table_name("moneyflow_hsgt"))

    moneyflow_hsgt__i_link_id__r_date__net_buy__net_flow = """
        SELECT 
            date, net_buy, net_flow
        FROM
            {table_name}
        where
            link_id = '{{link_id}}'
        ;
    """.format(table_name=JQNameSpace.full_table_name("moneyflow_hsgt"))


class AKTableCreateSQL:
    schema = """
    CREATE SCHEMA {schema_name};
    """.format(schema_name=AKNameSpace.schema)
