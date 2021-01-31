#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: config.py
@time: 2021/1/22 19:18
"""

TS_TOKEN = 'e6e6c3b909684abed9685243a38aa48d76e4f3a223f7e67c63243558'

JQ_USER = "15321354010"
JQ_PASSWD = "Neowang1995"


class DBConfig:
    host = "localhost"
    port = "5432"
    dbname = "aiquant"
    user = "aiquant"
    password = "aiquant"

    # uri format: dialect[+driver]://user:password@host/dbname[?key=value..]
    uri = 'postgresql+psycopg2://{user}:{passwd}@{host}:{port}/{dbname}'.format(
        host=host,
        port=port,
        user=user,
        passwd=password,
        dbname=dbname
    )
    pool_size = 10
    max_overflow = 20
    pool_recycle = 300
