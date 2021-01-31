#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: init_tables.py
@time: 2021/1/22 21:06
"""
import sqlalchemy

from db_operate.db_sqlalchemy.jq_sqlalchemy import jq_metadata, engine


def ts_init():
    ...


def jq_init():
    if not engine.dialect.has_schema(engine, jq_metadata.schema):
        engine.execute(sqlalchemy.schema.CreateSchema(jq_metadata.schema))
    jq_metadata.create_all(engine)
    print("table init finished!")


if __name__ == '__main__':
    # ts_init()
    jq_init()
