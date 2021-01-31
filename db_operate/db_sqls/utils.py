#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: utils.py
@time: 2021/1/25 10:18
"""
from psycopg2.extras import RealDictCursor
import psycopg2
from psycopg2 import sql

from config.config import DBConfig

psycopg2.extensions.register_adapter(list, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)


def insert_on_conflict_template(table: str, cols: list, vals: list, conflict_cols: list, update_cols: list):
    val_holders = ', '.join(["%s" for _ in range(len(vals))])

    command = sql.SQL("""
            INSERT INTO {table} ({cols})
            VALUES ({vals})
            ON CONFLICT ({conflict_cols}) 
            DO UPDATE SET {update_sql}
            returning {update_cols};
        """).format(
        table=sql.SQL(table),
        cols=sql.SQL(',').join([sql.Identifier(c) for c in cols]),
        conflict_cols=sql.SQL(',').join([sql.Identifier(c) for c in conflict_cols]),
        update_sql=sql.SQL(',').join([sql.SQL("{}=EXCLUDED.{}".format(c, c)) for c in update_cols]),
        update_cols=sql.SQL(',').join([sql.Identifier(c) for c in update_cols]),
        vals=sql.SQL(val_holders)
    )
    return command


def pg_execute(command: str, returning: bool = False):
    """
    执行 postgresql 数据库命令
    Args:
        returning: 是否返回
        command: SQL 命令

    Returns:

    """

    conn = psycopg2.connect(host=DBConfig.host,
                            port=DBConfig.port,
                            dbname=DBConfig.dbname,
                            user=DBConfig.user,
                            password=DBConfig.password)
    return_data = None
    try:
        command = sql.SQL(command)
        with conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            with cur:
                cur.execute(command)
                if returning:
                    return_data = cur.fetchall()
    except Exception as e:
        raise e
    finally:
        conn.close()
    return return_data


def insert_template(table: str, cols: list, vals: list):
    val_holders = ', '.join(["%s" for _ in range(len(vals))])

    command = sql.SQL("""
        INSERT INTO {table} ({cols})
        VALUES ({val_holders})
        returning id;
    """).format(
        table=sql.SQL(table),
        cols=sql.SQL(',').join(
            [sql.Identifier(c) for c in cols]
        ),
        val_holders=sql.SQL(val_holders)
    )
    return command


def pg_insert(table: str, cols: list, vals: list):
    """

    Args:
        table: 表名
        cols: 字段
        vals: 值

    Returns:

    """
    assert len(cols) == len(vals), "cols 和 vals 两个 list 长度必须一致！"
    command = insert_template(table, cols, vals)

    conn = psycopg2.connect(
        host=DBConfig.host,
        port=DBConfig.port,
        dbname=DBConfig.dbname,
        user=DBConfig.user,
        password=DBConfig.password
    )
    try:
        with conn:
            cur = conn.cursor()
            with cur:
                cur.execute(command, vals)
                conn.commit()
                id_ = cur.fetchone()
    finally:
        conn.close()

    return id_


def pg_insert_on_conflict(table: str, cols: list, vals: list, conflict_cols: list, update_cols: list):
    """

    Args:
        table: 表名
        cols: 更新字段
        vals: 更新值
        conflict_cols: 互斥字段
        update_cols: 更新字段

    Returns:

    """
    assert len(cols) == len(vals), "cols 和 vals 两个 list 长度必须一致！"
    command = insert_on_conflict_template(table, cols, vals, conflict_cols, update_cols)

    conn = psycopg2.connect(
        host=DBConfig.host,
        port=DBConfig.port,
        dbname=DBConfig.dbname,
        user=DBConfig.user,
        password=DBConfig.password
    )
    try:
        with conn:
            cur = conn.cursor()
            with cur:
                cur.execute(command, vals)
                conn.commit()
                id_ = cur.fetchone()
    finally:
        conn.close()

    return id_


def insert_on_conflict_batch_template(table: str, cols: list, conflict_cols: list, update_cols: list):
    val_holders = ', '.join(["%s" for _ in range(len(cols))])

    command = sql.SQL("""
            INSERT INTO {table} ({cols})
            VALUES ({vals})
            ON CONFLICT ({conflict_cols}) 
            DO UPDATE SET {update_sql};
        """).format(
        table=sql.SQL(table),
        vals=sql.SQL(val_holders),
        cols=sql.SQL(',').join([sql.Identifier(c) for c in cols]),
        conflict_cols=sql.SQL(',').join([sql.Identifier(c) for c in conflict_cols]),
        update_sql=sql.SQL(',').join([sql.SQL("{}=EXCLUDED.{}".format(c, c)) for c in update_cols]),
        update_cols=sql.SQL(',').join([sql.Identifier(c) for c in update_cols]),
    )
    return command


def pg_insert_on_conflict_batch(table: str, cols: list, lines: list, conflict_cols: list, update_cols: list):
    """

    Args:
        table: 表名
        cols: [k1, k2, ...]
        lines: [(v1, v2, ...), ...]
        conflict_cols: 互斥字段
        update_cols: 更新字段

    Returns:

    """
    command = insert_on_conflict_batch_template(table, cols, conflict_cols, update_cols)

    conn = psycopg2.connect(
        host=DBConfig.host,
        port=DBConfig.port,
        dbname=DBConfig.dbname,
        user=DBConfig.user,
        password=DBConfig.password
    )
    try:
        with conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            with cur:
                psycopg2.extras.execute_batch(cur, command, lines)
    except Exception as e:
        raise e
    finally:
        conn.close()


def insert_batch_template(table, cols):
    command = sql.SQL("""
        INSERT INTO {table} ({cols})
        VALUES %s;
    """).format(
        table=sql.SQL(table),
        cols=sql.SQL(',').join(
            [sql.Identifier(c) for c in cols]
        ),
    )
    template = "(" + ','.join(["%({})s".format(c) for c in cols]) + ")"
    return command, template


def pg_insert_batch(table: str, cols: list, lines: list):
    """
    批量插入数据
    Args:
        table: 表名
        cols: [k1, k2, ...]
        lines: [{k1:v1, k2:v2 ...}, ...]

    Returns:

    """
    command, template = insert_batch_template(table, cols)

    conn = psycopg2.connect(
        host=DBConfig.host,
        port=DBConfig.port,
        dbname=DBConfig.dbname,
        user=DBConfig.user,
        password=DBConfig.password
    )
    try:
        with conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            with cur:
                psycopg2.extras.execute_values(cur, command, lines, template)
    except Exception as e:
        raise e
    finally:
        conn.close()


def pg_fetchall(table: str, fields: list):
    """

    Args:
        table:
        fields:

    Returns:

    """
    command = sql.SQL("""
        select {fields} from {table};
        """).format(
        fields=sql.SQL(',').join(
            [sql.SQL(c) for c in fields]
        ),
        table=sql.SQL(table)
    )

    conn = psycopg2.connect(host=DBConfig.host,
                            port=DBConfig.port,
                            dbname=DBConfig.dbname,
                            user=DBConfig.user,
                            password=DBConfig.password)
    try:
        with conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            with cur:
                cur.execute(command)
                data = cur.fetchall()
    except Exception as e:
        raise e
    finally:
        conn.close()

    return data
