#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: NorthFlow.py
@time: 2021/1/22 19:03
@desc: 复现研报： https://mp.weixin.qq.com/s/Rd2qX9n8OlL_UP1CWcBTZw
"""
import os
from datetime import datetime

import pandas as pd

from db_operate.db_sqls.pg_sqls import JQQuerySQL
from db_operate.db_sqls.utils import pg_execute
from strategy_lab.lab_config.config import OUT_DIR

index_code2name = {
    "000001.XSHG": "上证指数",
    "000300.XSHG": "沪深300",
    # "000016.XSHG": "上证50",
    # "000010.XSHG": "上证180",
    # "000009.XSHG": "上证380",
    "000905.XSHG": "中证500",
    # "000002.XSHG": "A股指数",
}
TRADE_DAY_NUM_OF_YEAR = 252
TS = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def get_date_corr(link_id: str, column: str, code: str):
    """
    滑动窗口 252 个交易日计算过去资金流向和指数的相关系数
    Args:
        column: "net_buy" or "net_flow"
        link_id: 资金流 id
        code: 指数 id

    Returns:
        date_axis: ['2020-01-01', ...]
        corr_axis: [0.02, ...]
    """
    # 初始化数据
    moneyflow_hsgt__r = pg_execute(
        command=JQQuerySQL.moneyflow_hsgt__i_link_id__r_date__net_buy__net_flow.format(link_id=link_id), returning=True)
    mf_df = pd.DataFrame(moneyflow_hsgt__r)
    mf_df.sort_values(by="date", inplace=True, ignore_index=True)

    index_daily__r = pg_execute(command=JQQuerySQL.index_daily__i_code__r_date__close.format(code=code), returning=True)
    index_df = pd.DataFrame(index_daily__r)
    index_df.sort_values(by="date", inplace=True, ignore_index=True)

    # 取两个数据的交集时间
    mf_date = sorted(mf_df["date"].tolist())
    index_date = sorted(index_df["date"].tolist())
    # oldest = max(mf_date[0], index_date[0])
    latest = min(mf_date[-1], index_date[-1])

    # 滑动 252 个交易日的窗口计算相关系数
    date_axis = []
    corr_axis = []
    mf_end_idx = mf_df[mf_df["date"] == latest].index[0]
    while True:
        cur_date = mf_df.iloc[mf_end_idx]["date"]
        index_end_row = index_df[index_df["date"] == cur_date]
        if index_end_row.empty:
            mf_end_idx -= 1
            continue
        index_end_idx = index_end_row.index[0]

        mf_start_idx = mf_end_idx - TRADE_DAY_NUM_OF_YEAR
        index_start_idx = index_end_idx - TRADE_DAY_NUM_OF_YEAR

        if index_start_idx < index_df.index.min() or mf_start_idx < mf_df.index.min():
            break

        mf_df_wid = mf_df[mf_start_idx:mf_end_idx]
        index_df_wid = index_df[index_start_idx:index_end_idx]

        xdf = pd.DataFrame({
            "mf": mf_df_wid[column].tolist(),
            "index": index_df_wid["close"].tolist()},
            dtype=float)

        corr_df = xdf.corr()
        cur_corr = corr_df.loc["mf", "index"]

        date_axis.append(cur_date)
        corr_axis.append(cur_corr)

        mf_end_idx -= 1

    date_axis, corr_axis = date_axis[::-1], corr_axis[::-1]  # 按时间顺序排列
    return date_axis, corr_axis


def plot_and_save():
    title = "北向资金净买入与各个指数相关性系数"
    mf_column = "net_buy"
    link_id = "310005"
    x_label = "日期"
    y_label = "相关性系数"

    os.makedirs(OUT_DIR, exist_ok=True)

    import matplotlib.pyplot as plt
    from matplotlib import ticker

    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 显示中文

    fig_w_inch, fig_h_inch = 16, 8
    date_show_margin = 60  # 坐标轴日期间隔天


    fig, axs = plt.subplots()
    fig.set_size_inches(fig_w_inch, fig_h_inch)
    for code in index_code2name:
        date_l, corr_l = get_date_corr(link_id=link_id, column=mf_column, code=code)
        axs.plot(date_l, corr_l, label=index_code2name[code])
    axs.set_xlabel(x_label)
    axs.set_ylabel(y_label)
    axs.grid(True)
    axs.xaxis.set_major_locator(ticker.MultipleLocator(base=date_show_margin))

    fig.suptitle(title)
    plt.xticks(rotation=90)
    plt.legend(loc='lower right')
    plt.savefig(os.path.join(OUT_DIR, "{}_{}.png".format(title, TS)), bbox_inches='tight')
    plt.show()


if __name__ == '__main__':
    plot_and_save()
