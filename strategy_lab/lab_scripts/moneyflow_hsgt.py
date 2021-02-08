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
from strategy_lab.lab_logger.logger import logger
from strategy_lab.utils import argvalmin, trade_date_calc

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
DATE_FMT = "%Y-%m-%d"


def get_date_corr(link_id: str, column: str, code: str):
    """
    滑动窗口 252 个交易日计算过去资金流向和指数的相关系数
    Args:
        link_id: 资金流 id
        column: "net_buy" or "net_flow"
        code: 指数 id

    Returns:
        date_l: ['2020-01-01', ...]
        corr_l: [0.02, ...]
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
    date_l = []
    corr_l = []
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

        date_l.append(cur_date)
        corr_l.append(cur_corr)

        mf_end_idx -= 1

    date_l, corr_l = date_l[::-1], corr_l[::-1]  # 按时间顺序排列
    return date_l, corr_l


def plot_corr():
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

    fig, ax = plt.subplots()
    fig.set_size_inches(fig_w_inch, fig_h_inch)
    for code in index_code2name:
        date_l, corr_l = get_date_corr(link_id=link_id, column=mf_column, code=code)
        ax.plot(date_l, corr_l, label=index_code2name[code])
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.grid(True)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(base=date_show_margin))

    fig.suptitle(title)
    plt.xticks(rotation=90)
    plt.legend(loc='lower right')
    plt.savefig(os.path.join(OUT_DIR, "{}_{}.png".format(title, TS)), bbox_inches='tight')
    plt.show()


def get_date_mean_std(link_id: str, column: str, since: datetime):
    """
    滑动窗口 252 个交易日计算资金流向的均值和标准差
    Args:
        link_id: 资金流 id
        column: "net_buy" or "net_flow"
        since: 2020-01-01

    Returns:
        date_l: ['2020-01-01', ...]
        val_l: [12, ...]
        mean_l: [10, ...]
        std_l: [0.2, ...]

    """
    moneyflow_hsgt__r = pg_execute(
        command=JQQuerySQL.moneyflow_hsgt__i_link_id__r_date__net_buy__net_flow.format(link_id=link_id), returning=True)
    mf_df = pd.DataFrame(moneyflow_hsgt__r)
    mf_df = mf_df.loc[mf_df["date"] >= since.strftime("%Y-%m-%d")]
    mf_df.sort_values(by="date", inplace=True, ignore_index=True)

    mf_date = sorted(mf_df["date"].tolist())
    latest = mf_date[-1]

    # 滑动 252 个交易日的窗口
    date_l = []
    val_l = []
    mean_l = []
    std_l = []
    mf_end_idx = mf_df[mf_df["date"] == latest].index[0]
    while True:
        cur_date = mf_df.iloc[mf_end_idx]["date"]
        cur_val = mf_df.iloc[mf_end_idx][column]
        mf_start_idx = mf_end_idx - TRADE_DAY_NUM_OF_YEAR

        if mf_start_idx < mf_df.index.min():
            break

        mf_df_wid = mf_df[mf_start_idx:mf_end_idx]

        cur_mean = mf_df_wid[column].astype(float).mean()
        cur_std = mf_df_wid[column].astype(float).std()
        date_l.append(cur_date)
        val_l.append(cur_val)
        mean_l.append(cur_mean)
        std_l.append(cur_std)

        mf_end_idx -= 1
    date_l, val_l, mean_l, std_l = date_l[::-1], val_l[::-1], mean_l[::-1], std_l[::-1]  # 按时间顺序排列
    return date_l, val_l, mean_l, std_l


def run_strategy(code: str, buy_sig_date: list, sell_sig_date: list, capital: float = 1000000.00):
    """
    根据买入和卖出信号进行买卖策略，获得持有期状态，例如持有期收益。
    Args:
        code: 操作标的
        buy_sig_date: 触发买入信号日期列表
        sell_sig_date: 触发卖出入信号日期列表
        capital: 初始资金

    Returns:
        run_states: [{state1}, ...]

    """
    from strategy_lab.utils import first_true

    assert len(buy_sig_date) > 0 and len(sell_sig_date) > 0, "至少有一次买卖信号"
    init_capital = capital

    # 在收到信号的第二天开盘进行操作
    buy_sig_date = [trade_date_calc(d, add_days=1) for d in buy_sig_date]
    sell_sig_date = [trade_date_calc(d, add_days=1) for d in sell_sig_date]

    daily__r = pg_execute(
        command=JQQuerySQL.index_daily__i_code__r_date__open__close.format(code=code), returning=True)
    daily_df = pd.DataFrame(daily__r)
    daily_df.set_index(keys="date", inplace=True)
    daily_df.sort_index(inplace=True)
    daily_df = daily_df.astype(float)

    # 获取持有时间区间
    hold_period = []
    buy_sig_date.sort()
    sell_sig_date.sort()
    buy_date, sell_date = buy_sig_date[0], None
    while True:
        sell_date = first_true(sell_sig_date, pred=lambda x: x > buy_date)
        if not sell_date:
            break

        hold_period.append((buy_date, sell_date))

        buy_date = first_true(buy_sig_date, pred=lambda x: x > sell_date)
        if not buy_date:
            break

    # 获取持有区间的 收益、收益率
    period_states = []
    for buy_date, sell_date in hold_period:
        buy_row = daily_df.loc[buy_date]
        sell_row = daily_df.loc[sell_date]
        hold_days = daily_df.loc[buy_date: sell_date]

        if buy_row.empty or sell_row.empty:
            miss_date = buy_date if buy_row.empty else sell_date
            logger.warning("[数据缺失] {code}, 缺失日期：{miss_date}".format(code=code, miss_date=miss_date))
            continue

        # 交易策略
        # 默认开盘买入和卖出，每次操作都是全仓
        buy_price, sell_price = buy_row["open"], sell_row["open"]
        shares = capital / buy_price
        holdings = buy_price * shares
        profit = sell_price * shares - holdings
        profit_rate = profit / holdings
        day_profit = [r["close"] * shares - holdings for _, r in hold_days.iterrows()]
        day_profit_rate = [p / holdings for p in day_profit]
        max_retracement_idx, max_retracement_val = argvalmin([min(_, 0)for _ in day_profit_rate])  # 获取最大回撤当天的索引
        if max_retracement_val < 0:
            # 有回撤
            max_retracement = {
                "date": hold_days.iloc[max_retracement_idx].name,
                "value": day_profit_rate[max_retracement_idx],
            }
        else:
            # 无回撤
            max_retracement = {}


        # 更新本金
        capital_before = capital
        capital *= (1 + profit_rate)

        s = {
            "buy_date": buy_date,
            "sell_date": sell_date,
            "buy_price": buy_price,
            "sell_price": sell_price,
            "shares": shares,
            "profit": profit,
            "profit_rate": profit_rate,
            "capital_before": capital_before,
            "capital_after": capital,
            "day_profit": day_profit,
            "day_profit_rate": day_profit_rate,
            "max_retracement": max_retracement,
        }
        period_states.append(s)

    # 全局统计
    end_state = period_states[-1]
    total_profit = sum([s["profit"] for s in period_states])
    total_profit_rate = end_state["capital_after"] / init_capital
    total_max_retracement_p_idx, total_max_retracement_p_val = argvalmin([s["max_retracement"].get("value", 0) for s in period_states])
    if total_max_retracement_p_val < 0:
        # 有回撤
        total_max_retracement = period_states[total_max_retracement_p_idx]["max_retracement"]
    else:
        # 无回撤
        total_max_retracement = {}

    stat = {
        "daily_df": daily_df,
        "period_states": period_states,
        "total_profit": total_profit,
        "total_profit_rate": total_profit_rate,
        "total_max_retracement": total_max_retracement,
    }

    return stat


def report(states_info):
    period_states = states_info["period_states"]
    for s in period_states:
        logger.info("{buy_date} 以 [{buy_price}] 买入，{sell_date} 以 [{sell_price}] 卖出。收益：[{profit:.2f}], 收益率：[{profit_rate:.2%}]".format(
            buy_date=s["buy_date"],
            buy_price=s["buy_price"],
            sell_date=s["sell_date"],
            sell_price=s["sell_price"],
            profit=s["profit"],
            profit_rate=s["profit_rate"]
        ))
    start_date, end_date = period_states[0]["buy_date"], period_states[-1]["sell_date"]

    total_stat_log = "{buy_date} 至 {sell_date} 总收益：{total_profit:.2f}，总收益率：{total_profit_rate:.2%}。".format(
            buy_date=start_date,
            sell_date=end_date,
            total_profit=states_info["total_profit"],
            total_profit_rate=states_info["total_profit_rate"],

        )
    total_max_retracement = states_info["total_max_retracement"]
    if total_max_retracement:
        # 有回撤
        total_stat_log += "最大回撤：{total_max_retracement_val:.2%}，发生于：{total_max_retracement_date}。".format(
            total_max_retracement_val=total_max_retracement.get("value"),
            total_max_retracement_date=total_max_retracement.get("date"),
        )
    else:
        # 无回撤
        total_stat_log += "持仓期间无回撤。".format(
            total_max_retracement_val=total_max_retracement.get("value"),
            total_max_retracement_date=total_max_retracement.get("date"),
        )
    logger.info(total_stat_log)


def plot_boll():
    title = "北向资金净买入布林带"
    link_id = "310005"
    mf_column = "net_buy"
    since = datetime(2018, 1, 1)
    x_label = "日期"
    y_label = "亿"
    boll_factor = 1.5
    code = "000300.XSHG"

    os.makedirs(OUT_DIR, exist_ok=True)

    import matplotlib.pyplot as plt
    from matplotlib import ticker

    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 显示中文
    fig_w_inch, fig_h_inch = 128, 8  # 画布大小 (英寸)
    date_show_margin = 2  # 坐标轴日期间隔天

    date_l, val_l, mean_l, std_l = get_date_mean_std(link_id=link_id, column=mf_column, since=since)
    boll_top = [m + s * boll_factor for m, s in zip(mean_l, std_l)]
    boll_bottom = [m - s * boll_factor for m, s in zip(mean_l, std_l)]
    buy_pt = [(d, v) for d, v, b in zip(date_l, val_l, boll_top) if v >= b]
    buy_pt_x, buy_pt_y = [p[0] for p in buy_pt], [p[1] for p in buy_pt]
    sell_pt = [(d, v) for d, v, b in zip(date_l, val_l, boll_bottom) if v <= b]
    sell_pt_x, sell_pt_y = [p[0] for p in sell_pt], [p[1] for p in sell_pt]

    # 执行策略
    run_output = run_strategy(code=code, buy_sig_date=buy_pt_x, sell_sig_date=sell_pt_x)
    # 输出策略状态和结果
    report(run_output)

    # 绘制布林带策略图
    fig, axs = plt.subplots(nrows=2, ncols=1, sharex=True)
    fig.set_size_inches(fig_w_inch, fig_h_inch)

    ax11, ax21 = axs[0], axs[1]

    # 布林带图
    ax11.plot(date_l, val_l, label="北向资金净买入", color="orange")
    ax11.plot(date_l, mean_l, label="北向资金净买入前252交易日均值", alpha=0.6)
    ax11.plot(date_l, boll_top, label="北向资金净买入前252交易日均值 + {} 标准差".format(boll_factor), c="gray", linestyle='-', alpha=0.8)
    ax11.plot(date_l, boll_bottom, label="北向资金净买入前252交易日均值 - {} 标准差".format(boll_factor), c="gray", linestyle='-', alpha=0.8)
    ax11.scatter(buy_pt_x, buy_pt_y, marker="o", s=100, c="red")
    ax11.scatter(sell_pt_x, sell_pt_y, marker="o", s=100, c="green")

    for state in run_output["period_states"]:
        fx = [state["buy_date"], state["sell_date"]]
        color = "red" if state["profit_rate"] > 0 else "green"
        ax11.axvline(fx[0], color=color)
        ax11.axvline(fx[1], color=color)
        ax11.fill_between(fx, 0, 1, color=color, alpha=0.2, transform=ax11.get_xaxis_transform())
    ax11.set_ylabel(y_label)
    ax11.grid(True)
    ax11.xaxis.set_major_locator(ticker.MultipleLocator(base=date_show_margin))
    ax11.legend(loc='lower right')

    # 指数基金价格图
    daily_df = run_output["daily_df"]
    daily_y = daily_df.reindex(date_l).fillna(method="ffill")["close"].tolist()
    buy_sig_y = daily_df.reindex(buy_pt_x).fillna(method="ffill")["close"].tolist()
    sell_sig_y = daily_df.reindex(sell_pt_x).dropna()["close"].tolist()

    ax21.plot(date_l, daily_y, label="{}".format(index_code2name[code]), color="black")
    ax21.scatter(buy_pt_x, buy_sig_y, marker="o", s=100, c="red")
    ax21.scatter(sell_pt_x, sell_sig_y, marker="o", s=100, c="green")
    for state in run_output["period_states"]:
        fx = [state["buy_date"], state["sell_date"]]
        color = "red" if state["profit_rate"] > 0 else "green"
        ax21.axvline(fx[0], color=color)
        ax21.axvline(fx[1], color=color)
        ax21.fill_between(fx, 0, 1, color=color, alpha=0.2, transform=ax21.get_xaxis_transform())
    ax21.set_ylim(min(daily_y), max(daily_y))
    ax21.set_ylabel("元")
    ax21.grid(True)
    ax21.xaxis.set_major_locator(ticker.MultipleLocator(base=date_show_margin))
    ax21.legend(loc='lower right')

    fig.suptitle(title)
    plt.xlabel(x_label)
    plt.xticks(rotation=90)
    plt.savefig(os.path.join(OUT_DIR, "{}_{}.png".format(title, TS)), bbox_inches='tight')
    # plt.show()


if __name__ == '__main__':
    plot_boll()
    # plot_corr()
