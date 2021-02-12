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
from strategy_lab.lab_config.config import OUT_DIR, TS, TRADE_DAY_NUM_OF_YEAR, DATE_FMT
from strategy_lab.lab_logger.logger import logger
from strategy_lab.utils import argvalmin, trade_date_calc, yearly_profit_rate, first_true

index_code2name = {
    "000001.XSHG": "上证指数",
    "000300.XSHG": "沪深300",
    # "000016.XSHG": "上证50",
    # "000010.XSHG": "上证180",
    # "000009.XSHG": "上证380",
    "000905.XSHG": "中证500",
    # "000002.XSHG": "A股指数",
}

fund_code2name = {
    "510300.XSHG": "华泰柏瑞沪深300ETF (510300)"
}


def get_date_corr(link_id: str, column: str, code: str, window_size: int = TRADE_DAY_NUM_OF_YEAR):
    """
    滑动窗口 n 个交易日计算过去资金流向和指数的相关系数
    Args:
        link_id: 资金流 id
        column: "net_buy" or "net_flow"
        code: 指数 id
        window_size: 滑动窗口大小

    Returns:
        date_l: ['2020-01-01', ...]
        corr_l: [0.02, ...]
    """
    # 初始化数据
    moneyflow_hsgt__r = pg_execute(
        command=JQQuerySQL.moneyflow_hsgt__i_link_id__r_date__net_buy__net_flow.format(link_id=link_id), returning=True)
    mf_df = pd.DataFrame(moneyflow_hsgt__r)
    mf_df.set_index("date", inplace=True)
    mf_df.sort_index(inplace=True)
    mf_df = pd.DataFrame(mf_df[column])
    mf_df = mf_df.astype(float)

    index_daily__r = pg_execute(command=JQQuerySQL.index_daily__i_code__r_date__close.format(code=code), returning=True)
    index_df = pd.DataFrame(index_daily__r)
    index_df.set_index("date", inplace=True)
    index_df.sort_index(inplace=True)
    index_df = index_df.astype(float)

    # 取两个数据的交集时间
    # 滑动 n 个交易日的窗口计算相关系数
    date_l = []
    corr_l = []
    merged_df = mf_df.join(index_df, how="inner")
    # merged_df["close"] = mf_df.join(index_df, how="inner")["close"] / merged_df.iloc[0]["close"]
    window_df = merged_df.rolling(window=window_size).corr().dropna()
    for (cur_date, idx_col), r in window_df.iterrows():
        if idx_col == column:
            date_l.append(cur_date)
            corr_l.append(r["close"])

    return date_l, corr_l


def plot_corr():
    window_size = 120
    title = "北向资金净买入与各个指数相关性系数（滑动窗口={}天）".format(window_size)
    mf_column = "net_buy"
    link_id = "310005"
    x_label = "日期"
    y_label = "相关性系数"

    os.makedirs(OUT_DIR, exist_ok=True)

    import matplotlib.pyplot as plt
    from matplotlib import ticker

    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 显示中文

    fig_w_inch, fig_h_inch = 32, 8
    date_show_margin = 30  # 坐标轴日期间隔天

    fig, ax = plt.subplots()
    fig.set_size_inches(fig_w_inch, fig_h_inch)
    for code in index_code2name:
        date_l, corr_l = get_date_corr(link_id=link_id, column=mf_column, code=code, window_size=window_size)
        ax.plot(date_l, corr_l, label=index_code2name[code])
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.grid(True)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(base=date_show_margin))

    fig.suptitle(title)
    plt.xticks(rotation=90)
    plt.legend(loc='lower right')
    plt.savefig(os.path.join(OUT_DIR, "{}_{}.png".format(title, TS)), bbox_inches='tight')
    # plt.show()


def get_date_mean_std(link_id: str, column: str, since: datetime):
    """
    滑动窗口 n 个交易日计算资金流向的均值和标准差
    Args:
        link_id: 资金流 id
        column: "net_buy" or "net_flow"
        since: 2020-01-01

    Returns:
        DataFrame(
            {
            date: ['2020-01-01', ...],
            val: [12, ...],
            mean: [10, ...],
            std: [0.2, ...]}
        )

    """
    moneyflow_hsgt__r = pg_execute(
        command=JQQuerySQL.moneyflow_hsgt__i_link_id__r_date__net_buy__net_flow.format(link_id=link_id), returning=True)
    mf_df = pd.DataFrame(moneyflow_hsgt__r)
    mf_df = mf_df.loc[mf_df["date"] >= since.strftime("%Y-%m-%d")]
    mf_df.sort_values(by="date", inplace=True, ignore_index=True)

    mf_date = sorted(mf_df["date"].tolist())
    latest = mf_date[-1]

    # 滑动 n 个交易日的窗口
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
    return pd.DataFrame({"date": date_l, "val": val_l, "mean": mean_l, "std": std_l})


def run_strategy(code: str, code_column: str, buy_date_l: list, sell_date_l: list, capital: float = 1000000.00):
    """
    根据买入和卖出信号进行买卖策略，获得持有期状态，例如持有期收益。
    Args:
        code: 操作标的
        code_column: 操作时机 close / open
        buy_date_l: 触发买入信号日期列表
        sell_date_l: 触发卖出入信号日期列表
        capital: 初始资金

    Returns:
        run_states: [{state1}, ...]

    """

    assert len(buy_date_l) > 0 and len(sell_date_l) > 0, "至少有一次买卖信号"
    init_capital = capital

    daily__r = pg_execute(
        command=JQQuerySQL.fund_daily__i_code__r_date__open__close.format(code=code), returning=True)
    daily_df = pd.DataFrame(daily__r)
    daily_df.set_index(keys="date", inplace=True)
    daily_df.sort_index(inplace=True)
    daily_df = daily_df.astype(float)

    logger.info("初始本金: {}, 策略执行开始...".format(init_capital))

    # 获取持有时间区间
    hold_period = []
    buy_date_l.sort()
    sell_date_l.sort()
    buy_date, sell_date = buy_date_l[0], None
    while True:
        sell_date = first_true(sell_date_l, pred=lambda x: x > buy_date)
        if not sell_date:
            break

        hold_period.append((buy_date, sell_date))

        buy_date = first_true(buy_date_l, pred=lambda x: x > sell_date)
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
        buy_price, sell_price = buy_row[code_column], sell_row[code_column]
        shares = capital / buy_price
        holdings = buy_price * shares
        profit = sell_price * shares - holdings
        profit_rate = profit / holdings
        day_profit = [r["close"] * shares - holdings for _, r in hold_days.iterrows()]
        day_profit_rate = [p / holdings for p in day_profit]
        max_retracement_idx, max_retracement_val = argvalmin([min(_, 0) for _ in day_profit_rate])  # 获取最大回撤当天的索引
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
    total_profit_rate = end_state["capital_after"] / init_capital - 1
    total_max_retracement_p_idx, total_max_retracement_p_val = argvalmin(
        [s["max_retracement"].get("value", 0) for s in period_states])
    if total_max_retracement_p_val < 0:
        # 有回撤
        total_max_retracement = period_states[total_max_retracement_p_idx]["max_retracement"]
    else:
        # 无回撤
        total_max_retracement = {}

    start_date, end_date = period_states[0]["buy_date"], end_state["sell_date"]
    yearly_profit_rate_ = yearly_profit_rate(start_date=datetime.strptime(start_date, DATE_FMT),
                                             end_date=datetime.strptime(end_date, DATE_FMT),
                                             capital_before=init_capital,
                                             capital_after=end_state["capital_after"])
    stat = {
        "daily_df": daily_df,
        "period_states": period_states,
        "total_profit": total_profit,
        "total_profit_rate": total_profit_rate,
        "total_max_retracement": total_max_retracement,
        "yearly_profit_rate": yearly_profit_rate_,
    }

    return stat


def report(model_out):
    period_states = model_out["period_states"]
    for s in period_states:
        logger.info(
            "{buy_date} 以 [{buy_price}] 买入，{sell_date} 以 [{sell_price}] 卖出。收益：[{profit:.2f}], 收益率：[{profit_rate:.2%}]，本金：[{capital:.2f}]".format(
                buy_date=s["buy_date"],
                buy_price=s["buy_price"],
                sell_date=s["sell_date"],
                sell_price=s["sell_price"],
                profit=s["profit"],
                profit_rate=s["profit_rate"],
                capital=s["capital_after"],
            ))
    start_date, end_date = period_states[0]["buy_date"], period_states[-1]["sell_date"]

    total_stat_log = "{buy_date} 至 {sell_date} 总收益：{total_profit:.2f}，总收益率：{total_profit_rate:.2%}。".format(
        buy_date=start_date,
        sell_date=end_date,
        total_profit=model_out["total_profit"],
        total_profit_rate=model_out["total_profit_rate"],

    )
    total_max_retracement = model_out["total_max_retracement"]
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
    # 年化指标
    yearly_stat = "年化收益率：{yearly_profit_rate:.2%}，年化波动率".format(yearly_profit_rate=model_out["yearly_profit_rate"])

    total_stat_log += yearly_stat
    logger.info(total_stat_log)


def plot_boll():
    title = "北向资金净买入布林带策略"
    link_id = "310005"
    mf_column = "net_buy"
    since = datetime(2015, 1, 1)
    boll_factor = 1.5

    x_label = "日期"
    boll_y_label = "净买入（亿元）"
    daily_y_label = "净值（元）"

    good_color = "red"
    bad_color = "green"

    code = "510300.XSHG"
    code_column = "open"  # 操作时机 close / open


    os.makedirs(OUT_DIR, exist_ok=True)

    import matplotlib.pyplot as plt
    from matplotlib import ticker

    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 显示中文
    fig_w_inch, fig_h_inch = 128, 8  # 画布大小 (英寸)
    date_show_margin = 30  # 坐标轴日期间隔天

    boll_df = get_date_mean_std(link_id=link_id, column=mf_column, since=since)
    boll_df["next_trade_date"] = boll_df["date"].shift(-1)
    boll_df.iloc[-1]["next_trade_date"] = trade_date_calc(boll_df.iloc[-1]["date"], add_days=1)  # fill last date
    boll_df["boll_top"] = boll_df["mean"] + boll_df["std"] * boll_factor
    boll_df["boll_bottom"] = boll_df["mean"] - boll_df["std"] * boll_factor
    boll_buy_sig_df = boll_df.loc[boll_df["val"] >= boll_df["boll_top"]]
    boll_sell_sig_df = boll_df.loc[boll_df["val"] <= boll_df["boll_bottom"]]

    # 在收到信号的第二天开盘进行操作
    boll_buy_df = boll_df.set_index("date").loc[boll_buy_sig_df["next_trade_date"].to_list()].reset_index().rename(
        {"index": "date"})
    boll_sell_df = boll_df.set_index("date").loc[boll_sell_sig_df["next_trade_date"].to_list()].reset_index().rename(
        {"index": "date"})

    # 执行策略
    run_output = run_strategy(code=code, code_column=code_column, buy_date_l=boll_buy_df["date"].to_list(),
                              sell_date_l=boll_sell_df["date"].to_list())
    # 输出策略状态和结果
    report(run_output)

    # 绘制布林带策略图
    fig, axs = plt.subplots(nrows=2, ncols=1, sharex=True)
    fig.set_size_inches(fig_w_inch, fig_h_inch)

    ax11, ax21 = axs[0], axs[1]

    # 布林带图
    ax11.plot(boll_df["date"], boll_df["val"], label="北向资金净买入", color="orange")
    ax11.plot(boll_df["date"], boll_df["mean"], label="北向资金净买入前252交易日均值", alpha=0.6)
    ax11.plot(boll_df["date"], boll_df["boll_top"], label="北向资金净买入前252交易日均值 + {} 标准差".format(boll_factor), c="gray",
              linestyle='-', alpha=0.8)
    ax11.plot(boll_df["date"], boll_df["boll_bottom"], label="北向资金净买入前252交易日均值 - {} 标准差".format(boll_factor), c="gray",
              linestyle='-', alpha=0.8)
    ax11.scatter(boll_buy_sig_df["date"], boll_buy_sig_df["val"], marker="o", s=100, c=good_color, label="买入信号")
    ax11.scatter(boll_sell_sig_df["date"], boll_sell_sig_df["val"], marker="o", s=100, c=bad_color, label="卖出信号")
    ax11.scatter(boll_buy_df["date"], boll_buy_df["val"], marker="x", s=100, c=good_color, label="实际买入点")
    ax11.scatter(boll_sell_df["date"], boll_sell_df["val"], marker="x", s=100, c=bad_color, label="实际卖出点")

    for state in run_output["period_states"]:
        fx = [state["buy_date"], state["sell_date"]]
        color = good_color if state["profit_rate"] > 0 else bad_color
        ax11.axvline(fx[0], color=color)
        ax11.axvline(fx[1], color=color)
        ax11.fill_between(fx, 0, 1, color=color, alpha=0.2, transform=ax11.get_xaxis_transform())
    ax11.set_ylabel(boll_y_label)
    ax11.grid(True)
    ax11.xaxis.set_major_locator(ticker.MultipleLocator(base=date_show_margin))
    ax11.legend(loc='lower right')

    # 指数基金价格图
    daily_df = run_output["daily_df"]
    daily_df = daily_df.reindex(boll_df["date"]).fillna(method="ffill")
    daily_buy_sig_df = daily_df.reindex(boll_buy_sig_df["date"]).fillna(method="ffill")
    daily_sell_sig_df = daily_df.reindex(boll_sell_sig_df["date"]).fillna(method="ffill")
    daily_buy_df = daily_df.reindex(boll_buy_df["date"]).fillna(method="ffill")
    daily_sell_df = daily_df.reindex(boll_sell_df["date"]).fillna(method="ffill")

    ax21.plot(daily_df.index, daily_df[code_column], label="{}".format(fund_code2name[code]), color="black")
    ax21.scatter(daily_buy_sig_df.index, daily_buy_sig_df[code_column], marker="o", s=100, c=good_color, label="买入信号")
    ax21.scatter(daily_sell_sig_df.index, daily_sell_sig_df[code_column], marker="o", s=100, c=bad_color, label="卖出信号")
    ax21.scatter(daily_buy_df.index, daily_buy_df[code_column], marker="x", s=100, c=good_color, label="实际买入点")
    ax21.scatter(daily_sell_df.index, daily_sell_df[code_column], marker="x", s=100, c=bad_color, label="实际卖出点")

    for state in run_output["period_states"]:
        fx = [state["buy_date"], state["sell_date"]]
        fy = [state["buy_price"], state["sell_price"]]
        color = good_color if state["profit_rate"] > 0 else bad_color
        ax21.axvline(fx[0], color=color)
        ax21.axvline(fx[1], color=color)
        ax21.fill_between(fx, 0, 1, color=color, alpha=0.2, transform=ax21.get_xaxis_transform())
        ax21.annotate("{}".format(fy[0]),
                      xy=(fx[0], fy[0]), xycoords='data',
                      xytext=(0, 50), textcoords='offset points',
                      bbox=dict(boxstyle="round", fc=good_color),
                      arrowprops=dict(arrowstyle="->",
                                      connectionstyle="arc3"),
                      fontsize='large', color="white",
                      )
        ax21.annotate("{}".format(fy[1]),
                      xy=(fx[1], fy[1]), xycoords='data',
                      xytext=(0, 50), textcoords='offset points',
                      bbox=dict(boxstyle="round", fc=bad_color),
                      arrowprops=dict(arrowstyle="->",
                                      connectionstyle="arc3"),
                      fontsize='large', color="white",
                      )
    ax21.set_ylim(min(daily_df[code_column]), max(daily_df[code_column]))
    ax21.set_ylabel(daily_y_label)
    ax21.grid(True)
    ax21.xaxis.set_major_locator(ticker.MultipleLocator(base=date_show_margin))
    ax21.legend(loc='lower right')

    fig.suptitle(title)
    plt.xlabel(x_label)
    plt.xticks(rotation=90)
    plt.savefig(os.path.join(OUT_DIR, "{}_{}.png".format(title, TS)), bbox_inches='tight')
    # plt.show()


if __name__ == '__main__':
    # plot_boll()
    plot_corr()
