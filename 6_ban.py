## 导入函数库
from jqdata import *


## 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark("000300.XSHG")
    # 开启动态复权模式(真实价格)
    set_option("use_real_price", True)
    # 输出内容到日志 log.info()
    log.info("初始函数开始运行且全局只运行一次")
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')
    # g 内置全局变量
    g.my_security = "510300.XSHG"
    g.held_stock = []
    g.order_list = []
    set_universe([g.my_security])
    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(
        OrderCost(
            close_tax=0.001,
            open_commission=0.0003,
            close_commission=0.0003,
            min_commission=5,
        ),
        type="stock",
    )

    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
    # 开盘前运行
    run_daily(before_market_open, time="before_open", reference_security="000300.XSHG")
    # 开盘时运行
    run_daily(market_open, time="every_bar", reference_security="000300.XSHG")
    # run_daily(market_run_sell, time='every_bar', reference_security='000300.XSHG')

    # 收盘后运行before_open
    # run_daily(after_market_close, time='after_close', reference_security='000300.XSHG')


## 开盘时运行函数
def market_open(context):
    # time_buy = context.current_dt.strftime("%H:%M:%S")
    # aday = datetime.datetime.strptime('13:00:00', '%H:%M:%S').strftime('%H:%M:%S')
    security_positions = context.portfolio.positions
    if len(g.held_stock) > 0:
        for stock in g.held_stock:
            # log.info("当前时间 %s" % (context.current_dt))
            # log.info("股票 %s 的最新价: %f" % (stock, get_current_data()[stock].last_price))
            cash = context.portfolio.available_cash
            # print(cash)
            # day_open_price = get_current_data()[stock].day_open
            current_price = get_current_data()[stock].last_price
            high_limit_price = get_current_data()[stock].high_limit
            pre_date = (context.current_dt + timedelta(days=-1)).strftime("%Y-%m-%d")
            df_panel = get_price(
                stock,
                count=1,
                end_date=pre_date,
                frequency="daily",
                fields=[
                    "open",
                    "high",
                    "close",
                    "low",
                    "high_limit",
                    "money",
                    "pre_close",
                ],
            )
            # pre_high_limit = df_panel['high_limit'].values
            pre_close = df_panel["close"].values
            # pre_open = df_panel['open'].values
            # pre_high = df_panel['high'].values
            # pre_low = df_panel['low'].values
            # pre_pre_close = df_panel['pre_close'].values
            #
            # now = context.current_dt
            # zeroToday = now - datetime.timedelta(hours=now.hour, minutes=now.minute, seconds=now.second,microseconds=now.microsecond)
            # lastToday = zeroToday + datetime.timedelta(hours=9, minutes=31, seconds=00)
            # df_panel_allday = get_price(stock, start_date=lastToday, end_date=context.current_dt, frequency='minute', fields=['high','low','close','high_limit','money'])
            # low_allday = df_panel_allday.loc[:,"low"].min()
            # high_allday = df_panel_allday.loc[:,"high"].max()
            # sum_plus_num_two = (df_panel_allday.loc[:,'close'] == df_panel_allday.loc[:,'high_limit']).sum()

            # pre_date_two =  (context.current_dt + timedelta(days = -10)).strftime("%Y-%m-%d")
            # df_panel_150 = get_price(stock, count = 150,end_date=pre_date_two, frequency='daily', fields=['open', 'close','high_limit','money','low','high','pre_close'])
            # df_max_high_150 = df_panel_150["close"].max()
            ##当前持仓有哪些股票
            print(
                "当前持仓："
                + str(security_positions)
                + " 当前现金："
                + str(cash)
                + " 当前价格："
                + str(current_price)
                + " 当前涨幅："
                + str((current_price / float(pre_close) - 1) * 100)
                + "% 涨停价："
                + str(high_limit_price)
            )
            if cash > 5000 and high_limit_price > current_price >= pre_close * 1.05:
                print(
                    stock
                    + " 涨幅大于6%，买入金额 "
                    + str(cash)
                    + " at "
                    + context.current_dt.strftime("%H:%M:%S")
                )
                # send_micromessage("2.跳空首阴反包---" + stock)
                orders = order_value(stock, cash)
                if str(orders.status) == "held":
                    g.order_list.append(stock)
                    g.held_stock.remove(stock)

    # time_sell = context.current_dt.strftime("%H:%M:%S")
    # cday = datetime.datetime.strptime("14:30:00", "%H:%M:%S").strftime("%H:%M:%S")
    # sell_day = datetime.datetime.strptime("11:10:00", "%H:%M:%S").strftime("%H:%M:%S")
    # sell_day_10 = datetime.datetime.strptime("13:30:00", "%H:%M:%S").strftime(
    #     "%H:%M:%S"
    # )

    if len(security_positions) > 0:
        for cur_security in security_positions:
            if cur_security in g.order_list:
                continue

            ##获取前一天的收盘价
            pre_date = (context.current_dt + timedelta(days=-1)).strftime("%Y-%m-%d")
            cur_security_pre_date_panel = get_price(
                cur_security,
                count=1,
                end_date=pre_date,
                frequency="daily",
                fields=[
                    "open",
                    "close",
                    "high_limit",
                    "money",
                    "low",
                ],
            )
            cur_security_pre_date_close = cur_security_pre_date_panel["close"].values
            cur_security_pre_date_high_limit = cur_security_pre_date_panel[
                "high_limit"
            ].values

            if cur_security_pre_date_close == cur_security_pre_date_high_limit:
                current_price_list = get_ticks(
                    cur_security,
                    start_dt=None,
                    end_dt=context.current_dt,
                    count=1,
                    fields=["time", "current", "high", "low", "volume", "money"],
                )
                current_price = current_price_list["current"][0]

                if current_price > cur_security_pre_date_close * 1.07:
                    print(
                        "涨幅超过7%, 卖出："
                        + cur_security
                        + " at "
                        + context.current_dt.strftime("%H:%M:%S")
                        + " 当前价格："
                        + str(current_price)
                        + " 当前涨幅："
                        + str(
                            (current_price / float(cur_security_pre_date_close) - 1)
                            * 100
                        )
                    )
                    order_obj = order_target(cur_security, 0)
                    print("卖出价格：" + str(order_obj.price))

                # if current_price < cur_security_pre_date_close * 0.94:
                #     order_target(cur_security, 0)
                #     print(
                #         "跌幅大于6%， 卖出："
                #         + cur_security
                #         + " at "
                #         + context.current_dt.strftime("%H:%M:%S")
                #     )

                if context.current_dt.strftime("%H:%M:%S") > datetime.datetime.strptime(
                    "14:50:00", "%H:%M:%S"
                ).strftime("%H:%M:%S"):
                    print(
                        "快要收盘 卖出："
                        + cur_security
                        + " at "
                        + context.current_dt.strftime("%H:%M:%S")
                    )
                    order_obj = order_target(cur_security, 0)
                    print("卖出价格：" + str(order_obj.price))

            else:
                if context.current_dt.strftime("%H:%M:%S") > datetime.datetime.strptime(
                    "09:31:00", "%H:%M:%S"
                ).strftime("%H:%M:%S"):
                    print(
                        "前一天未涨停， 卖出："
                        + cur_security
                        + " at "
                        + context.current_dt.strftime("%H:%M:%S")
                    )
                    order_obj = order_target(cur_security, 0)
                    print("卖出价格：" + str(order_obj.price))

    #
    # if time_sell > cday:
    #     stock_owner = context.portfolio.positions
    #     if len(stock_owner) > 0:
    #         for stock_two in stock_owner:
    #             if context.portfolio.positions[stock_two].closeable_amount > 0:
    #                 current_price_list = get_ticks(
    #                     stock_two,
    #                     start_dt=None,
    #                     end_dt=context.current_dt,
    #                     count=1,
    #                     fields=["time", "current", "high", "low", "volume", "money"],
    #                 )
    #                 current_price = current_price_list["current"][0]
    #                 day_open_price = get_current_data()[stock_two].day_open
    #                 # day_high_limit = get_current_data()[stock_two].high_limit
    #
    #                 now = context.current_dt
    #                 zeroToday = now - datetime.timedelta(
    #                     hours=now.hour,
    #                     minutes=now.minute,
    #                     seconds=now.second,
    #                     microseconds=now.microsecond,
    #                 )
    #                 lastToday = zeroToday + datetime.timedelta(
    #                     hours=9, minutes=31, seconds=00
    #                 )
    #                 df_panel_allday = get_price(
    #                     stock_two,
    #                     start_date=lastToday,
    #                     end_date=context.current_dt,
    #                     frequency="minute",
    #                     fields=["high", "low", "close", "high_limit", "money"],
    #                 )
    #                 low_allday = df_panel_allday.loc[:, "low"].min()
    #                 high_allday = df_panel_allday.loc[:, "high"].max()
    #                 sum_plus_num_two = (
    #                     df_panel_allday.loc[:, "close"]
    #                     == df_panel_allday.loc[:, "high_limit"]
    #                 ).sum()
    #
    #                 ##获取前一天的收盘价
    #                 pre_date = (context.current_dt + timedelta(days=-1)).strftime(
    #                     "%Y-%m-%d"
    #                 )
    #                 df_panel = get_price(
    #                     stock_two,
    #                     count=1,
    #                     end_date=pre_date,
    #                     frequency="daily",
    #                     fields=[
    #                         "open",
    #                         "close",
    #                         "high_limit",
    #                         "money",
    #                         "low",
    #                     ],
    #                 )
    #                 pre_low_price = df_panel["low"].values
    #                 pre_close_price = df_panel["close"].values
    #
    #                 # 平均持仓成本
    #                 cost = context.portfolio.positions[stock_two].avg_cost
    #                 if (
    #                     current_price < high_allday * 0.92
    #                     and day_open_price > pre_close_price
    #                 ):
    #                     print("1.卖出股票：小于最高价0.869倍" + str(stock_two))
    #                     order_target(stock_two, 0)
    #                 elif current_price > cost * 1.3 and sum_plus_num_two < 80:
    #                     print("2.卖出股票：亏8个点" + str(stock_two))
    #                     order_target(stock_two, 0)
    #                 elif (
    #                     day_open_price < pre_close_price * 0.98
    #                     and current_price < pre_close_price * 0.93
    #                 ):
    #                     print("3.卖出股票：1.3以下" + str(stock_two))
    #                     order_target(stock_two, 0)
    # else:
    #     stock_owner = context.portfolio.positions
    #     if len(stock_owner) > 0:
    #         for stock_two in stock_owner:
    #             if context.portfolio.positions[stock_two].closeable_amount > 0:
    #                 current_price_list = get_ticks(
    #                     stock_two,
    #                     start_dt=None,
    #                     end_dt=context.current_dt,
    #                     count=1,
    #                     fields=["time", "current", "high", "low", "volume", "money"],
    #                 )
    #                 current_price = current_price_list["current"][0]
    #                 day_open_price = get_current_data()[stock_two].day_open
    #                 day_high_limit = get_current_data()[stock_two].high_limit
    #
    #                 ##获取前一天的收盘价
    #                 pre_date = (context.current_dt + timedelta(days=-1)).strftime(
    #                     "%Y-%m-%d"
    #                 )
    #                 df_panel = get_price(
    #                     stock_two,
    #                     count=1,
    #                     end_date=pre_date,
    #                     frequency="daily",
    #                     fields=[
    #                         "open",
    #                         "close",
    #                         "high_limit",
    #                         "money",
    #                         "low",
    #                     ],
    #                 )
    #                 pre_low_price = df_panel["low"].values
    #                 pre_close_price = df_panel["close"].values
    #                 now = context.current_dt
    #                 zeroToday = now - datetime.timedelta(
    #                     hours=now.hour,
    #                     minutes=now.minute,
    #                     seconds=now.second,
    #                     microseconds=now.microsecond,
    #                 )
    #                 lastToday = zeroToday + datetime.timedelta(
    #                     hours=9, minutes=31, seconds=00
    #                 )
    #                 df_panel_allday = get_price(
    #                     stock_two,
    #                     start_date=lastToday,
    #                     end_date=context.current_dt,
    #                     frequency="minute",
    #                     fields=["high", "low", "close", "high_limit", "money"],
    #                 )
    #                 low_allday = df_panel_allday.loc[:, "low"].min()
    #                 high_allday = df_panel_allday.loc[:, "high"].max()
    #                 sum_plus_num_two = (
    #                     df_panel_allday.loc[:, "close"]
    #                     == df_panel_allday.loc[:, "high_limit"]
    #                 ).sum()
    #
    #                 current_price = context.portfolio.positions[
    #                     stock_two
    #                 ].price  # 持仓股票的当前价
    #                 cost = context.portfolio.positions[stock_two].avg_cost
    #
    #                 if current_price < cost * 0.91:
    #                     print("6.卖出股票：亏5个点" + str(stock_two))
    #                     order_target(stock_two, 0)
    #                 elif current_price < pre_close_price and time_sell > sell_day:
    #                     print("7.高位放量，请走！" + str(day_open_price))
    #                     order_target(stock_two, 0)
    #                 elif (
    #                     day_open_price < pre_close_price * 0.95
    #                     and current_price > pre_close_price * 0.97
    #                 ):
    #                     print("add.高位放量，请走！" + str(day_open_price))
    #                     order_target(stock_two, 0)
    #                 elif (
    #                     high_allday > pre_close_price * 1.09
    #                     and current_price < day_open_price
    #                     and day_open_price < day_high_limit * 0.95
    #                     and current_price < cost * 1.2
    #                 ):
    #                     print("8.高位放量，请走！" + str(day_open_price))
    #                     order_target(stock_two, 0)
    #                 elif (
    #                     current_price > cost * 1.25
    #                     and current_price < day_high_limit * 0.95
    #                     and time_sell > sell_day
    #                 ):
    #                     print("9.挣够25%，高位放量，请走！" + str(day_open_price))
    #                     order_target(stock_two, 0)
    #                 elif (
    #                     day_open_price < pre_close_price * 0.98
    #                     and current_price < high_allday * 0.95
    #                     and high_allday > pre_close_price * 1.05
    #                 ):
    #                     print("10.挣够25%，高位放量，请走！" + str(day_open_price))
    #                     order_target(stock_two, 0)
    #                 elif (
    #                     current_price < high_allday * 0.93
    #                     and high_allday > pre_close_price * 1.06
    #                     and time_sell > sell_day_10
    #                 ):
    #                     print("11.挣够25%，高位放量，请走！" + str(day_open_price))
    #                     order_target(stock_two, 0)
    #                 elif (
    #                     day_open_price < pre_close_price * 0.97
    #                     and current_price > pre_close_price
    #                 ):
    #                     print("10.挣够25%，高位放量，请走！" + str(day_open_price))
    #                     order_target(stock_two, 0)
    # if time_sell > cday and len(held_stock) > 0:
    #     instead_stock = held_stock
    #     for stock_remove in instead_stock:
    #         held_stock.remove(stock_remove)


def send_micromessage(result_in):
    # 调用send_message
    send_message(result_in)


# 选出连续涨停的数为count_num的个股
def _select_fix_number_continous_high_limit_security(securities, end_date, count_num=5):
    t1 = datetime.datetime.now()

    df_panel = get_price(
        securities,
        count=count_num + 1,
        end_date=end_date,
        frequency="daily",
        fields=["open", "close", "high_limit", "money", "pre_close"],
        panel=False,
        skip_paused=True,
    )

    all_high_limit_securities = []
    df_panel_all = df_panel[df_panel.close == df_panel.high_limit]
    df_grouped = df_panel_all.groupby("code").count()
    for code in df_grouped[df_grouped.close == count_num].index:
        all_high_limit_securities.append(code)

    # print(all_high_limit_securities)

    t2 = datetime.datetime.now()
    # print(t2-t1)

    selected_high_limit_securities = []
    for security in all_high_limit_securities:
        df_security = df_panel[df_panel["code"] == security]
        sum_plus_num = (
            df_security.loc[:, "close"] == df_security.loc[:, "high_limit"]
        ).sum()
        df_security = df_security.reset_index(drop=True)
        if (
            sum_plus_num == count_num
            and df_security.loc[0, "close"] != df_security.loc[0, "high_limit"]
        ):
            selected_high_limit_securities.append(security)
    t3 = datetime.datetime.now()
    # print(t3-t2)

    return selected_high_limit_securities


# def _select_fix_number_continous_high_limit_security(securities, end_date, count_num=5):
#     df_panel = get_price(
#         securities,
#         count=count_num + 1,
#         end_date=end_date,
#         frequency="daily",
#         fields=["open", "close", "high_limit", "money", "pre_close"],
#         panel=False,
#     )
#     high_limit_securities = []
#     for security in securities:
#         df_security = df_panel[df_panel["code"] == security]
#         sum_plus_num = (
#             df_security.loc[:, "close"] == df_security.loc[:, "high_limit"]
#         ).sum()
#         df_security = df_security.reset_index(drop=True)
#         if (
#             sum_plus_num == count_num
#             and df_security.loc[0, "close"] != df_security.loc[0, "high_limit"]
#         ):
#             high_limit_securities.append(security)
#     return high_limit_securities


##
def before_market_open(context):
    g.held_stock = []
    g.order_list = []
    date_now = (context.current_dt + timedelta(days=-1)).strftime(
        "%Y-%m-%d"
    )  # '2021-01-15'#datetime.datetime.now()
    date_30_days_ago = (context.current_dt + timedelta(days=-30)).strftime("%Y-%m-%d")
    trade_dates_30 = get_trade_days(
        start_date=date_30_days_ago, end_date=date_now, count=None
    )

    stocks = list(get_all_securities(["stock"]).index)
    filter_st_stock = filter_st(stocks)
    filtered_stocks = filter_stock_by_days(context, filter_st_stock, 180)
    filtered_stocks = filter_cyb_kcb(filtered_stocks)
    date_yesterday = trade_dates_30[trade_dates_30.size - 1]

    count_30_days_5_ban = 0
    count_10_days_5_ban = 0
    i = 0
    for cur_end_date in trade_dates_30[0 : trade_dates_30.size - 1]:
        cur_continuous_price_limit = _select_fix_number_continous_high_limit_security(
            filtered_stocks, cur_end_date
        )
        count_30_days_5_ban += len(cur_continuous_price_limit)
        if i >= trade_dates_30.size - 10:
            count_10_days_5_ban += len(cur_continuous_price_limit)

        i += 1

    count_30_days_6_ban = 0
    count_10_days_6_ban = 0
    i = 0
    for cur_end_date in trade_dates_30[1:]:
        cur_continuous_price_limit = _select_fix_number_continous_high_limit_security(
            filtered_stocks, cur_end_date, count_num=6
        )
        count_30_days_6_ban += len(cur_continuous_price_limit)

        if i >= trade_dates_30.size - 10:
            count_10_days_6_ban += len(cur_continuous_price_limit)

        i += 1

    continue_ratio_30 = (
        count_30_days_6_ban / float(count_30_days_5_ban)
        if count_30_days_5_ban > 0
        else 0
    )
    print(
        "前30天6板个数为: "
        + str(count_30_days_6_ban)
        + " 前30天5板个数为: "
        + str(count_30_days_5_ban)
        + "前30天5进6的概率为: "
        + str(continue_ratio_30)
    )

    continue_ratio_10 = (
        count_10_days_6_ban / float(count_10_days_5_ban)
        if count_10_days_5_ban > 0
        else 0
    )
    print(
        "前10天6板个数为: "
        + str(count_10_days_6_ban)
        + " 前10天5板个数为: "
        + str(count_10_days_5_ban)
        + "前10天5进6的概率为: "
        + str(continue_ratio_10)
    )

    if (
        # continue_ratio_30 >= 0.75
        # and continue_ratio_10 >= 0.8
        # and count_30_days_6_ban >= 5
        # and count_10_days_6_ban >= 3
        continue_ratio_30 >= 0.65
        and continue_ratio_10 >= 0.75
        and count_30_days_6_ban >= 13
        and count_10_days_6_ban >= 3
    ):
        # 选出5板的个股
        continuous_price_limit = _select_fix_number_continous_high_limit_security(
            filtered_stocks, date_yesterday
        )

        print("选出的连扳股票")
        print(continuous_price_limit)
        for stock in continuous_price_limit:
            count_limit = count_limit_num_all(stock, context)
            if count_limit <= 8:
                g.held_stock.append(stock)
        print("被选出的股票为:")
        print(g.held_stock)


##查看他总的涨停数
def count_limit_num_all(stock, context):
    date_yesterday = (context.current_dt + timedelta(days=-1)).strftime(
        "%Y-%m-%d"
    )  # '2021-01-15'#datetime.datetime.now()
    date_30_days_ago = (context.current_dt + timedelta(days=-30)).strftime("%Y-%m-%d")
    trade_dates = get_trade_days(
        start_date=date_30_days_ago, end_date=date_yesterday, count=None
    )
    limit_num = 0
    for cur_date in trade_dates:
        df_panel = get_price(
            stock,
            count=1,
            end_date=cur_date,
            frequency="daily",
            fields=["open", "close", "high_limit", "money", "pre_close"],
        )
        df_close = df_panel["close"].values
        df_high_limit = df_panel["high_limit"].values
        df_pre_close = df_panel["pre_close"].values
        if df_close == df_high_limit and df_close > df_pre_close:
            limit_num = limit_num + 1
    return limit_num


##去除st的股票
def filter_st(codelist):
    current_data = get_current_data()
    codelist = [code for code in codelist if not current_data[code].is_st]
    return codelist


##去除创业板科创板的股票
def filter_cyb_kcb(codelist):
    codelist = [
        code
        for code in codelist
        if not (code[0:3] == "300" or code[0:3] == "688" or code[0:3] == "689")
    ]
    return codelist


##过滤上市时间不满days天的股票
def filter_stock_by_days(context, stock_list, days):
    tmpList = []
    for stock in stock_list:
        days_public = (
            context.current_dt.date() - get_security_info(stock).start_date
        ).days
        if days_public > days:
            tmpList.append(stock)
    return tmpList


## 收盘后运行函数
def after_market_close(context):
    log.info(str("函数运行时间(after_market_close):" + str(context.current_dt.time())))
    # 得到当天所有成交记录
    trades = get_trades()
    for _trade in trades.values():
        log.info("成交记录：" + str(_trade))
    log.info("一天结束")
    log.info("##############################################################")
