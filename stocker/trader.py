# 负责交易业务相关，可以当做操作者
import datetime
import time

from stocker.dbHelper import get_previouse_valid_date, get_code, get_data
from stocker.strategy import ma_divergency, name_filter

g_is_data_prepared = False


def prepare_data(market, screen_date, strategies):
    """
    根据策略读取足够的数据放到缓存备用，
    目前是获取目标市场的所有数据做缓存
    :param market:
    :param screen_date:
    :param strategies:
    :return:
    """
    # strategies = [[func,param],[func,param]]
    # 找出所有选股策略中最大的数据天数
    # 如果有过滤股票名的策略，加入列表，后面使用
    filter_name = []
    for row in strategies:
        if row[0] == name_filter:
            filter_name = row[1]
        # should be screener function
        elif row[0] == ma_divergency:
            # 获取均线最大的天数的数据,比如 60日均线，需要之前 60 天的收盘价做计算
            default_datas_days = row[1][-1] * 2
            start_date = screen_date - datetime.timedelta(days=default_datas_days)
            end_date = screen_date

    codes = get_code(market)
    datas = []
    bSkip = False
    for code in codes:
        # 股票名称过滤策略
        for name in filter_name:
            if -1 != code['name'].find(name):
                bSkip = True
                # print('name_filter: skip {}'.format(code['name']))
                break
        if bSkip:
            bSkip = False
            continue
        per_data = get_data(code, start_date, end_date)
        if per_data.empty:
            print('{} data empty between {} - {}'.format(code, start_date, end_date))
            continue
        datas.append(per_data)
    g_is_data_prepared = True
    return datas


def screen_stock(market, screen_date, strategies):
    """
    负责根据策略选股操作
    :param market: 选股的市场，目前支持 ALL，SH，SZ 三种
    :param screen_date: 指定选股日期
    :param strategies: 指定选股策略
    :return:
    """
    # 如果数据为准备好，根据选股策略，获取对应的数据
    if not g_is_data_prepared:
        # 20秒
        # start = time.clock()
        datas = prepare_data(market, screen_date, strategies)
        # end = time.clock()
        # print('CPU执行时间: ', end - start)

    pickup_list = []
    for data in datas:
        bFound = True
        for row in strategies:
            # 股票名称过滤策略在获取数据的时候应用，这里不需要执行
            if row[0] == name_filter:
                continue
            # print(row)
            pickup = row[0](data, row[1])
            if pickup is None:
                bFound = False
                break
        if bFound:
            # print(pickup)
            pickup_list.append(pickup)
    return pickup_list
