# 负责交易业务相关，可以当做操作者
import datetime

from stocker.dbHelper import get_valid_date, get_code, get_data
from stocker.strategy import ma_divergency

is_data_prepared = False


def prepare_data(strategies):
    """
    根据策略读取足够的数据放到缓存备用，
    目前是获取目标市场的所有数据做缓存
    :param strategies:
    :return:
    """
    # 默认获取 2 年的数据
    default_datas_days = 365 * 2
    # 默认所有市场
    market = 'ALL'
    str_cur_date = datetime.datetime.now().strftime("%Y%m%d")
    cur_trade_date = datetime.datetime.strptime(str_cur_date, '%Y%m%d')
    # 获取有效的最后一个交易日
    screen_date = get_valid_date(cur_trade_date)

    # strategies = [市场,选股日期,[func,param],[func,param]]
    # 找出所有选股策略中最大的数据天数
    for row in strategies:
        # should be market param
        row_type = type(row)
        print(row_type)
        if isinstance(row, str):
            if row in ['SH', 'SZ', 'ALL']:
                market = row
        elif isinstance(row, datetime.datetime):
            screen_date = row
        elif isinstance(row, list):
            # should be screener function
            if row[0] == ma_divergency:
                # 获取均线最大的天数的数据,比如 60日均线，需要之前 60 天的收盘价做计算
                default_datas_days = row[1][-1] * 2
                start_date = screen_date - datetime.timedelta(days=default_datas_days)
                end_date = screen_date
        else:
            assert False
            print('发现未支持类型')
            return []

    codes = get_code(market)
    datas = []
    for code in codes:
        per_data = get_data(code, start_date, end_date)
        datas.append(per_data)
    is_data_prepared = True
    return datas


def screen_stock(strategies):
    """
    负责根据策略选股操作
    :return:
    """
    # 如果数据未准备好，根据选股策略，获取对应的数据
    if not is_data_prepared:
        datas = prepare_data(strategies)

    for row in strategies:
        print(row)
        if isinstance(row, list):
            row[0](datas, row[1])
