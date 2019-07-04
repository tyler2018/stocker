
def calc_ma(datas, ma_list):
    """
    根据 ma_list 计算 ma 数值
    calc
    :param datas: dataframe
    :param ma_list: string list
    :return:
    """
    try:
        for ma in ma_list:
            datas['ma' + ma] = ta.SMA(datas['close'].values, int(ma))
    except Exception as e:
        print(e)
    return datas


def ma_divergency(datas, ma_list):
    """
    基础条件是必须满足的条件。
    附加条件是为了优化而添加的，从后往前逐个过滤，如果选出来的很少，就去掉最后面的过滤条件，
    例如：去掉60线向上，去掉25日线在60日线上面
    基础条件：5, 10, 25, 43, 60
    均线发散，多头排列，选股当日有交易（过滤掉停牌的）
    附加条件：25
    日线在60日均线上面，60
    日线向上

    :param datas:
    :param ma_list:
    :return:
    """
    for data in datas:
        print(data)
        data_ma = calc_ma(data, ma_list)
    print("call in")
    pass


def close_filter(datas, threshold):
    """

    :param datas:
    :param threshold:
    :return:
    """
    print("call in")
    pass