import talib as ta


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
            datas['ma%d' % ma] = ta.SMA(datas['close'].values, ma)
    except Exception as e:
        print(e)
    return datas


def ma_divergency(data, ma_list):
    """
    基础条件：5, 10, 25, 43, 60日均线发散，多头排列，选股当日有交易（过滤掉停牌的），不考虑收盘价格的大小

    :param data: 股票数据的 dataframe
    :param ma_list:
    :return: 符合条件的列表 [name,code]
    """
    datas_ma = calc_ma(data, ma_list)
    ma_val_cur = 0
    for ma in ma_list[::-1]:
        # check whether has ma column
        if 'ma{}'.format(ma) not in data:
            print('{} {}数据不足，跳过'.format(data['ts_code'], data['name']))
            continue
        # print('close{}:{:.2f}'.format(ma, datas_ma['ma%d' % ma].tolist()[-1]))
        if not ma_val_cur:
            ma_val_cur = datas_ma['ma%d' % ma].tolist()[-1]
        else:
            if ma_val_cur < datas_ma['ma%d' % ma].tolist()[-1]:
                # 符合条件，继续判断
                ma_val_cur = datas_ma['ma%d' % ma].tolist()[-1]
            else:
                # 不符合条件，退出循环
                return None
    return [data['ts_code'].tolist()[0], data['name'].tolist()[0]]


def close_filter(data, params):
    """
    选出收盘价大于等于指定参数的股票
    :param data:
    :param params:
    :return:
    """
    if data['close'].tolist()[-1] >= params:
        return [data['ts_code'].tolist()[0], data['name'].tolist()[0]]
    else:
        return None


def name_filter(data, name_list):
    """
    过滤掉符合条件的股票：根据名字过滤，比如ST
    :param data:
    :param name_list:
    :return:
    """
    print(type(data))
    for name in name_list:
        if data['name'].find(name):
            return True
    return False
