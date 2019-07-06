import os
import time

from stocker import ts, pd
from stocker.dbHelper import get_last_valid_trade_date
from stocker.strategy import ma_divergency, close_filter, name_filter
from stocker.trader import screen_stock

if __name__ == "__main__":
    print("tushare version:" + ts.__version__)
    # 每隔一段时间执行一次即可。先删除集合在插入
    # dbHelper.fetch_code()
    # 一年执行一次即可
    # dbHelper.fetch_calender()
    # 补全数据，每周补全一次
    # dbHelper.fetch_data()
    # 创建索引，不删除集合的情况下只用执行一次。
    # dbHelper.create_index()
    # 选股操作 ===========================

    # 设置选股日期为最后一个有效交易日
    valid_screen_date = get_last_valid_trade_date()
    # [[func,param],[func,param]]
    strategies = [[name_filter, ['ST']], [close_filter, 25], [ma_divergency, [10, 25, 43, 60]]]
    # 市场,选股日期,[[func,param],[func,param]]
    pickup_list = screen_stock('ALL', valid_screen_date, strategies)
    print('选股策略执行完毕')
    # 获得当前时间
    otherStyleTime = time.strftime("%Y-%m-%d %H_%M_%S", time.localtime())
    df = pd.DataFrame(pickup_list, columns=['code', 'name'])
    curPath = os.path.split(os.path.realpath(__file__))[0]
    df.to_csv(curPath + '/' + otherStyleTime + '_maDivergency.csv', index=False, encoding='utf_8_sig')
