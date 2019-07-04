from stocker import ts
from stocker.dbHelper import get_valid_date
from stocker.strategy import ma_divergency, close_filter
from stocker.trader import screen_stock
import datetime

if __name__ == "__main__":
    print("tushare version:" + ts.__version__)
    # 每隔一段时间执行一次即可。先删除集合在插入
    # dbHelper.fetch_code()
    # 一年执行一次即可
    # dbHelper.fetch_calender()
    # 补全数据，每周补全一次
    # dbHelper.fetch_data()
    # 选股操作 ===========================
    # [市场,选股日期,[func,param],[func,param]]
    # 设置选股日期为最后一个有效交易日
    # 获取当天的日期，去掉时间
    str_cur_date = datetime.datetime.now().strftime("%Y%m%d")
    cur_trade_date = datetime.datetime.strptime(str_cur_date, '%Y%m%d')
    # 获取有效的最后一个交易日
    cur_trade_date = get_valid_date(cur_trade_date)
    screen_date = cur_trade_date
    strategies = ['ALL', screen_date, [ma_divergency, [10, 25, 43, 60]], [close_filter, [0, 25]]]

    screen_stock(strategies)
