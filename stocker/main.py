from stocker import dbHelper
from stocker import ts

if __name__ == "__main__":
    print("tushare version:" + ts.__version__)
    # 每隔一段时间执行一次即可。先删除集合在插入
    # dbHelper.fetch_code()
    # 一年执行一次即可
    # dbHelper.fetch_calender()
    # 补全数据，每周补全一次
    dbHelper.fetch_data('000001.SZ', '20180701', '20180718')
