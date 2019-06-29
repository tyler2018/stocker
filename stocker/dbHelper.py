# 负责提供数据服务

from stocker import ts_pro
from stocker import pd
import pymongo

conn = pymongo.MongoClient('127.0.0.1', 27017)
# database name is stock
db = conn['stock']


def fetch_code():
    """
    从接口获取代码，存入数据库，返回结构样例
             ts_code  symbol  name      area industry list_date
        0     000001.SZ  000001  平安银行   深圳       银行  19910403
        1     000002.SZ  000002   万科A   深圳     全国地产  19910129
        3541  603936.SH  603936  博敏电子   广东      元器件  20151209
        3542  603937.SH  603937  丽岛新材   江苏        铝  20171102
    :return:
    """
    # 查询当前所有正常上市交易的股票列表
    datas = ts_pro.query('stock_basic', exchange='', list_status='L',
                         fields='ts_code,symbol,name,area,industry,list_date')
    dataArr = datas.to_dict('records')
    # 创建集合，相当于表
    t_codes = db['codes']
    t_codes.drop()
    # 插入获取的代码
    result = t_codes.insert_many(dataArr)
    if len(result.inserted_ids) != len(dataArr):
        assert False
        print('插入代码表数据失败.请检查。')
        return
    else:
        print("插入代码表数据成功")


def fetch_calender():
    """
        从接口获取有效交易日信息，存入数据库，返回结构样例
                  exchange  cal_date  is_open
        0           SSE  20180101        0
        1           SSE  20180102        1
        2           SSE  20180103        1
    """
    calender = ts_pro.trade_cal(start_date='20190101', end_date='20191231')
    # 创建集合，相当于表
    t_calender = db['calender']
    dataArr = calender.to_dict('records')
    result = t_calender.insert_many(dataArr)
    if len(result.inserted_ids) != len(dataArr):
        assert False
        print('插入代码表数据失败.请检查。')
        return
    else:
        print("插入代码表数据成功")


def fetch_data(code, start_date, end_date):
    """
    code: 股票代码
    start_date: 起始日期
    end_date: 结束日期
    返回包括首尾日期的数据。

    从接口补全市场数据，存入数据库，返回结构样例
             ts_code trade_date  open  high   low  close  pre_close  change  pct_chg         vol       amount
    0   000001.SZ   20180718  8.75  8.85  8.69   8.70       8.72   -0.02    -0.23   525152.77   460697.377
    1   000001.SZ   20180717  8.74  8.75  8.66   8.72       8.73   -0.01    -0.11   375356.33   326396.994
    2   000001.SZ   20180716  8.85  8.90  8.69   8.73       8.88   -0.15    -1.69   689845.58   603427.713
    3   000001.SZ   20180713  8.92  8.94  8.82   8.88       8.88    0.00     0.00   603378.21   535401.175
    """
    df = ts_pro.daily(ts_code='000001.SZ', start_date='20180701', end_date='20180718')
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', 500,
                           'display.width', 5000):
        print(df)
    pass
