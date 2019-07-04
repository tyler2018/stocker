# 负责提供数据服务
import datetime
import time
from stocker import ts_pro
from stocker import pd
import pymongo
import requests

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
    data_arr = datas.to_dict('records')
    # 创建集合，相当于表
    t_codes = db['codes']
    t_codes.drop()
    # 插入获取的代码
    result = t_codes.insert_many(data_arr)
    if len(result.inserted_ids) != len(data_arr):
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
    t_calender.drop()
    dataArr = calender.to_dict('records')
    result = t_calender.insert_many(dataArr)
    if len(result.inserted_ids) != len(dataArr):
        assert False
        print('插入代码表数据失败.请检查。')
        return
    else:
        print("插入代码表数据成功")


def fetch_data():
    """
    从接口补全市场数据，存入数据库，返回结构样例
             ts_code trade_date  open  high   low  close  pre_close  change  pct_chg         vol       amount
    0   000001.SZ   20180718  8.75  8.85  8.69   8.70       8.72   -0.02    -0.23   525152.77   460697.377
    1   000001.SZ   20180717  8.74  8.75  8.66   8.72       8.73   -0.01    -0.11   375356.33   326396.994
    2   000001.SZ   20180716  8.85  8.90  8.69   8.73       8.88   -0.15    -1.69   689845.58   603427.713
    3   000001.SZ   20180713  8.92  8.94  8.82   8.88       8.88    0.00     0.00   603378.21   535401.175
    """
    codes = get_code('ALL')
    # 获取当天的日期，去掉时间
    str_cur_date = datetime.datetime.now().strftime("%Y%m%d")
    cur_trade_date = datetime.datetime.strptime(str_cur_date, '%Y%m%d')
    # 获取有效的上一个交易日
    cur_trade_date = get_valid_date(cur_trade_date)
    # 访问接口计数器，避免超时发生
    timeout_count = 0
    for row in codes:
        print(row)
        # 默认获取数据间隔 3 年
        fetch_data_interval = 3
        # 每个代码创建一个集合
        t_codes = db[row['ts_code']]
        row_cnt = t_codes.count_documents({}, limit=1)
        # 获取起始日期
        if row_cnt:
            results = t_codes.find().sort('trade_date', pymongo.DESCENDING).limit(1)
            start_trade_date = datetime.datetime.strptime(results[0]['trade_date'], '%Y%m%d')
            start_trade_date = start_trade_date + datetime.timedelta(days=1)
            # 获取有效的上一个交易日
            start_trade_date = get_valid_date(start_trade_date)
            print(row['ts_code'] + ' has data exist before: ' + start_trade_date.strftime('%Y%m%d'))
        else:
            # 没有数据存在从上市第一天开始获取数据
            print(row['ts_code'] + ' has no data exist')
            start_trade_date = datetime.datetime.strptime(row['list_date'], '%Y%m%d')

        end_date = start_trade_date + datetime.timedelta(days=365 * fetch_data_interval)
        if end_date >= cur_trade_date:
            end_date = cur_trade_date

        while start_trade_date < end_date:
            try:
                datas = ts_pro.daily(ts_code=row['ts_code'], start_date=start_trade_date.strftime('%Y%m%d'),
                                     end_date=end_date.strftime('%Y%m%d'))
                timeout_count = timeout_count + 1
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                print(repr(e))
                # 不知道为什么偶尔会超时，这里暂停 1 后重试
                time.sleep(1)
                continue
            except Exception as e:
                # 未知异常
                print('发现未知异常：' + repr(e))
                time.sleep(1)
                continue
            if not datas.empty:
                datas.sort_values('trade_date', inplace=True)
                data_arr = datas.to_dict('records')
                # 插入获取的代码
                result = t_codes.insert_many(data_arr)
                if len(result.inserted_ids) != len(data_arr):
                    assert False
                    print('插入日线数据失败.请检查。')
            else:
                # 有的股票一直停牌没有数据，放大到 4 年间隔
                fetch_data_interval = 4

            time.sleep(0.5)
            # 调整日期
            start_trade_date = end_date + datetime.timedelta(days=1)
            end_date = start_trade_date + datetime.timedelta(days=365 * fetch_data_interval)
            if end_date >= cur_trade_date:
                end_date = cur_trade_date

    print('市场数据补全完毕')


def get_valid_date(base_date):
    """
    根据参数向前找出合法的交易日，跳过节假日，休息日
    :return:
    """
    pre_base_date = base_date - datetime.timedelta(days=10)
    t_calender = db['calender']
    result = t_calender.find(
        {'cal_date': {'$gte': pre_base_date.strftime('%Y%m%d'),
                      '$lte': base_date.strftime('%Y%m%d')}}).sort('cal_date',
                                                                   pymongo.DESCENDING)
    for value in result:
        if value['is_open']:
            return datetime.datetime.strptime(value['cal_date'], '%Y%m%d')
    print('Warning: 未找到有效交易日期')
    return base_date


def get_code(market):
    """
     获取股票代码表
    market [in] string
        SH 上海,SZ 深圳,ALL 所有市场
    output:
        {'ts_code': '000001.SZ', 'symbol': '000001', 'name': '平安银行', 'industry': '银行', 'list_date': '19910403'}
    :return:
    """
    # 打开代码表集合
    t_codes = db['codes']
    if market == 'ALL':
        codes = list(t_codes.find({}, {"_id": 0, "ts_code": 1, "symbol": 1, "name": 1, "industry": 1, "list_date": 1}))
    else:
        codes = list(t_codes.find({"ts_code": {"$regex": ".{}".format(market)}},
                                  {"_id": 0, "ts_code": 1, "symbol": 1, "name": 1, "industry": 1, "list_date": 1}))
    return codes


def get_data(code, start_date, end_date):
    """
    获取股票数据
    :param code:
    :param start_date:
    :param end_date:
    :return:
    """
    t_data = db[code['ts_code']]
    cursor_result = t_data.find(
        {'ts_code': code['ts_code'], 'trade_date': {'$gte': start_date.strftime('%Y%m%d'),
                                                    '$lte': end_date.strftime('%Y%m%d')}})
    labels = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']
    df = pd.DataFrame(list(cursor_result), columns=labels)
    return df
