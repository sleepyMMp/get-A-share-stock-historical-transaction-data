from pytdx.exhq import *   # pip install pytdx
from pytdx.hq import *
from collections import defaultdict
import datetime
from datetime import datetime as dt
import csv
from pprint import pprint


api_hq = TdxHq_API()
api_hq = api_hq.connect('119.147.212.81', 7709)

def get_all_trans_data(api, code, date):
    start = 0
    data = []
    while True:
        part = api.get_history_transaction_data(TDXParams.MARKET_SZ, code, start, 888, int(date))
        data.extend(part)
        if len(part) < 888:
            break
        start += 888
    return data

def get_dates_as_int(sy,sm,sd,ey,em,ed): #start_year, start_month, start_day, end_year, end_month, end_day: int
    dates = []
    begin = datetime.date(sy,sm,sd)
    end = datetime.date(ey,em,ed)
    for i in range((end - begin).days+1):
        day = begin + datetime.timedelta(days=i)
        day = dt.strftime(day, "%Y%m%d")
        dates.append(day)
    return dates


if __name__ == "__main__":
    # date = 20230523  #must be int like this
    dates = get_dates_as_int(2023,5,26,2023,6,16) #start_year: int, start_month: int, start_day: int, end_year: int, end_month: int, end_day: int
    stock_code = "000625"
    for date in dates:
        # data = get_all_trans_data(api_hq, stock_code, date)
        data = api_hq.get_history_minute_time_data(TDXParams.MARKET_SZ,stock_code,date)
        # pprint(data)

        ## save as csv
        dict_data = []
        for item in data:
            dict_data.append({"price":item["price"], "vol":item["vol"]})

        csv_columns = ["price", "vol"]
        csv_file = stock_code + "_" + str(date) + ".csv"
        try:
            with open(csv_file, 'w', newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                for data in dict_data:
                    writer.writerow(data)
        except IOError:
            print("I/O error")