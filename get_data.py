from pytdx.exhq import *   # pip install pytdx
from pytdx.hq import *
from collections import defaultdict
import datetime
from datetime import datetime as dt
import csv


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
    dates = get_dates_as_int(2018,1,1,2023,5,22) #start_year: int, start_month: int, start_day: int, end_year: int, end_month: int, end_day: int
    stock_code = "000625"
    for date in dates:
        data = get_all_trans_data(api_hq, stock_code, date)

        trans = defaultdict(list)
        for tran in data:
            "%Y%m%d %H:%M"
            trans[datetime.datetime.strptime(str(date) + " " + tran["time"], "%Y%m%d %H:%M")].append({
                "price": tran["price"],
                "volume": tran["vol"],
                "turnover": float(tran["price"]) * float(tran["vol"]) * 100,
            })
        trans = dict(sorted(trans.items(), key=lambda x: x[0]))

        ## save as csv
        dict_data = []
        for k, v in trans.items():
            for item in v:
                dict_data.append({"time":k, "price":item["price"], "vol":item["volume"], "turnover":item["turnover"]})

        csv_columns = ["time", "price", "vol", "turnover"]
        csv_file = stock_code + "_" + str(date) + ".csv"
        try:
            with open(csv_file, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                for data in dict_data:
                    writer.writerow(data)
        except IOError:
            print("I/O error")