import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
import sys
sys.path.append("D:\\Program Files\\Tinysoft\\Analyse.NET")
import TSLPy3 as ts
from jinja2 import Template
from dateutil.parser import parse as dateparse
import os


class TsTickData(object):

    def __enter__(self):
        if ts.Logined() is False:
            print('天软未登陆或客户端未打开，将执行登陆操作')
            self.__tsLogin()
            return self

    def __tsLogin(self):
        ts.ConnectServer("tsl.tinysoft.com.cn", 443)
        dl = ts.LoginServer("fzzqjyb", "fz123456")
        print('天软登陆成功')

    def __exit__(self, *arg):
        ts.Disconnect()
        print('天软连接断开')

    def ticks(self, code, start_date, end_date):
        ts_template = Template('''begT:= StrToDate('{{start_date}}');
                                  endT:= StrToDate('{{end_date}}');
                                  setsysparam(pn_cycle(),cy_1s());
                                  setsysparam(pn_rate(),0);
                                  setsysparam(pn_RateDay(),rd_lastday);
                                  r:= select  ["StockID"] as 'ticker', datetimetostr(["date"]) as "time", ["price"]
                                      from markettable datekey begT to endT of "{{code}}" end;
                                  return r;''')
        ts_sql = ts_template.render(start_date=dateparse(start_date).strftime('%Y-%m-%d'),
                                    end_date=dateparse(end_date).strftime('%Y-%m-%d'),
                                    code=code)

        fail, data, _ = ts.RemoteExecute(ts_sql, {})

        def gbk_decode(strlike):
            if isinstance(strlike, (str, bytes)):
                strlike = strlike.decode('gbk')
            return strlike

        def bytes_to_unicode(record):
            return dict(map(lambda s: (gbk_decode(s[0]), gbk_decode(s[1])), record.items()))

        if not fail:
            unicode_data = list(map(bytes_to_unicode, data))
            return pd.DataFrame(unicode_data).set_index(['time', 'ticker'])
        else:
            raise Exception("Error when execute tsl")


if __name__ == "__main__":
    date = "20191101"
    end_date = "20191102"
    df = pd.read_excel("当日委托201911011658016.xlsx", encoding="gbk")
    df.sort_values(by="时间", inplace=True)
    df["ticker"] = df["代码/名称"].apply(lambda s: "SH" + s[:6] if s.startswith('6') else "SZ" + s[:6])
    df["name"] = df["代码/名称"].apply(lambda s: s[7:])
    df.columns = ["time", "ticker/name", "direction", "quantity", "price", "status", "ticker", "name"]
    df = df[["time", "ticker", "name", "price", "quantity", "direction", "status"]]
    try:
        df["time"] = df["time"].apply(lambda s: str(dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").time()))
        print("This is historical order format.")
    except ValueError:
        df["time"] = df["time"].apply(lambda s: str(dt.datetime.strptime(s, "%H:%M:%S").time()))
        print("This is today's order format.")
    ticker_set = set(sorted(df["ticker"].tolist()))
    i = 0
    try:
        os.makedirs("pictures_" + date +'/')
    except FileExistsError:
        pass
    for ticker in ticker_set:
        with TsTickData() as obj:
            data = obj.ticks(code=ticker, start_date=date, end_date=end_date)
        data["index"] = data.index
        data["time"] = data["index"].apply(lambda tu: tu[0][-8:])
        data = data[(data["time"] <= "11:30:00") | (data["time"] >= "13:00:00")]
        data["time_offset"] = list(range(data.shape[0]))
        plt.figure(figsize=(20, 10))
        plt.plot(data["time_offset"], data["price"], color="gray", alpha=0.5)
        sub_df = df[df["ticker"] == ticker]
        sub_df["pct"] = round(sub_df["quantity"] / sub_df["quantity"].sum() * 100).astype(int)
        for key, record in sub_df.iterrows():
            if (record["time"] >= "11:30:00" and record["time"] <= "13:00:00") or record["time"] < "09:30:00" or record["time"] > "15:00:00":
                print("Entrust order at illegal time!!!")
                continue
            if record["direction"] == "买入" and record["status"] == "已成":
                marker = 'or'
            elif record["direction"] == "卖出" and record["status"] == "已成":
                marker = 'sg'
            elif record["direction"] == "买入" and record["status"] == "已撤":
                marker = '*r'
            elif record["direction"] == "卖出" and record["status"] == "已撤":
                marker = 'xg'
            time_offset = data[data["time"] == record["time"]]["time_offset"].squeeze()
            try:
                plt.plot([time_offset,], [record["price"],], marker, markersize=6)
            except ZeroDivisionError:
                print("integer division or modulo by zero")
                print(time_offset)
                print(record["price"])
            plt.text(time_offset, record["price"] + 0.01, str(record["pct"]) +'%')
        xticks = list(range(14401))[::1800]
        xticklabels = ["9:30", "10:00", "10:30", "11:00", "11:30/13:00", "13:30", "14:00", "14:30", "15:00"]
        plt.xticks(xticks, xticklabels)
        plt.title(ticker + " | " + date, fontsize=25)
        plt.savefig("pictures_" + date +'/' + ticker + '_' + date + '.png')
        plt.close()
        i += 1
        print("Process " + str(i) + " stocks")