# -*- coding:utf-8 -*-
"""
Date: 2023/09/22
Desc: 上交所
"""
import json
import time
import requests
import pandas as pd


class SSE:

    def __init__(self) -> None:
        self.stock = Stock()
        self.bond = Bond()
        self.fund = Fund()
        self.index = Index()
        self.option = Option()
        self.repo = Repo()


class Stock:

    def __init__(self):
        self.stock_list_url = "http://query.sse.com.cn//sseQuery/commonExcelDd.do?sqlId=COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L&type=inParams&CSRC_CODE=&STOCK_CODE=&REG_PROVINCE=&STOCK_TYPE={stock_type}&COMPANY_STATUS=2,4,5,7,8"
        self.delist_list_url = "http://query.sse.com.cn//sseQuery/commonExcelDd.do?sqlId=COMMON_SSE_CP_GPJCTPZ_GPLB_ZZGP_L&type=inParams&CSRC_CODE=&STOCK_CODE=&REG_PROVINCE=&STOCK_TYPE={stock_type}&COMPANY_STATUS=3"
        self.stock_eod_url = "http://yunhq.sse.com.cn:32041/v1/sh1/dayk/{stock}?callback=jQuery112409826351965297482_{t}&begin=0&end=-1&period=day&_={t}"

        self.eod_url = "http://yunhq.sse.com.cn:32041/v1/sh1/list/exchange/equity?callback=jsonpCallback62585600&select=code%2Copen%2Chigh%2Clow%2Clast%2Cvolume%2Camount%2C&order=&begin=0&end=-1&_={t}"

        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Referer": "http://www.sse.com.cn/"
        }

    def get_stock_list(self) -> pd.DataFrame:
        stock_list = pd.DataFrame()
        for stock_type in ["1", "8"]:
            r = requests.get(url=self.stock_list_url.format(stock_type=stock_type), headers=self.headers, timeout=10)
            tmp = pd.read_excel(r.content)
            stock_list = pd.concat([stock_list, tmp])
        stock_list.set_index("A股代码", inplace=True)
        return stock_list

    def get_delist(self) -> pd.DataFrame:
        delist = pd.DataFrame()
        for stock_type in ["1", "8"]:
            r = requests.get(url=self.delist_list_url.format(stock_type=stock_type), headers=self.headers, timeout=10)
            tmp = pd.read_excel(r.content)
            delist = pd.concat([delist, tmp])
        delist.set_index("原公司代码", inplace=True)
        return delist

    def get_single_stock_eod(self, stock: str, date: str = "all") -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.stock_eod_url.format(t=t, stock=stock), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(", 1)[1][:-1])
        eod = pd.DataFrame(data["kline"], columns=["TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"])
        eod["InstrumentID"] = stock
        eod = eod[self.eod_columns]
        if date != "all":
            eod = eod[eod["TradingDay"] == int(date)]
        return eod

    def get_eod_history(self) -> pd.DataFrame:
        stock_list = self.get_stock_list()
        delist = self.get_delist()
        _all = list(stock_list.index) + list(delist.index)
        eod = pd.DataFrame()
        for stock in _all:
            retry = 0
            df = pd.DataFrame()
            while retry < 3:
                try:
                    df = self.get_single_stock_eod(stock)
                    break
                except:
                    retry += 1
                    continue
            eod = pd.concat([eod, df])
        return eod

    # ongoing
    def get_eod(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.eod_url.format(t=t), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(")[1][:-1])
        eod = pd.DataFrame(data["list"], columns=["InstrumentID", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"])
        eod["TradingDay"] = data["date"]
        eod = eod[self.eod_columns]
        return eod


class Bond:

    def __init__(self):
        self.bond_list_url = "http://query.sse.com.cn/sseQuery/commonSoaQuery.do?jsonCallBack=jsonpCallback64754136&sqlId=CP_ZQ_ZQLB&BOND_TYPE=%E5%85%A8%E9%83%A8&_={t}"
        self.bond_eod_url = "http://yunhq.sse.com.cn:32041/v1/shb1/dayk/{bond}?callback=jQuery112402788803963488542_{t}&begin=0&end=-1&period=day&_={t}"

        self.eod_url = "http://yunhq.sse.com.cn:32041/v1/shb1/list/exchange/all?callback=jsonpCallback62585600&select=code%2Copen%2Chigh%2Clow%2Clast%2Cvolume%2Camount%2C&order=&begin=0&end=-1&_={t}"

        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Referer": "http://www.sse.com.cn/"
        }

    def get_bond_list(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.bond_list_url.format(t=t), headers=self.headers, timeout=30)
        data = json.loads(r.text.split("(", 1)[1][:-1])
        bond_list = pd.DataFrame(data["result"])
        bond_list.set_index("BOND_CODE", inplace=True)
        return bond_list

    def get_single_bond_eod(self, bond: str, date: str = "all") -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.bond_eod_url.format(t=t, bond=bond), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(")[1][:-1])
        eod = pd.DataFrame(data["kline"], columns=["TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"])
        eod["InstrumentID"] = bond
        eod = eod[self.eod_columns]
        if date != "all":
            eod = eod[eod["TradingDay"] == int(date)]
        return eod

    def get_eod_history(self) -> pd.DataFrame:
        bond_list = self.get_bond_list()
        eod = pd.DataFrame()
        for bond in bond_list.index:
            retry = 0
            df = pd.DataFrame()
            while retry < 3:
                try:
                    df = self.get_single_bond_eod(bond)
                    break
                except IndexError:
                    break
                except Exception:
                    retry += 1
                    continue
            eod = pd.concat([eod, df])
        return eod

    # ongoing
    def get_eod(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.eod_url.format(t=t), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(")[1][:-1])
        eod = pd.DataFrame(data["list"], columns=["InstrumentID", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"])
        eod["TradingDay"] = data["date"]
        eod = eod[self.eod_columns]
        return eod


class Fund:

    def __init__(self):
        self.fund_list_url = "http://query.sse.com.cn/commonSoaQuery.do?jsonCallBack=jsonpCallback14059342&sqlId=FUND_LIST&fundType=00%2C10%2C20%2C30%2C40%2C50%2C&order=&_={t}"
        self.fund_eod_url = "http://yunhq.sse.com.cn:32041/v1/sh1/dayk/{fund}?callback=jQuery112409826351965297482_{t}&begin=0&end=-1&period=day&_={t}"

        self.eod_url = "http://yunhq.sse.com.cn:32041/v1/sh1/list/exchange/fwr?callback=jsonpCallback87559922&select=code%2Copen%2Chigh%2Clow%2Clast%2Cvolume%2Camount%2C&order=&begin=0&end=-1&_={t}"

        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Referer": "http://www.sse.com.cn/"
        }

    def get_fund_list(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(url=self.fund_list_url.format(t=t), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(", 1)[1][:-1])
        fund_list = pd.DataFrame(data["result"])
        fund_list.set_index("fundCode", inplace=True)
        return fund_list

    def get_single_fund_eod(self, fund: str, date: str = "all") -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.fund_eod_url.format(t=t, fund=fund), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(")[1][:-1])
        eod = pd.DataFrame(data["kline"], columns=["TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"])
        eod["InstrumentID"] = fund
        eod = eod[self.eod_columns]
        if date != "all":
            eod = eod[eod["TradingDay"] == int(date)]
        return eod

    def get_eod_history(self) -> pd.DataFrame:
        fund_list = self.get_fund_list()
        eod = pd.DataFrame()
        for fund in fund_list.index:
            df = pd.DataFrame()
            retry = 0
            while retry < 3:
                try:
                    df = self.get_single_fund_eod(fund)
                    break
                except:
                    retry += 1
                    continue
            eod = pd.concat([eod, df])
        return eod

    # ongoing
    def get_eod(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.eod_url.format(t=t), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(")[1][:-1])
        eod = pd.DataFrame(data["list"], columns=["InstrumentID", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"])
        eod["TradingDay"] = data["date"]
        eod = eod[self.eod_columns]
        return eod


class Index:

    def __init__(self):

        self.eod_url = "http://yunhq.sse.com.cn:32041/v1/sh1/list/exchange/index?callback=jsonpCallback54049231&select=code%2Cprev_close%2Copen%2Chigh%2Clow%2Clast%2Cvolume%2Camount%2C&order=&begin=0&end=-1&_={t}"

        self.eod_columns = ["InstrumentID", "TradingDay", "PreClosePrice", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Referer": "http://www.sse.com.cn/"
        }

    # ongoing
    def get_eod(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.eod_url.format(t=t), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(")[1][:-1])
        eod = pd.DataFrame(
            data["list"], columns=["InstrumentID", "PreClosePrice", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]
        )
        eod["TradingDay"] = data["date"]
        eod = eod[self.eod_columns]
        return eod


class Option:

    def __init__(self):
        self.ref_url = "http://query.sse.com.cn/commonQuery.do?jsonCallBack=jsonpCallback25531396&sqlId=SSE_ZQPZ_YSP_GGQQZSXT_XXPL_DRHY_SEARCH_L&_={t}"
        self.eod_url = "http://yunhq.sse.com.cn:32041/v1/sho/list/tstyle/{underlying}_{expiremonth}?callback=jsonpCallback65983501&select=contractid%2Clast%2Cpresetpx%2C&order=contractid%2C&_={t}"
        self.expiremonth_url = "http://yunhq.sse.com.cn:32041/v1/sho/list/exchange/stockexpire?callback=jsonpCallback12956269&select=stockid%2Cexpiremonth&_={t}"

        self.eod_columns = ["InstrumentID", "TradingDay", "SettlePrice", "PreSettlePrice"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Referer": "http://www.sse.com.cn/"
        }

    def get_ref(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(url=self.ref_url.format(t=t), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(", 1)[1][:-1])
        ref = pd.DataFrame(data["result"])
        ref.set_index("SECURITY_ID", inplace=True)
        return ref

    def get_underlying_expiremonth(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.expiremonth_url.format(t=t), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(")[1][:-1])
        ue = pd.DataFrame(data["list"], columns=["Underlying", "ExpireMonth"])
        ue["ExpireMonth"] = ue["ExpireMonth"].map(lambda x: str(x)[-2:])
        return ue

    # ongoing
    def get_eod(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        ue = self.get_underlying_expiremonth()
        eod = pd.DataFrame()
        for _, row in ue.iterrows():
            r = requests.get(self.eod_url.format(underlying=row["Underlying"], expiremonth=row["ExpireMonth"], t=t), headers=self.headers, timeout=10)
            data = json.loads(r.text.split("(", 1)[1][:-1])
            df = pd.DataFrame(data["list"], columns=["InstrumentID", "SettlePrice", "PreSettlePrice"])
            df["TradingDay"] = data["date"]
            df = df[self.eod_columns]
            eod = pd.concat([eod, df])
        return eod


class Repo:

    def __init__(self):
        self.repo_list_url = "http://query.sse.com.cn/commonQuery.do?jsonCallBack=jsonpCallback68949527&isPagination=true&pageHelp.pageSize=1000&pageHelp.pageNo=1&sqlId=COMMON_SSE_ZQPZ_ZQLB_ZQHGLB_TOTAL&_={t}"

        self.eod_url = "http://yunhq.sse.com.cn:32041/v1/shb1/dayk/{repo}?callback=jQuery11240494446136766604_{t}&begin=0&end=-1&period=day&_={t}"

        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Referer": "http://www.sse.com.cn/"
        }

    def get_repo_list(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(url=self.repo_list_url.format(t=t), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(", 1)[1][:-1])
        repo_list = pd.DataFrame(data["result"])
        repo_list.set_index("BOND_ID", inplace=True)
        return repo_list

    def get_single_repo_eod(self, repo: str, date: str = "all") -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.eod_url.format(t=t, repo=repo), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(")[1][:-1])
        eod = pd.DataFrame(data["kline"], columns=["TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"])
        eod["InstrumentID"] = repo
        eod = eod[self.eod_columns]
        if date != "all":
            eod = eod[eod["TradingDay"] == int(date)]
        return eod

    def get_eod(self, date: str = "all") -> pd.DataFrame:
        repo_list = self.get_repo_list()
        eod = pd.DataFrame()
        for repo in repo_list.index:
            retry = 0
            df = pd.DataFrame()
            while retry < 3:
                try:
                    df = self.get_single_repo_eod(repo, date)
                    break
                except IndexError:
                    break
                except Exception:
                    retry += 1
                    continue
            eod = pd.concat([eod, df])
        return eod


if __name__ == "__main__":
    sse = SSE()
    print(sse.bond.get_eod())
