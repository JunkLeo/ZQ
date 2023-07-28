# -*- coding:utf-8 -*-
"""
Date: 2023/07/28
Desc: 上期所每日REF/EOD
"""
import os
import sys
import requests
import pandas as pd
from pathlib import Path
from decimal import Decimal
from math import ceil, floor
parent_path = str(Path(__file__).parents[1])
sys.path.append(parent_path)
from tools.tradingday import CN_TradingDay
from tools.helper import new_round
config_path = os.path.join(parent_path, "config")


class SHFE:

    def __init__(self):
        self.config_file = os.path.join(config_path, "shfe.csv")
        self.ci_url = "https://www.shfe.com.cn/data/instrument/ContractBaseInfo{date}.dat"
        self.tp_url = "https://www.shfe.com.cn/data/instrument/ContractDailyTradeArgument{date}.dat"
        self.eod_url = "https://www.shfe.com.cn/data/dailydata/kx/kx{date}.dat"

        self.ci_columns = ["InstrumentID", "ProductID", "ListPrice", "FirstTradingDay", "LastTradingDay", "FirstDeliveryDay", "LastDeliveryDay"]
        self.tp_columns = ["InstrumentID", "UpperLimit", "LowerLimit"]
        self.ref_columns = ["InstrumentID", "ProductID", "Unit", "TickSize", "ListPrice", "UpperLimitPrice", "LowerLimitPrice", "PositionLimit", "FirstTradingDay", "LastTradingDay", "LastDeliveryDay"]
        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "PreSettlePrice", "SettlePrice", "Volume", "Turnover", "OpenInterest"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

        self.tradingday = CN_TradingDay()

    def get_contract_info(self, date: str) -> pd.DataFrame:
        r = requests.get(url=self.ci_url.format(date=date), headers=self.headers)
        ci = pd.DataFrame(r.json()["ContractBaseInfo"])
        ci["ProductID"] = ci["INSTRUMENTID"].map(lambda x: x[:-4])
        ci = ci[["INSTRUMENTID", "ProductID", "BASISPRICE", "OPENDATE", "EXPIREDATE", "STARTDELIVDATE", "ENDDELIVDATE"]]
        ci.columns = self.ci_columns
        return ci

    def get_trade_para(self, date: str) -> pd.DataFrame:
        r = requests.get(url=self.tp_url.format(date=date), headers=self.headers)
        tp = pd.DataFrame(r.json()["ContractDailyTradeArgument"])
        tp = tp[["INSTRUMENTID", "UPPER_VALUE", "LOWER_VALUE"]]
        tp["UPPER_VALUE"] = tp["UPPER_VALUE"].astype(float)
        tp["LOWER_VALUE"] = tp["LOWER_VALUE"].astype(float)
        tp.columns = self.tp_columns
        return tp

    @staticmethod
    def calc_upper(row):
        pre_settle = Decimal(str(row["PreSettlePrice"]))
        upper_limit = Decimal(str(row["UpperLimit"]))
        tick_size = Decimal(str(row["TickSize"]))
        up = pre_settle * (1 + upper_limit)
        up = new_round(floor(up / tick_size) * tick_size)
        return up

    @staticmethod
    def calc_lower(row):
        pre_settle = Decimal(str(row["PreSettlePrice"]))
        lower_limit = Decimal(str(row["LowerLimit"]))
        tick_size = Decimal(str(row["TickSize"]))
        low = pre_settle * (1 - lower_limit)
        low = new_round(floor(low / tick_size) * tick_size)
        return low

    def get_ref(self, date: str) -> pd.DataFrame:
        ci = self.get_contract_info(date)
        tp = self.get_trade_para(date)
        ref = pd.merge(ci, tp, on="InstrumentID")
        config = pd.read_csv(self.config_file, dtype=str)
        config = config[config["Type"] == "futures"].set_index("Product")
        ref["Unit"] = ref["ProductID"].map(lambda x: config.loc[x, "Unit"])
        ref["TickSize"] = ref["ProductID"].map(lambda x: config.loc[x, "TickSize"])
        ref["PositionLimit"] = None
        pre_date = self.tradingday.get_pre(date)
        pre_eod = self.get_eod(pre_date).set_index("InstrumentID")
        ref["PreSettlePrice"] = ref.apply(lambda x: pre_eod.loc[x["InstrumentID"], "SettlePrice"] if x["InstrumentID"] in pre_eod.index else x["ListPrice"], axis=1)
        ref["UpperLimitPrice"] = ref.apply(self.calc_upper, axis=1)
        ref["LowerLimitPrice"] = ref.apply(self.calc_lower, axis=1)
        ref = ref[self.ref_columns]
        return ref

    def get_eod(self, date: str) -> pd.DataFrame:
        r = requests.get(url=self.eod_url.format(date=date), headers=self.headers)
        eod = pd.DataFrame(r.json()["o_curinstrument"])
        eod["InstrumentID"] = eod.apply(lambda x: x["PRODUCTGROUPID"].strip() + x["DELIVERYMONTH"], axis=1)
        eod["TradingDay"] = date
        eod = eod[["InstrumentID", "TradingDay", "OPENPRICE", "HIGHESTPRICE", "LOWESTPRICE", "CLOSEPRICE", "PRESETTLEMENTPRICE", "SETTLEMENTPRICE", "VOLUME", "TURNOVER", "OPENINTEREST"]]
        eod.columns = self.eod_columns
        eod = eod[eod["SettlePrice"].str.len() != 0]
        eod["Turnover"] = eod["Turnover"].map(lambda x: Decimal(str(x)) * 10000)
        return eod


if __name__ == "__main__":
    shfe = SHFE()
    print(shfe.get_ref("20230728"))
    print(shfe.get_eod("20230728"))
