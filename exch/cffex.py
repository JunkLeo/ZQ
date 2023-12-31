# -*- coding:utf-8 -*-
"""
Date: 2023/09/22
Desc: 中金所
"""
import os
import re
import requests
import pandas as pd
from pathlib import Path
from random import randint

config_path = os.path.join(Path(__file__).parents[1], "config")


class CFFEX:

    def __init__(self) -> None:
        self.futures = Futures()
        self.option = Option()


class Futures:

    def __init__(self):
        self.config_file = os.path.join(config_path, "cffex.csv")
        self.tip_url = "http://www.cffex.com.cn/cp/index_6719.xml?id={id}"
        self.ref_url = "http://www.cffex.com.cn/sj/jycs/{YYYYMM}/{DD}/index.xml?id={id}"
        self.eod_url = "http://www.cffex.com.cn/sj/hqsj/rtj/{YYYYMM}/{DD}/index.xml?id={id}"

        self.ref_columns = [
            "InstrumentID",
            "ProductID",
            "Unit",
            "TickSize",
            "ListPrice",
            "UpperLimitPrice",
            "LowerLimitPrice",
            "PositionLimit",
            "FirstTradingDay",
            "LastTradingDay",
            "FirstDeliveryDay",
            "LastDeliveryDay"
        ]
        self.eod_columns = [
            "InstrumentID",
            "TradingDay",
            "OpenPrice",
            "HighPrice",
            "LowPrice",
            "ClosePrice",
            "PreSettlePrice",
            "SettlePrice",
            "Volume",
            "Turnover",
            "OpenInterest"
        ]

    def get_tip(self) -> pd.DataFrame:
        rand_id = str(randint(10, 60))
        tip = pd.read_xml(self.tip_url.format(id=rand_id))
        tip = tip[~tip["INSTRUMENTID"].str.contains("-")]
        tip = tip[["INSTRUMENTID", "STARTDELIVDATE", "ENDDELIVDATE"]]
        tip["STARTDELIVDATE"] = tip["STARTDELIVDATE"].map(lambda x: x if pd.isna(x) else str(int(x)))
        tip["ENDDELIVDATE"] = tip["ENDDELIVDATE"].map(lambda x: x if pd.isna(x) else str(int(x)))
        return tip

    def get_ref(self, date: str, mode: str = "ongoing") -> pd.DataFrame:
        rand_id = str(randint(10, 60))
        ref = pd.read_xml(self.ref_url.format(YYYYMM=date[:6], DD=date[6:], id=rand_id))
        config = pd.read_csv(self.config_file, dtype=str).set_index("Product")
        ref["Unit"] = ref["PRODUCT_ID"].map(lambda x: config.loc[x, "Unit"])
        ref["TickSize"] = ref["PRODUCT_ID"].map(lambda x: config.loc[x, "TickSize"])
        columns = [
            "INSTRUMENT_ID",
            "PRODUCT_ID",
            "Unit",
            "TickSize",
            "BASIS_PRICE",
            "UPPERLIMITPRICE",
            "LOWERLIMITPRICE",
            "LONG_LIMIT",
            "OPEN_DATE",
            "END_TRADING_DAY"
        ]
        ref = ref[~ref["INSTRUMENT_ID"].str.contains("-")][columns]
        if mode == "ongoing":
            tip = self.get_tip()
            ref = pd.merge(ref, tip, how="left", left_on="INSTRUMENT_ID", right_on="INSTRUMENTID")
            ref.drop(columns=["INSTRUMENTID"], inplace=True)
            ref.columns = self.ref_columns
        else:
            ref.columns = self.ref_columns[:-2]
        return ref

    def get_eod(self, date: str) -> pd.DataFrame:
        rand_id = str(randint(10, 60))
        eod = pd.read_xml(self.eod_url.format(YYYYMM=date[:6], DD=date[6:], id=rand_id))
        columns = [
            "instrumentid",
            "tradingday",
            "openprice",
            "highestprice",
            "lowestprice",
            "closeprice",
            "presettlementprice",
            "settlementprice",
            "volume",
            "turnover",
            "openinterest"
        ]
        eod = eod[~eod["instrumentid"].str.contains("-")][columns]
        eod.columns = self.eod_columns
        return eod


class Option:

    def __init__(self):
        self.config_file = os.path.join(config_path, "cffex.csv")
        self.js_url1 = "http://www.cffex.com.cn/sj/jscs/option/index_6792.xml?id={id}"
        self.js_url2 = "http://www.cffex.com.cn/sj/jscs/option/{YYYYMM}/{DD}/index.xml?id={id}"
        self.ref_url = "http://www.cffex.com.cn/sj/jycs/{YYYYMM}/{DD}/index.xml?id={id}"
        self.eod_url = "http://www.cffex.com.cn/sj/hqsj/rtj/{YYYYMM}/{DD}/index.xml?id={id}"

        self.js_columns = ["Underlying", "AdjustFactor", "GuaranteeFactor"]
        self.ref_columns = [
            "InstrumentID",
            "ProductID",
            "Unit",
            "TickSize",
            "ListPrice",
            "UpperLimitPrice",
            "LowerLimitPrice",
            "PositionLimit",
            "FirstTradingDay",
            "LastTradingDay",
            "CallPut",
            "StrikePrice",
            "ExecType",
            "DeliveryMethod",
            "Underlying",
            "Margin"
        ]
        self.eod_columns = [
            "InstrumentID",
            "TradingDay",
            "OpenPrice",
            "HighPrice",
            "LowPrice",
            "ClosePrice",
            "PreSettlePrice",
            "SettlePrice",
            "Volume",
            "Turnover",
            "OpenInterest"
        ]

    def get_js(self) -> pd.DataFrame:
        rand_id = str(randint(10, 60))
        r = requests.get(url=self.js_url1.format(id=rand_id), timeout=10)
        date = re.findall("\\d{8}", r.text)[0]
        js = pd.read_xml(self.js_url2.format(YYYYMM=date[:6], DD=date[6:], id=rand_id))
        js = js[["OPTION_SERIES_ID", "MARGIN_ADJUSTMENT_FACTOR", "MARGINRISKMANAGEPARAM"]]
        js.columns = self.js_columns
        return js

    # TODO: Margin
    def calc_margin(self, row):
        """
            Margin
            1.每手看涨期权交易保证金=（合约当日结算价×合约乘数）+max（标的指数当日收盘价×合约乘数×合约保证金调整系数-虚值额，最低保障系数×标的指数当日收盘价×合约乘数×合约保证金调整系数）
            2.每手看跌期权交易保证金=（合约当日结算价×合约乘数）+max（标的指数当日收盘价×合约乘数×合约保证金调整系数-虚值额，最低保障系数×合约行权价格×合约乘数×合约保证金调整系数）
            3.其中，股指期权合约的保证金调整系数、最低保障系数由交易所另行规定。
                3.1 看涨期权虚值额为：max[（本合约行权价格-标的指数当日收盘价）×合约乘数，0]
                3.2 看跌期权虚值额为：max[（标的指数当日收盘价-本合约行权价格）×合约乘数，0]
        """
        return None

    def get_ref(self, date: str) -> pd.DataFrame:
        rand_id = str(randint(10, 60))
        ref = pd.read_xml(self.ref_url.format(YYYYMM=date[:6], DD=date[6:], id=rand_id))
        config = pd.read_csv(self.config_file, dtype=str).set_index("Product")
        ref["Unit"] = ref["PRODUCT_ID"].map(lambda x: config.loc[x, "Unit"])
        ref["TickSize"] = ref["PRODUCT_ID"].map(lambda x: config.loc[x, "TickSize"])
        columns = [
            "INSTRUMENT_ID",
            "PRODUCT_ID",
            "Unit",
            "TickSize",
            "BASIS_PRICE",
            "UPPERLIMITPRICE",
            "LOWERLIMITPRICE",
            "LONG_LIMIT",
            "OPEN_DATE",
            "END_TRADING_DAY"
        ]
        ref = ref[ref["INSTRUMENT_ID"].str.contains("-")][columns]
        ref["CallPut"] = ref["INSTRUMENT_ID"].map(lambda x: x.split("-")[1])
        ref["StrikePrice"] = ref["INSTRUMENT_ID"].map(lambda x: x.split("-")[2])
        ref["Underlying"] = ref["INSTRUMENT_ID"].map(lambda x: x.split("-")[0])
        ref["ExecType"] = "European"
        ref["DeliveryMethod"] = "Cash"
        ref["Margin"] = ref.apply(self.calc_margin, axis=1)
        ref.columns = self.ref_columns
        return ref

    def get_eod(self, date: str) -> pd.DataFrame:
        rand_id = str(randint(10, 60))
        eod = pd.read_xml(self.eod_url.format(YYYYMM=date[:6], DD=date[6:], id=rand_id))
        columns = [
            "instrumentid",
            "tradingday",
            "openprice",
            "highestprice",
            "lowestprice",
            "closeprice",
            "presettlementprice",
            "settlementprice",
            "volume",
            "turnover",
            "openinterest"
        ]
        eod = eod[eod["instrumentid"].str.contains("-")][columns]
        eod.columns = self.eod_columns
        return eod


if __name__ == "__main__":
    cffex = CFFEX()
    import sys
    from datetime import datetime

    from loguru import logger
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.today().strftime("%Y%m%d")
    logger.info(f"Run {date}")
    print(cffex.futures.get_ref(date))
    print(cffex.futures.get_eod(date))
    print(cffex.option.get_ref(date))
    print(cffex.option.get_eod(date))
