# -*- coding:utf-8 -*-
"""
Date: 2023/07/27
Desc: 中金所每日REF/EOD
"""
import pandas as pd
from random import randint


class CFE:

    def __init__(self):
        self.tip_url = "http://www.cffex.com.cn/cp/index_6719.xml?id={id}"
        self.ref_url = "http://www.cffex.com.cn/sj/jycs/{YYYYMM}/{DD}/index.xml?id={id}"
        self.eod_url = "http://www.cffex.com.cn/sj/hqsj/rtj/{YYYYMM}/{DD}/index.xml?id={id}"

        self.ref_columns = ["InstrumentID", "ProductID", "ListPrice", "UpperLimitPrice", "LowerLimitPrice", "PositionLimit", "FirstTradingDay", "LastTradingDay", "LastDeliveryDay"]
        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "PreSettlePrice", "SettlePrice", "Volume", "Turnover", "OpenInterest"]

    def get_tip(self) -> pd.DataFrame:
        rand_id = str(randint(10, 60))
        tip = pd.read_xml(self.tip_url.format(id=rand_id))
        tip = tip[~tip["INSTRUMENTID"].str.contains("-")]
        tip = tip[["INSTRUMENTID", "ENDDELIVDATE"]]
        tip["ENDDELIVDATE"] = tip["ENDDELIVDATE"].map(lambda x: x if pd.isna(x) else str(int(x)))
        return tip

    def get_ref(self, date: str, mode: str = "ongoing", product: str = "all") -> pd.DataFrame:
        rand_id = str(randint(10, 60))
        ref = pd.read_xml(self.ref_url.format(YYYYMM=date[:6], DD=date[6:], id=rand_id))
        if product != "all":
            ref = ref[ref["PRODUCT_ID"] == product]
        columns = ["INSTRUMENT_ID", "PRODUCT_ID", "BASIS_PRICE", "UPPERLIMITPRICE", "LOWERLIMITPRICE", "LONG_LIMIT", "OPEN_DATE", "END_TRADING_DAY"]
        ref = ref[~ref["INSTRUMENT_ID"].str.contains("-")][columns]
        if mode == "ongoing":
            tip = self.get_tip()
            ref = pd.merge(ref, tip, how="left", left_on="INSTRUMENT_ID", right_on="INSTRUMENTID")
            ref.drop(columns=["INSTRUMENTID"], inplace=True)
            ref.columns = self.ref_columns
        else:
            ref.columns = self.ref_columns[:-2]
        return ref

    def get_eod(self, date: str, product: str = "all") -> pd.DataFrame:
        rand_id = str(randint(10, 60))
        eod = pd.read_xml(self.eod_url.format(YYYYMM=date[:6], DD=date[6:], id=rand_id))
        if product != "all":
            eod = eod[eod["productid"] == product]
        columns = ["instrumentid", "tradingday", "openprice", "highestprice", "lowestprice", "closeprice", "presettlementprice", "settlementprice", "volume", "turnover", "openinterest"]
        eod = eod[~eod["instrumentid"].str.contains("-")][columns]
        eod.columns = self.eod_columns
        return eod


if __name__ == "__main__":
    cfe = CFE()
    print(cfe.get_ref("20230727", product="IH"))
    print(cfe.get_eod("20230727", product="IH"))
