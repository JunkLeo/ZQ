# -*- coding:utf-8 -*-
"""
Date: 2023/08/08
Desc: 港交所期货每日EOD
"""
import requests
import pandas as pd
from decimal import Decimal


class HKG:

    def __init__(self):
        self.product_url = "https://sc.hkex.com.hk/gb/www.hkex.com.hk/chi/market/rm/rm_dcrm/riskdata/margin_hkcc/mertc_hkcc_{YYMMDD}.htm"
        self.eod_url = "https://sc.hkex.com.hk/TuniS/www.hkex.com.hk/eng/stat/dmstat/dayrpt/{product}{YYMMDD}.htm"

        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "PreSettlePrice", "SettlePrice", "Volume", "Turnover", "OpenInterest"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

        self.month_mapper = {
            "JAN": "01",
            "FEB": "02",
            "MAR": "03",
            "APR": "04",
            "MAY": "05",
            "JUN": "06",
            "JUL": "07",
            "AUG": "08",
            "SEP": "09",
            "OCT": "10",
            "NOV": "11",
            "DEC": "12"
        }

        self.columns = [
            ["Product", "ContractMonth", "OpenPrice", "HighPrice", "LowPrice", "SettlePrice", "SettlePriceChg", "Volume", "OpenInterest", "OpenInterestChg"],
            ["Product", "ContractMonth", "OpenPrice", "HighPrice", "LowPrice", "SettlePrice", "SettlePriceChg", "ContractHigh", "ContractLow", "Volume", "OpenInterest", "OpenInterestChg"],
            [
                "Product", "ContractMonth", "AHTOpenPrice", "AHTHighPrice", "AHTLowPrice", "AHTClosePrice", "AHTVolume", "DTOpenPrice", "DTHighPrice", "DTLowPrice", "DTVolume", "SettlePrice",
                "SettlePriceChg", "ContractHigh", "ContractLow", "CombinedVolume", "OpenInterest", "OpenInterestChg"
            ]
        ]

        self.product_mapper = {
            "stock": {
                "sep": "MULTIPLIER",
                "columns": self.columns[0]
            },
            "hibor": {
                "sep": "HIBOR",
                "columns": self.columns[1]
            },
            "dividend": {
                "sep": "index",
                "columns": self.columns[1]
            },
            "crmbc": {
                "sep": "Futures",
                "columns": self.columns[2]
            },
            "lme": {
                "sep": "Futures",
                "columns": self.columns[2]
            },
            "lmeu": {
                "sep": "Futures",
                "columns": self.columns[2]
            },
            "iron": {
                "sep": "FUTURES",
                "columns": self.columns[2]
            },
            "tri": {
                "sep": "index",
                "columns": self.columns[2]
            },
            "hgt": {
                "sep": "index",
                "columns": self.columns[2]
            },
            "CHH": {
                "columns": self.columns[1]
            },
            "MBI": {
                "columns": self.columns[1]
            },
            "VHS": {
                "columns": self.columns[1]
            }
        }

    def get_products(self, date: str) -> dict:
        products = {}
        df = pd.read_html(self.product_url.format(YYMMDD=date[2:]))[-1]
        for row in df.values:
            products.update({i: i + "f" for i in row if not pd.isna(i)})
        products["MBI"] = "SECTIDXF"
        products["tri"] = "trif"
        products["hgt"] = "hgtf"
        products["stock"] = "stock"
        products["hibor"] = "hibor"
        products["dividend"] = "dividend"
        products["crmbc"] = "CRMBCF"
        products["iron"] = "IRONF"
        products["lme"] = "lmef"
        products["lmeu"] = "lmeuf"
        return products

    def get_eod_by_product(self, date: str, product: str, para: str) -> pd.DataFrame:
        eod = pd.DataFrame()
        retry = 0
        while retry < 5:
            try:
                r = requests.get(self.eod_url.format(product=para, YYMMDD=date[2:]), headers=self.headers, timeout=10)
                break
            except Exception as e:
                print(e)
                retry += 1
                continue
        if r.status_code != 200:
            return eod
        if product in ["stock", "dividend", "hibor", "tri", "hgt", "crmbc", "lme", "lmeu", "iron"]:
            lines = [i for i in r.text.replace(",", "").replace("|", "").split("\r\n") if "-" in i and "Calendar Spread" not in i]
            records = []
            for line in lines:
                if self.product_mapper[product]["sep"] in line:
                    p = line.split("-")[0].strip()
                    continue
                if line.split("-")[0] in self.month_mapper and "EXPIRED" not in "".join(line.split()):
                    record = [p] + line.split()
                    records.append(record)
            eod = pd.DataFrame(records, columns=self.product_mapper[product]["columns"])
        else:
            lines = [
                [product] + i.split()
                for i in r.text.replace(",", "").replace("|", "").split("\r\n")
                if "-" in i and "/" not in i and i.split("-")[0] in self.month_mapper and "EXPIRED" not in "".join(i.split())
            ]
            eod = pd.DataFrame(lines, columns=self.product_mapper[product]["columns"] if product in self.product_mapper else self.columns[2])
        for column in eod.columns[2:]:
            eod[column] = eod[column].map(lambda x: 0 if x.strip() == "-" else Decimal(x))
        if "AHTOpenPrice" in eod.columns:
            eod["OpenPrice"] = eod.apply(lambda x: x["DTOpenPrice"] if Decimal(x["AHTOpenPrice"]) == 0 else x["AHTOpenPrice"], axis=1)
            eod["HighPrice"] = eod.apply(lambda x: max(Decimal(x["AHTHighPrice"]), Decimal(x["DTHighPrice"])), axis=1)
            eod["LowPrice"] = eod.apply(lambda x: min(Decimal(x["AHTLowPrice"]), Decimal(x["DTLowPrice"])), axis=1)
            eod["Volume"] = eod.apply(lambda x: Decimal(x["AHTVolume"]) + Decimal(x["DTVolume"]), axis=1)
        eod["ClosePrice"] = eod["SettlePrice"]
        eod["TradingDay"] = date
        eod["InstrumentID"] = eod.apply(lambda x: x["Product"] + x["ContractMonth"].split("-")[1] + self.month_mapper[x["ContractMonth"].split("-")[0]], axis=1)
        eod = eod[["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "SettlePrice", "Volume", "OpenInterest"]]
        return eod

    def get_eod(self, date: str) -> pd.DataFrame:
        products = self.get_products(date)
        eod = pd.DataFrame()
        for product, para in products.items():
            df = self.get_eod_by_product(date, product, para)
            eod = pd.concat([eod, df])
        eod.to_csv("/tmp/20230808.csv", index=False)
        return eod


if __name__ == "__main__":
    hkg = HKG()
    print(hkg.get_eod("20230816"))
