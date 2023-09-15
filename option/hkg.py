# -*- coding:utf-8 -*-
"""
Date: 2023/08/09
Desc: 港交所期权每日EOD
"""
import sys
import requests
import pandas as pd
from decimal import Decimal
from datetime import datetime


class HKG:

    def __init__(self):
        self.eod_url = "https://sc.hkex.com.hk/TuniS/www.hkex.com.hk/eng/stat/dmstat/dayrpt/{product}{YYMMDD}.htm"

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
            ["Product", "ContractMonth", "StrikePrice", "CallPut", "OpenPrice", "HighPrice", "LowPrice", "SettlePrice", "SettlePriceChg", "IV%", "Volume", "OpenInterest", "OpenInterestChg"],
            [
                "Product", "ContractMonth", "StrikePrice", "CallPut", "AHTOpenPrice", "AHTHighPrice", "AHTLowPrice", "AHTClosePrice", "AHTVolume", "DTOpenPrice", "DTHighPrice", "DTLowPrice",
                "SettlePrice", "SettlePriceChg", "IV%", "DTVolume", "ContractHigh", "ContractLow", "CombinedVolume", "OpenInterest", "OpenInterestChg"
            ]
        ]

        self.products = {
            "HSI": "hsio",
            "PHS": "phso",
            "HSIW": "hsiwo",
            "MHI": "mhio",
            "HTI": "htio",
            "PTE": "pteo",
            "HHI": "hhio",
            "PHH": "phho",
            "HHIW": "hhiwo",
            "MCH": "mcho",
            "MTW": "mtwo",
            "CUS": "cuso",
            "dqe": "dqe"
        }

    def gen_instrumentid(self, row: pd.Series) -> str:
        product = row["Product"]
        contractmonth = row["ContractMonth"]
        callput = row["CallPut"]
        strikeprice = row["StrikePrice"]
        if "-" in contractmonth:
            cm = contractmonth.split("-")
            if len(cm) == 2:
                maturity = cm[1] + self.month_mapper[cm[0]]
            elif len(cm) == 3:
                maturity = cm[2] + self.month_mapper[cm[1]] + "W" + cm[0]
            else:
                raise Exception("Error ContractMonth " + contractmonth)
        else:
            maturity = contractmonth[-2:] + self.month_mapper[contractmonth[:3]]
        return product + maturity + callput + strikeprice

    def get_eod_by_product(self, date: str, product: str, para: str) -> pd.DataFrame:
        eod = pd.DataFrame()
        retry = 0
        while retry < 5:
            try:
                r = requests.get(self.eod_url.format(product=para, YYMMDD=date[2:]), headers=self.headers, timeout=10)
                break
            except Exception as e:
                retry += 1
                continue
        if product in ["MTW", "CUS"]:
            lines = [
                [product] + i.split() for i in r.text.replace(",", "").replace("|", "").split("\r\n") if "-" in i and "/" not in i and i.split("-")[-2] in self.month_mapper and len(i.split()) == 12
            ]
            eod = pd.DataFrame(lines, columns=self.columns[0])
        elif product in ["dqe"]:
            lines = [i for i in r.text.replace(",", "").replace("|", "").split("\r\n") if (i[:3] in self.month_mapper and len(i.split()) == 12) or ("-" in i and "CLOSING PRICE" in i)]
            records = []
            for line in lines:
                if "CLOSING PRICE" in line:
                    p = line.split("-")[0].split()[1].strip()
                    continue
                record = [p] + line.split()
                records.append(record)
            eod = pd.DataFrame(records, columns=self.columns[0])
        else:
            lines = [
                [product] + i.split()
                for i in r.text.replace(",", "").replace("|", "").split("\r\n")
                if "-" in i and "/" not in i and (i.split("-")[0] in self.month_mapper or i.split("-")[1] in self.month_mapper) and len(i.split()) == 20
            ]
            eod = pd.DataFrame(lines, columns=self.columns[1])
        for column in eod.columns[2:]:
            eod[column] = eod[column].map(lambda x: 0 if x.strip() == "-" else x)
        if "AHTOpenPrice" in eod.columns:
            eod["OpenPrice"] = eod.apply(lambda x: x["DTOpenPrice"] if Decimal(x["AHTOpenPrice"]) == 0 else x["AHTOpenPrice"], axis=1)
            eod["HighPrice"] = eod.apply(lambda x: max(Decimal(x["AHTHighPrice"]), Decimal(x["DTHighPrice"])), axis=1)
            eod["LowPrice"] = eod.apply(lambda x: min(Decimal(x["AHTLowPrice"]), Decimal(x["DTLowPrice"])), axis=1)
            eod["Volume"] = eod.apply(lambda x: Decimal(x["AHTVolume"]) + Decimal(x["DTVolume"]), axis=1)
        eod["ClosePrice"] = eod["SettlePrice"]
        eod["TradingDay"] = date
        eod["InstrumentID"] = eod.apply(self.gen_instrumentid, axis=1)
        eod = eod[["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "SettlePrice", "Volume", "OpenInterest"]]
        return eod

    def get_eod(self, date: str) -> pd.DataFrame:
        eod = pd.DataFrame()
        for product, para in self.products.items():
            df = self.get_eod_by_product(date, product, para)
            eod = pd.concat([eod, df])
        eod.sort_values(by=["InstrumentID"], inplace=True)
        return eod


if __name__ == "__main__":
    today = datetime.today().strftime("%Y%m%d")
    date = sys.argv[1] if len(sys.argv) > 1 else today
    hkg = HKG()
    print(hkg.get_eod(date))
