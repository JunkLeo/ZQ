# -*- coding:utf-8 -*-
"""
Date: 2023/09/22
Desc: 港交所
"""
from collections import defaultdict
from decimal import Decimal

import pandas as pd
import requests
pd.set_option('display.unicode.east_asian_width', True)


class HKG:

    def __init__(self) -> None:
        self.stock = Stock()
        self.futures = Futures()
        self.option = Option()


class Stock:

    def __init__(self):
        self.ref_url = "https://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/services/trading/securities/securitieslists/ListOfSecurities_c.xlsx"
        self.eod_url = "https://sc.hkex.com.hk/gb/www.hkex.com.hk/chi/stat/smstat/dayquot/d{YYMMDD}c.htm"

        self.eod_columns = ["InstrumentID", "TradingDay", "Currency", "PreClosePrice", "BidPrice", "AskPrice", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

    def get_ref(self) -> pd.DataFrame:
        r = requests.get(self.ref_url, headers=self.headers, timeout=10)
        ref = pd.read_excel(r.content, skiprows=2)
        return ref

    def get_open(self, resp: requests.Response) -> pd.DataFrame:
        lines = resp.text.split("\r\n")
        sep = "-------------------------------------------------------------------------------"
        sep_index = [index for (index, value) in enumerate(lines) if value == sep]
        raw = [line.replace("</font></pre><pre><font size='1'>", "") for line in lines[sep_index[1] + 4:sep_index[2] - 1]]
        raw_records = defaultdict(str)
        for line in raw:
            if "<" in line and not line.startswith("      "):
                instrumentid = line.split()[0].zfill(5)
                raw_records[instrumentid] += line.lstrip()

        records = []
        for instrumentid, trans in raw_records.items():
            op = "0.0"
            record = []
            trans = trans.replace(",", "")
            for i in trans.split():
                if "-" in i and i.split("-")[1].replace(".", "").isdigit():
                    record.append(i)
            for pv in record:
                if pv[0] in ["P", "D", "Y"]:
                    continue
                op = pv.split("-")[1]
                break
            records.append([instrumentid, op])
        ops = pd.DataFrame(records, columns=["InstrumentID", "OpenPrice"]).set_index("InstrumentID")
        return ops

    # missing open price
    def get_eod(self, date: str) -> pd.DataFrame:
        r = requests.get(self.eod_url.format(YYMMDD=date[2:]), headers=self.headers, timeout=30)
        r.encoding = "gbk"
        ops = self.get_open(r)
        lines = r.text.split("\r\n")
        sep = "---------------------------------------------------------------------------------------------------------"
        sep_index = [index for (index, value) in enumerate(lines) if value == sep]
        raw = [line.lstrip("*").replace("</font></pre><pre><font size='1'>", "") for line in lines[sep_index[-3] + 7:sep_index[-2]]]
        records = []
        for line in raw:
            instrumentid = line.split()[0].strip().split("#")[0].zfill(5)
            tmp = line.split()
            if "TRADING SUSPENDED" in line or "TRADING HALTED" in line:
                record = [instrumentid, date, tmp[-4], "0.0", "0.0", "0.0", "0.0", "0.0", "0.0", "0.0", "0.0"]
            else:
                record = [instrumentid, date] + tmp[-9:]
            records.append(record)
        columns = ["InstrumentID", "TradingDay", "Currency", "PreClosePrice", "BidPrice", "AskPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]
        eod = pd.DataFrame(records, columns=columns)
        for column in eod.columns[3:]:
            eod[column] = eod[column].map(lambda x: x.strip().replace("N/A", "0.0").replace("-", "0.0").replace(",", ""))
        eod["OpenPrice"] = eod["InstrumentID"].map(lambda x: ops.loc[x, "OpenPrice"] if x in ops.index else "0.0")
        eod = eod[self.eod_columns]
        eod.to_csv(f"/tmp/{date}.csv", index=False)
        return eod


class Futures:

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
            products.update({
                i: i + "f" for i in row if not pd.isna(i)
            })
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
        else:
            raise Exception("requests failed")
        if r.status_code != 200:
            return eod
        if product in ["stock", "dividend", "hibor", "tri", "hgt", "crmbc", "lme", "lmeu", "iron"]:
            lines = [i for i in r.text.replace(",", "").replace("|", "").split("\r\n") if "-" in i and "Calendar Spread" not in i]
            records = []
            for line in lines:
                p = line.split("-")[0].strip()
                if self.product_mapper[product]["sep"] in line:
                    continue
                if p in self.month_mapper and "EXPIRED" not in "".join(line.split()):
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


class Option:

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
        contractmonth = str(row["ContractMonth"])
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
            except:
                retry += 1
                continue
        else:
            raise Exception("requests failed")
        if product in ["MTW", "CUS"]:
            lines = [
                [product] + i.split() for i in r.text.replace(",", "").replace("|", "").split("\r\n") if "-" in i and "/" not in i and i.split("-")[-2] in self.month_mapper and len(i.split()) == 12
            ]
            eod = pd.DataFrame(lines, columns=self.columns[0])
        elif product in ["dqe"]:
            lines = [i for i in r.text.replace(",", "").replace("|", "").split("\r\n") if (i[:3] in self.month_mapper and len(i.split()) == 12) or ("-" in i and "CLOSING PRICE" in i)]
            records = []
            for line in lines:
                p = line.split("-")[0].split()[1].strip()
                if "CLOSING PRICE" in line:
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
    hkg = HKG()
    import sys
    from datetime import datetime

    from loguru import logger
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.today().strftime("%Y%m%d")
    logger.info(f"Run {date}")
    print(hkg.stock.get_eod(date))
    print(hkg.futures.get_eod(date))
    print(hkg.option.get_eod(date))
