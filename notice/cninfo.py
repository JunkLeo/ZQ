import pandas as pd
import requests
from loguru import logger


class CNINFO:

    def __init__(self, date) -> None:
        self.date = "-".join([date[:4], date[4:6], date[6:]])
        self.url = "http://www.cninfo.com.cn/new/information/memoQuery"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }
        logger.info(f"Start {self.date}")

    def query_data(self) -> dict:
        para = {
            "queryDate": self.date
        }
        r = requests.post(url=self.url, headers=self.headers, data=para, timeout=10)
        raw = pd.DataFrame(r.json()["clusterSRTbTrade0112"]["srTbTrade0112s"])
        data = {}
        for _, row in raw.iterrows():
            data[row["tradingTipsName"]] = pd.DataFrame(row["tbTrade0112s"])
        return data


if __name__ == "__main__":
    jc = CNINFO("20231017")
    data = jc.query_data()
    for key, value in data.items():
        print(key)
        print(value)
