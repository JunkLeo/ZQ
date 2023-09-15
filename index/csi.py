# -*- coding:utf-8 -*-
"""
Date: 2023/08/03
Desc: 中证指数每日REF
"""
import requests
import pandas as pd


class CSI:

    def __init__(self):
        self.ref_url = "https://www.csindex.com.cn/csindex-home/index-list/query-index-item"

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

    def get_ref(self) -> pd.DataFrame:
        para = {
            "indexFilter": {
                "currency": None,
                "hotSpot": None,
                "ifCustomized": None,
                "ifTracked": None,
                "ifWeightCapped": None,
                "indexClassify": None,
                "indexCompliance": None,
                "indexSeries": ["1", "2", "3", "7", "4", "5", "6"],
                "region": None,
                "undefined": None
            },
            "pager": {
                "pageNum": 1,
                "pageSize": 5000
            },
            "sorter": {
                "sortField": "null",
                "sortOrder": None
            }
        }
        r = requests.post(url=self.ref_url, json=para, headers=self.headers, timeout=10)
        ref = pd.DataFrame(r.json()["data"]).sort_values(by="indexCode")
        return ref


if __name__ == "__main__":
    csi = CSI()
    print(csi.get_ref())
