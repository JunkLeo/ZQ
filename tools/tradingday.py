# -* - coding: UTF-8 -* -
import os
import pandas as pd
from datetime import datetime
from pathlib import Path

calendar_path = os.path.join(Path(__file__).parents[1], "calendar")


class NaturalDay:

    def __init__(self):
        self.all()

    def all(self):
        today = datetime.today().strftime("%Y%m%d")
        end_year = int(today[:4]) + 5
        self._all = [i.strftime("%Y%m%d") for i in pd.date_range(start="19900101", end=f"{end_year}1231")]

    def offset(self, day, delta):
        if day not in self._all:
            raise Exception(f"{day} not in tradingdays")
        index = self._all.index(day) + delta
        return self._all[index]

    def get_next(self, day):
        return self.offset(day, 1)

    def get_pre(self, day):
        return self.offset(day, -1)

    def get_month_end(self, day):
        index = self._all.index(day)
        month_end = day
        for date in self._all[index:]:
            if day[:6] == date[:6]:
                month_end = date
            else:
                return month_end

    def is_month_end(self, day):
        next_day = self.get_next(day)
        if day[:6] != next_day[:6]:
            return True
        return False

    def calc_weekday(self, day):
        week = datetime.strptime(day, "%Y%m%d").weekday()
        return week

    def is_weekday(self, day):
        week = self.calc_weekday(day)
        if week < 5:
            return True
        return False

    def get_week_end(self, day):
        index = self._all.index(day)
        week_end = day
        for date in self._all[index:]:
            if self.calc_weekday(date) < 5:
                week_end = date
            else:
                return week_end


class CN_TradingDay(NaturalDay):

    def __init__(self):
        self.all()

    def all(self):
        self._all = []
        tradingday_file = os.path.join(calendar_path, "cn_tradingday.txt")
        with open(tradingday_file, "r") as f:
            for line in f:
                self._all.append(line.strip())


if __name__ == "__main__":
    cn_tradingday = CN_TradingDay()
    print(cn_tradingday.is_month_end("20230428"))
