import pandas as pd
import numpy as np

def my_ceil(a, precision=0):
    return np.true_divide(np.ceil(a * 10 ** precision), 10 ** precision)


def my_floor(a, precision=0):
    return np.true_divide(np.floor(a * 10 ** precision), 10 ** precision)


def my_trunc(values, precision=0):
    return np.trunc(values * 10 ** precision) / (10 ** precision)

class Expression:
    def __init__(self, method, **kwargs):
        self.method = method
        self.kwargs = kwargs

    # 100 IQ recursion
    def execute(self, df):
        for key, value in self.kwargs.items():
            if isinstance(value, dict):
                exp = Expression(value["method"], **value["kwargs"])
                self.kwargs[key] = exp.execute(df)

        return getattr(self, self.method)(df)


    def column(self, df):
        return df[self.kwargs["columnName"]]


    def timeBetween(self, df):
        if self.kwargs.get("rounding") == "ceil":
            return my_ceil((pd.to_datetime(self.kwargs["to"]) - pd.to_datetime(self.kwargs["from"])) / np.timedelta64(1, self.kwargs["unit"]), precision=self.kwargs.get("precision",3))
        elif self.kwargs.get("rounding") == "floor":
            return my_floor((pd.to_datetime(self.kwargs["to"]) - pd.to_datetime(self.kwargs["from"])) / np.timedelta64(1, self.kwargs["unit"]), precision=self.kwargs.get("precision",3))
        else:
            return my_trunc((pd.to_datetime(self.kwargs["to"]) - pd.to_datetime(self.kwargs["from"])) / np.timedelta64(1, self.kwargs["unit"]), precision=self.kwargs.get("precision",3))
