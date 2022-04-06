import pandas as pd
import numpy as np

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

    def timeBetween(self, df):
        return (pd.to_datetime(self.kwargs["to"]) - pd.to_datetime(self.kwargs["from"])) / np.timedelta64(1, self.kwargs["unit"])

    def column(self, df):
        return df[self.kwargs["columnName"]]

    def value(self, df):
        return self.kwargs["value"]