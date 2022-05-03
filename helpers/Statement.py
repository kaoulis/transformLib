import numpy as np
from helpers.Condition import Condition

class Statement:
    def __init__(self, conditions):
        self.conditions = [Condition(**c) for c in conditions]

    def __call__(self, df):
        result = None
        for cond in self.conditions:
            if cond(df)[1] is None:
                result = cond(df)[0]
            elif cond(df)[1] == "and":
                result = np.logical_and(result, cond(df)[0])
            elif cond(df)[1] == "or":
                result = np.logical_or(result, cond(df)[0])
        return result