import numpy as np

class If:
    def __init__(self, statement, true_expression, false_expression=np.NaN):
        self.statement = statement
        self.true_expression = true_expression
        self.false_expression = false_expression

    def setElse(self, false_exp):
        self.false_expression = false_exp

    def __call__(self, df):
        return np.where(self.statement(df), self.true_expression, self.false_expression)