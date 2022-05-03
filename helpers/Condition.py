from Expression import Expression

class Condition:
    def __init__(self, column, compare, value=None, logic=None, **kwargs):
        self.column = column
        self.operator = compare
        if isinstance(value, dict):
            self.value = Expression(value["expression"]["method"], value["expression"]["kwargs"]).execute()
        else:
            self.value = value
        self.value = value
        self.logic = logic
        self.kwargs = kwargs

    def __call__(self, df):
        if self.operator == '==':
            return df[self.column] == self.value, self.logic
        elif self.operator == '!=':
            return df[self.column] != self.value, self.logic
        elif self.operator == '<=':
            return df[self.column] <= self.value, self.logic
        elif self.operator == '>=':
            return df[self.column] >= self.value, self.logic
        elif self.operator == '<':
            return df[self.column] < self.value, self.logic
        elif self.operator == '>':
            return df[self.column] > self.value, self.logic
        elif self.operator == 'hasvalues':
            return df[self.column].isin(self.value), self.logic
        elif self.operator == 'startswith':
            return df[self.column].str.startswith(self.value, na=False), self.logic
        elif self.operator == 'endswith':
            return df[self.column].str.endswith(self.value, na=False), self.logic
        elif self.operator == 'contains':
            return df[self.column].str.contains(self.value, case=self.kwargs.get("case", False),
                                                regex=self.kwargs.get("regex", False), na=False), self.logic
        elif self.operator == 'ismissing':
            return df[self.column].isna(), self.logic
        elif self.operator == 'notmissing':
            return df[self.column].notna(), self.logic
        elif self.operator == 'isTrue':
            return df[self.column] == True, self.logic
        elif self.operator == 'isFalse':
            return df[self.column] == False, self.logic