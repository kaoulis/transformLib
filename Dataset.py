import pandas as pd
from pandas.errors import ParserError


class Dataset:

    def __init__(self, df: pd.DataFrame, dataset_msg: dict = None , frozen: bool = True, enable_sum_stats: bool = False) -> None:
        self.df = df
        if dataset_msg is None:
            self.dataset_msg = {}
        else:
            self.dataset_msg = dataset_msg 
        if frozen:
            dataset_msg
        else:
            self.process()


    def process(self):
        self.inferSchema()
        # Execute Transformations
        self.dataset_msg["transformations"] = self.transform()
        # Return Schema
        self.dataset_msg["schema"] = self.getSchema()
        # Return Shape
        self.dataset_msg["shape"] = self.getShape()
        # Return Summary Statistics
        self.dataset_msg["sumStats"] = self.getSumStats()
        # Return Preview
        self.dataset_msg["preview"] = self.getPreview()


    def transform(self, steps: list = None):
        if steps is None:
            steps = self.dataset_msg.get("transformations", [])
        self.inferSchema()
        for t in steps:
            getattr(self, next(iter(t)))(**t[next(iter(t))])
        return steps
            
        
    def getSchema(self) -> dict:
        return self.df.dtypes.apply(lambda x:
                        "String" if ((x.name).lower()).startswith("string") else
                        "Integer" if ((x.name).lower()).startswith("int") else
                        "Float" if ((x.name).lower()).startswith("float") else
                        "Date" if ((x.name).lower()).startswith("datetime") else
                        "Boolean" if ((x.name).lower()).startswith("boolean") else
                        x.name).to_dict()


    def getShape(self) -> list:
        return list(self.df.shape)


    def getSumStats(self) -> dict:
        sumStats = {}
        dtypes = self.df.dtypes.apply(lambda x: x.name).tolist()
        try:
            numStats = self.df.describe(include=list(set(x for x in dtypes if (x.lower()).startswith("int") or (x.lower()).startswith("float") or (x.lower()).startswith("date"))), datetime_is_numeric=True)
            numStats = numStats.reset_index().rename(columns={'index': 'Numeric statistics'})
            sumStats["numStats"] = numStats.astype(str).to_dict('records')
        except:
            sumStats["numStats"] = []
        
        try:
            catStats = self.df.describe(include=list(set(x for x in dtypes if (x.lower()).startswith("string") or (x.lower()).startswith("boolean"))))
            catStats = catStats.reset_index().rename(columns={'index': 'Categorical statistics'})
            sumStats["catStats"] = catStats.astype(str).to_dict('records')
        except:
            sumStats["catStats"] = []
        return sumStats


    def getPreview(self, size = 100) -> dict:
        preview = {}
        preview["head"] = self.df.head(size)
        preview["head"] = preview["head"].astype(str).to_dict('records')
        preview["tail"] = self.df.tail(size)
        preview["tail"] = preview["tail"].astype(str).to_dict('records')
        return preview


    def inferSchema(self):
        for c in self.df.columns[self.df.dtypes == 'object']:  # don't cnvt num
            try:
                self.df[c] = pd.to_datetime(self.df[c], infer_datetime_format=True)
            except (ParserError, ValueError):  # Can't cnvrt some
                pass  # ...so leave whole column as-is unconverted
        self.df = self.df.convert_dtypes()


    def generate_pandas_profile(self):
        pass


    def rename(self, columns: dict):
        self.df = self.df.rename(columns=columns)


    def filter(self, action, statement):
        pass