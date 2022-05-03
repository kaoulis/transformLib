import pandas as pd
from pandas.errors import ParserError
from Dataflow import Dataflow
from Expression import Expression
from linkedServices import LinkedService
from helpers.If import If
from helpers.Statement import Statement


class Dataset:

    def __init__(self, df: pd.DataFrame = None, dataset_msg: dict = None , frozen: bool = True, enable_sum_stats: bool = False) -> None:
        self.df = df
        if dataset_msg is None:
            self.dataset_msg = {}
        else:
            self.dataset_msg = dataset_msg
        self.frozen = frozen
        self.enable_sum_stats = enable_sum_stats
        if not frozen:
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
        if self.enable_sum_stats:
            self.dataset_msg["sumStats"] = self.getSumStats()
        else:
            self.dataset_msg["sumStats"] = "Summary statistics are disabled."
        # Return Preview
        self.dataset_msg["preview"] = self.getPreview()


    def transform(self, steps: list = None) -> dict:
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


    def filter(self, action, statement: dict):
        statement = Statement(**statement)
        if action == "select":
            self.df = self.df[statement(self.df)]
        elif self.action == "drop":
            self.df = self.df[~statement(self.df)]


    def na(self, action, axis, how, subset=None):
        if action == "drop":
            self.df = self.df.dropna(axis=axis, how=how, subset=subset)


    def aggregate(self, groupBy, function, of, new_column):
        self.df[new_column] = self.df.groupby(by=groupBy)[of].transform(function)


    def expression(self, method, kwargs, new_column):
        exp = Expression(method, **kwargs)
        self.df[new_column] = exp.execute(self.df)


    def ifelse(self, elifs, els, new_column):
        for idx, i in enumerate(elifs):
            if isinstance(i.get("then"), dict):
                then = Expression(i.get("then")["expression"]["method"], i.get("then")["expression"]["kwargs"]).execute()
            else:
                then = i.get("then")
            elifs[idx] = If(Statement(**i.get("statement")), then)
        if els is not None:
            elifs[-1].setElse(els)
        for idx, i in reversed(list(enumerate(elifs))):
            if idx == 0:
                self.df[new_column] = i(self.df)
                self.df[new_column] = self.df[new_column].convert_dtypes()
                break
            elifs[idx - 1].setElse(i(self.df))


    def merge(self, rightDs, how, leftOn, rightOn, columns):
        pass





class Workspace:

    def __init__(self, sources, dataflows) -> None:
        self.sources = sources
        self.dataflows = dataflows



class Dataflow:
    
    def __init__(self, source, script, dest = None, frozen = False) -> None:
        self.source = source
        self.script = script
        self.dest = dest
        self.frozen = frozen


    def getPrePandas(self):
        pass


    def getPostPandas(self):
        pass


    def execute(self):
        pass



class LinkedDataset:
    
    def __init__(self, linkedService, linkedItem) -> None:
        self.linkedService = linkedService
        self.linkedItem = linkedItem
        self.metadata: dict = linkedService.getMetadata(linkedItem)


    def getPandas(self) -> pd.DataFrame:
        self.linkedService.getPandas(self.metadata)



class FlowDataset:

    def __init__(self, dataflow: Dataflow, **metadata) -> None:
        self.dataflow = dataflow
        self.metadata = metadata


    def getPandas(self) -> pd.DataFrame:
        self.dataflow.execute()




