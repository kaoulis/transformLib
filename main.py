import pandas as pd
from Dataset import Dataset

df = pd.read_csv("datasets\dog_visit_data.csv")

msg = {
    "transformations": [
        {
            "aggregate": {
                "groupBy": ["dog_id"], 
                "function": "min", 
                "of": "visit_date", 
                "new_column": "first_visit"
            }
        },
        {
            "expression": {
                "method": "timeBetween", 
                "kwargs": {"from": {"method": "column" , "kwargs": {"columnName": "first_visit"}}, 
                            "to": {"method": "column" , "kwargs": {"columnName": "visit_date"}},
                            "unit": "Y"}, 
                "new_column": "years"
            }
        },
        {
            "ifelse": {
                "elifs": [
                        {
                            "statement": {
                                "conditions": [
                                    {"column": "years", "compare": "<=", "value": 1}
                                ]
                            },
                            "then": 1
                        },
                        {
                            "statement": {
                                "conditions": [
                                    {"column": "years", "compare": ">", "value": 1},
                                    {"column": "years", "compare": "<=", "value": 2, "logic": "and"}
                                ]
                            },
                            "then": 2
                        }
                    ],
                "els": 3,
                "new_column": "year_group"
            }
        }
    ]
}
ds = Dataset(df, dataset_msg=msg, frozen=False)

print(pd.DataFrame(ds.dataset_msg["preview"]["head"]))