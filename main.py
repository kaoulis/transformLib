from msilib import schema
import pandas as pd
from Dataset import Dataset

df = pd.read_csv("datasets\dog_visit_data.csv")

msg = {
    "transformations": [
        {
            "rename": {
                "columns": {
                    "dog_id": "id"
                }
            }
        }
    ]
}
ds = Dataset(df, dataset_msg=msg, frozen=False)

print(ds.dataset_msg["schema"])