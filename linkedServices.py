import os
import pyodbc
import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobClient

class LinkedService:

    def __init__(self) -> None:
        pass

    def getMetadata(self) -> dict:
        return None

    def getPandas(self):
        return None

class localDevice(LinkedService):

    def __init__(self, path) -> None:
        super().__init__()
        self.path = path

    def getPandas(self, filename, filetype):
        pass


class SQLService(LinkedService):
    def __init__(self) -> None:
        super().__init__()


class DIHAHService(LinkedService):

    def __init__(self, SQLConnectionString: str, StorageConnectionString: str) -> None:
        super().__init__()
        self.SQLConnectionString = SQLConnectionString
        self.StorageConnectionString = StorageConnectionString

    def getMetadata(self, uuid) -> dict:
        connection = pyodbc.connect(self.SQLConnectionString)
        datasetCursor = connection.cursor()
        datasetCursor.execute("SELECT * FROM datasets WHERE datasets.datasetUUID = ?;", uuid)
        columns = [column[0] for column in datasetCursor.description]
        row = datasetCursor.fetchone()
        connection.close()
        datasetCursor.close()    
        return dict(zip(columns, row))


    def getPandas(self, uuid, path) -> pd.DataFrame:
        blob_bytes = self.getBytes(uuid, path)
        return pd.read_csv(BytesIO(blob_bytes), encoding='utf8')


    def getBytes(self, uuid, path):
        blob = BlobClient.from_connection_string(
            conn_str = self.StorageConnectionString, 
            container_name = path,
            blob_name = uuid
        )
        blob_bytes = blob.download_blob().readall()
        return blob_bytes
