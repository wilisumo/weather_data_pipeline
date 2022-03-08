"""from datetime import date
import awswrangler as wr
import pandas as pd

df = pd.DataFrame({
    "id": [1, 2],
    "value": ["foo", "boo"],
    "date": [date(2020, 1, 1), date(2020, 1, 2)]
})

wr.s3.to_parquet(
    df=df,
    path=path,
    dataset=True,
    mode="overwrite",
    partition_cols=["date"]
)

wr.s3.read_parquet(path, dataset=True)"""
from src.common.Dynamodb import Dynamodb
from src.common.common_helper import (
    LOGGER,
)

class Store:
    # def __init__(self):

    def store_transaction(self, item, job_id, PK,table):
        try:
            dynamo_db = Dynamodb()
            dynamo_db.write_transaction(item, job_id, PK,table)
        except Exception as e:
            LOGGER.info(f"Exception in Dynamo {e}")
