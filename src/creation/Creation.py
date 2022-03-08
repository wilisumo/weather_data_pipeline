from src.common.Dynamodb import Dynamodb
from src.common.common_helper import (
    LOGGER,
)


class Creation:
    # def __init__(self):

    def process(self, table_name, attributes, schema, provisions):
        try:
            dynamo_db = Dynamodb()
            dynamo_db.create_table(table_name, attributes, schema, provisions)
        except Exception as e:
            LOGGER.info(f"Exception in Dynamo {e}")
