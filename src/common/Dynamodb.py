from src.common.common_helper import remove_empty_from_dict
import src.common.common_helper as helper
from boto3.dynamodb.types import TypeSerializer
from src.common.common_helper import (
    LOGGER,
)

class Dynamodb(object):
    def __init__(self):
        self.dynamodb = helper.get_dynamo_instance()
        #self.table = self.dynamodb.Table(table)
        #self.table_name = table

    def save(self, item):
        item = remove_empty_from_dict(item)
        self.table.put_item(Item=item)

    def get(self, key_value):
        return self.table.get_item(Key=key_value)

    def attributevalueupdate(self, key_value, field, newvalue):
        self.table.update_item(
            Key=key_value,
            UpdateExpression=f"set {field} = :r",
            ExpressionAttributeValues={":r": newvalue},
            ReturnValues="UPDATED_NEW",
        )

    def create_table(self, table_name, attributes, schema, provisions):
        self.dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=attributes,
            KeySchema=schema,
            ProvisionedThroughput=provisions,
        )

    def write_transaction(self, items, job_id, PK,table_name):
        try:
            self.dynamodb.transact_write_items(
                TransactItems=[{'Put':{'TableName': table_name,'Item': val,'ConditionExpression': f'attribute_not_exists({PK})',}} for val in items],
                ClientRequestToken= job_id
            )
            LOGGER.info("NEW TRANSACTION FINISHED")
        except Exception as e:
            LOGGER.info(e)

