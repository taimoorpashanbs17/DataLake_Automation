

import boto3
import pandas as pd


class DynamoDBHelper:

    dynamo_client = boto3.client('dynamodb')

    def get_data(self, table, id, return_type):
        """
                Retrieve Data from Table
                :param table: Table name, you want to retrieve data from.
                :param id: Record id, you want to retrieve.
                :param return_type: wether you want to return dataframe or Dictionary.
        """
        try:
            response = self.dynamo_client.get_item(TableName=table,
                                                   Key={"id": {'S': id}})
            items = response.get("Item")
            if return_type == "dataframe":
                df = pd.DataFrame(items)
                return df
            else:
                return items
        except Exception as e:
            print(e)

    def add_data(self, table, item):
        """
                Add Records to Table
                :param table: Table name, you want to enter data to.
                :param item: Contains details of record you want to create as dictionary.
        """
        try:
            self.dynamo_client.put_item(TableName=table, Item=item)
        except Exception as e:
            print(e)

    def delete_data(self, table, item):
        """
                Delete Data from Table
                :param table: Table name, you want to delete data from.
                :param item: Contains id of record you want to delete as dictionary.
        """
        try:
            self.dynamo_client.delete_item(TableName=table, Key=item)
        except Exception as e:
            print(e)

    def add_table(self, tablename, keyschema, attributes, throughput):
        """
                 Create a new table
                 :param attributes: An array of attributes that describe the key schema for the table and indexes.
                 :param tablename: The name of the table to create.
                 :param keyschema: Specifies the attributes that make up the primary key for a table or an index.
         """
        try:
            table = self.dynamo_client.create_table(TableName=tablename, KeySchema=keyschema,
                                                    AttributeDefinitions=attributes, ProvisionedThroughput=throughput)
            return table
        except Exception as e:
            print(e)

    def del_table(self, tablename):
        """
                 Delete a table
                 :param tablename: The name of the table to delete.
        """
        try:
            self.dynamo_client.delete_table(TableName=tablename)
        except Exception as e:
            print(e)
