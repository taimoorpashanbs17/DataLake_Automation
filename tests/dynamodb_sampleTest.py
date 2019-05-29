from helpers.dynamodb_helper import DynamodbHelper as ddbHelper


def sampleTest1():
    ddb = ddbHelper()
    table = 'Partioning-Tables'
    df = "dataframe"
    id = "1"
    item_add = {
        "id": {"N": "3"},
        "file_size": {"S": "8.9 KB"},
        "filename": {"S": "Mustang.csv"},
        "path": {"S": "s3://qa-automation-tables/Partioning-Tables"}
    }
    item_delete = {
        "id": {"S": "3"},

    }
    AttributeDefinitions = [
        {
            'AttributeName': 'id',
            'AttributeType': 'N'
        },

    ]
    KeySchema = [
        {
            'AttributeName': 'id',
            'KeyType': 'HASH'
        },

    ]
    ProvisionedThroughput = {
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
    print(ddb.get_data(table, id, df))
    # ddb.add_data(table, item_add)
    # ddb.delete_data(table, item_delete)
    # ddb.add_table(table, KeySchema,AttributeDefinitions, ProvisionedThroughput)
    # ddb.add_data(table, item_add)
    # ddb.del_table(table)

sampleTest1()