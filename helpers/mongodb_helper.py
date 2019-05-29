
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import pandas as pd

class mongodb_helper:
    def __init__(self, url):
        connection_string = url
        self.client = MongoClient(connection_string,connect=True)

    def create_cursor(self, database_name, collection_name):
        try:
            my_database = self.client[database_name]
            my_collection = my_database[collection_name]
            self.cursor = my_collection.find()
            return self.cursor
        except ServerSelectionTimeoutError as e:
            print(e)

    def execute_query(self, database, collection, return_type = 'df'):
        self.create_cursor(database_name= database, collection_name= collection)
        if return_type == 'json':
            for record in self.cursor:
             return record
        else:
            df = pd.DataFrame(self.cursor)
            print(df)