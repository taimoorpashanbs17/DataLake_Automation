""""
----------------------------------------------------------------------------------------------------------
Description:

usage: Data Conversion Helpers

Modification Log:

How to execute:
-----------------------------------------------------------------------------------------------------------
Date                Author              Story               Description
-----------------------------------------------------------------------------------------------------------
05/10/2019        Taimoor Pasha                          Initial draft.

-----------------------------------------------------------------------------------------------------------
"""


import pandas as pd


class DataConversions:
    def csv_to_dataframe(self, file):
        """Convert CSV File Data into Dataframe"""
        """:param file_name: Pass File path along with name"""

        data = pd.read_csv(file)
        return data


    def dict_to_dataframe(self, dict):
        """Convert Python Data Type Dictionary into Dataframe"""
        """:param dict: Pass dict along with name"""

        dict_df = pd.DataFrame.from_dict(dict)
        print(dict_df)

    def json_to_dataframe(self, json_file):
        """Read JSON File and Generate the results in DataFrame"""
        """:param json_file: Pass File path along with name"""
        json_data = pd.read_json(json_file)
        return json_data

        

