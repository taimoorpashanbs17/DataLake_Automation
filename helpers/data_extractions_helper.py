"""
----------------------------------------------------------------------------------------------------------
Description:

usage: Data Extractions Helpers

Modification Log:

How to execute:
-----------------------------------------------------------------------------------------------------------
Date                Author              Story               Description
-----------------------------------------------------------------------------------------------------------
05/20/2019        Taimoor Pasha                          Initial draft.

-----------------------------------------------------------------------------------------------------------
"""
import pandas as pd

class DataExtractions:

    def read_txt_file(self, file_name):
        """Read all the content present within Text File"""
        """:param file_name: We have to pass the path of text file. """

        file = open(file_name, 'r')
        file_data = file.read()
        return file_data

    def read_csv(self, file):
        """Read CSV File and Generate the results in DataFrame"""
        """:param file_name: Pass File path along with name"""

        data = pd.read_csv(file)
        return data

    def read_json(self, json_file):
        """Read JSON File and Generate the results in DataFrame"""
        """:param json_file: Pass File path along with name"""
        json_data = pd.read_json(json_file)
        return json_data
