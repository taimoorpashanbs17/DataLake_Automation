"""
----------------------------------------------------------------------------------------------------------
Description:

usage: S3 to Athena data comparison method

Author  : Ghufran Ahmad

Modification Log:

How to execute:
-----------------------------------------------------------------------------------------------------------
Date                Author              Story               Description
-----------------------------------------------------------------------------------------------------------
05/29/2019        Ghufran Ahmad                              Initial draft.

-----------------------------------------------------------------------------------------------------------
"""
import helpers.s3_helper as s3
import helpers.athena_helper as athena
import helpers.comparison_helper as comparison
import json


def s3_to_athena_comparison():
    s3_obj = s3.S3Helper()
    athena_obj = athena.AthenaHelper()
    athena_bucket = ''
    s3_bucket = ''
    s3_file_name = ''
    athena_query = ''
    athena_output_loc = ''
    with open("athena_s3.json") as f:
        data = json.load(f)
    for obj in data:
        if obj == "s3":
            s3_bucket = data[obj]['data']['bucket']
            s3_file_name = data[obj]['data']['filename']
        elif obj == 'athena':
            athena_bucket = data[obj]['bucket']
            athena_output_loc = data[obj]['output_location']
            athena_query = data[obj]['query']
        else:
            print("Please, use a valid JSON file.")

    df1 = athena_obj.exec_query_athena(athena_query, athena_bucket, athena_output_loc)
    df2 = s3_obj.read_s3_file(s3_bucket, s3_file_name)
    result = comparison.compare_dataframes(df2, df1)
    if result == True:
        print("Both data are same.")
    elif result == False:
        print("Both data aren't same.")
    else:
        print("Comparison Failed!")
        print(result, " is not same in both data frames.")


