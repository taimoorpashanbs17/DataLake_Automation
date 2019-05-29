"""
----------------------------------------------------------------------------------------------------------
Description:

usage: Athena to DB data comparison method

Author  : Ghufran Ahmad

Modification Log:

How to execute:
-----------------------------------------------------------------------------------------------------------
Date                Author              Story               Description
-----------------------------------------------------------------------------------------------------------
05/28/2019        Ghufran Ahmad                              Initial draft.

-----------------------------------------------------------------------------------------------------------
"""
import helpers.athena_helper as athena
import helpers.db_helper as db
import helpers.comparison_helper as comparison
import json


def athena_to_db_comparison():
    athena_obj = athena.AthenaHelper()
    db_obj = db.DbHelper()
    bucket = ''
    output = ''
    athena_query = ''
    database = ''
    host = ''
    port = ''
    user = ''
    password = ''
    db_name = ''
    table_name = ''
    sql = ''
    with open("athena_db.json") as f:
        data = json.load(f)
    for obj in data:
        if obj == "athena":
            bucket = data[obj]['bucket']
            output = data[obj]['output_location']
            athena_query = data[obj]['query']
        elif obj == 'sqlite':
            sql = data[obj]['connection']['path']
            database = obj
        else:
            database = obj
            user = data[obj]['connection']['user']
            host = data[obj]['connection']['host']
            port = data[obj]['connection']['port']
            password = data[obj]['connection']['password']
            db_name = data[obj]['connection']['database']
            table_name = data[obj]['data']['table']

    conn = db_obj.generate_connection_string(database, user, password, host, port, db_name, sql)
    db_obj.create_conn(conn)
    query = db_obj.query_builder('query.json', 'query1')
    df1 = db_obj.query_execution(query)
    df2 = athena_obj.exec_query_athena(athena_query, bucket, output)
    result = comparison.compare_dataframes(df1, df2)
    if result == True:
        print("Both data are same.")
    elif result == False:
        print("Both data aren't same.")
    else:
        print("Comparison Failed!")
        print(result, " is not same in both data frames.")

