"""
----------------------------------------------------------------------------------------------------------
Description:

usage: DB to DB data comparison method

Author  : Ghufran Ahmad

Modification Log:

How to execute:
-----------------------------------------------------------------------------------------------------------
Date                Author              Story               Description
-----------------------------------------------------------------------------------------------------------
05/27/2019        Ghufran Ahmad                              Initial draft.

-----------------------------------------------------------------------------------------------------------
"""
import helpers.db_helper as db
import helpers.comparison_helper as comparison
import json


def db_to_db_comparison():
    db_obj = db.DbHelper()
    bucket = ''
    file_name = ''
    database = ''
    host = ''
    port = ''
    user = ''
    password = ''
    db_name = ''
    table_name = ''
    sql = ''
    count = 0
    with open("comparison_db.json") as f:
        data = json.load(f)
    for obj in data:
        if obj == 'sqlite':
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
        if count == 0:
            df1 = db_obj.query_execution(query)
            count += 1
        else:
            df2 = db_obj.query_execution(query)
    result = comparison.compare_dataframes(df1, df2)
    if result == True:
        print("Both data are same.")
    elif result == False:
        print("Both data aren't same.")
    else:
        print("Comparison Failed!")
        print(result, " is not same in both data frames.")

