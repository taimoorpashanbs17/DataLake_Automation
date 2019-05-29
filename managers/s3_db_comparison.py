"""
----------------------------------------------------------------------------------------------------------
Description:

usage: S3 to DB data comparison method

Author  : Ghufran Ahmad

Modification Log:

How to execute:
-----------------------------------------------------------------------------------------------------------
Date                Author              Story               Description
-----------------------------------------------------------------------------------------------------------
05/27/2019        Ghufran Ahmad                              Initial draft.

-----------------------------------------------------------------------------------------------------------
"""
import helpers.s3_helper as s3
import helpers.db_helper as db
import helpers.comparison_helper as comparison
import json


def s3_to_db_comparison():
    s3_obj = s3.S3Helper()
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
    with open("comparison.json") as f:
        data = json.load(f)
    for obj in data:
        if obj == "s3":
            bucket = data[obj]['data']['bucket']
            file_name = data[obj]['data']['filename']
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
    df2 = s3_obj.read_s3_file(bucket, file_name)
    result = comparison.compare_dataframes(df1, df2)
    if bool(result):
        print("Both data are same.")
    else:
        print("Both data aren't same.")


s3_to_db_comparison()