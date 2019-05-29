from helpers.db_helper import DbHelper as dbhelper


def db_sampleTest1():
    # 'mysql+pymysql://taimoorpasha:Test1234@10.10.0.71:3306/postgres_Database'
    # 'postgresql+psycopg2://postgres:test1234@postgresdb.cacv2h3sjrrb.us-east-1.rds.amazonaws.com:5432/postgres'
    # db = 'oracle'
    # host = 'oracledb.cacv2h3sjrrb.us-east-1.rds.amazonaws.com'
    # port = '1521'
    # username = 'nbsoracle'
    # password = 'Test1234'
    # dbname = 'ORCL'
    # query_string = "select * from RDSADMIN.RDS_CONFIGURATION"
    db_helper_obj = dbhelper()
    # con = db_helper_obj.generate_connection_string(db, username, password, host, port, dbname)
    # db_helper_obj.create_conn(con)
    # result = db_helper_obj.query_execution(query_string)
    result = db_helper_obj.query_builder('Query.json', 'query1')
    print(result)


db_sampleTest1()