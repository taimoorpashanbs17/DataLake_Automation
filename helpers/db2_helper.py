
import ibm_db_dbi
import pandas as pd


class DB2Helper:
    def create_conn_db2(self, host, port, database,username,password,protocol):
        """
        Creating connection for DB2
        :param host: host name
        :param port: port name
        :param database: db name
        :param username: user id
        :param password: password
        :param protocol: TCPIP
        """
        try:
            self.connection = ibm_db_dbi.connect("DATABASE="+database+";HOSTNAME="+host+";PORT="+port+";PROTOCOL="+protocol+";UID="+username+";PWD="+password+";", "", "")

        except Exception as e:
            print("The connection was unsuccessful.\n" + str(e))

    def execute_query_db2(self, query):
        """
        For executing DB2 query
        :param query: The query string
        :return: A data frame
        """
        try:
            df = pd.read_sql_query(query, self.connection)
            return df
        except Exception as e:
            print("An error occured during execution of query.\n" + str(e))