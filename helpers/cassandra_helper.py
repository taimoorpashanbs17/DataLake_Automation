

class CassandraHelper:
    def __init__(self, host, port, keyspace):
        """
        creating connection with cassandra
        :param host: The host name.
        :param port: The port in use.
        :param keyspace: The name of the keyspace
        """
        try:
            cluster = Cluster([host], port=port)
            self.session = cluster.connect(keyspace)
        except Exception as e:
            print("The connection was unsuccessful.\n" + str(e))

    def execute_query_cassandra(self, query):
        """
        For executing cassandra query
        :param query: The query string
        :return: A data frame
        """
        try:
            df = pd.DataFrame(list(self.session.execute(query)))
            return df
        except Exception as e:
            print("An error occurred during execution of query.\n" + str(e))


