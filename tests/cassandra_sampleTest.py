from helpers.cassandra_helper import CassandraHelper



host = '127.0.0.1'
port = 9042
keyspace = 'test1'
query = 'select * from employee'
ch = CassandraHelper()
ch.create_conn_cassandra(host,port,keyspace)
ch.execute_query_cassandra(query)