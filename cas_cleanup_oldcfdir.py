
import os;
import cassandra;

from cassandra.cluster import Cluster
from cassandra.policies import (TokenAwarePolicy, DCAwareRoundRobinPolicy, RetryPolicy)
from cassandra.query import (PreparedStatement, BoundStatement)

cluster = Cluster()


#  contact_points=['127.0.0.1'],
#  load_balancing_policy= TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='datacenter1')),
# default_retry_policy = RetryPolicy()
# )
session = cluster.connect('system')
#select keyspace_name from system.schema_keyspaces;
result_keyspaces = session.execute("select keyspace_name from system.schema_keyspaces;")
for x in result_keyspaces: print x.keyspace_name

open()
for line in open("C:\Users\Navjyot\.ccm\test\node1\conf\cassandra.yaml"):
    if "data_file_directories:" in line:
        print line



#select keyspace_name,columnfamily_name,cf_id from system.schema_columnfamilies;
result_table = session.execute("select keyspace_name,columnfamily_name,cf_id from system.schema_columnfamilies;")
for x in result_table: print x.keyspace_name,x.columnfamily_name,x.cf_id
