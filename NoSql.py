# Import the required modules from the Cassandra driver
import cassandra  # Base Cassandra module (used indirectly)
from cassandra.cluster import Cluster  # Cluster class to connect to the Cassandra database

try:
    # Initialize a Cluster object with the specified IP address of the Cassandra node (localhost in this case)
    cluster = Cluster(['127.0.0.1'])  
    
    # Establish a session with the Cassandra cluster
    session = cluster.connect()
    
    # Print a success message if the session is established successfully
    print('Start Session')

except Exception as e:  
    # Catch any exceptions that occur during cluster connection or session creation
    # Print an error message along with the exception details
    print(f"Session failed to start because of {e}")


#Create keyspace to work in
try: 
    session.exectute("""
    CREATE KEYSPACE IF NOT EXISITS james_keyspace 
    WITH REPLICATION = 
    {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)

except Expection as e: 
    print(e)

try: 
    session.set_keyspace('james_keyspace')
except Exception as e: 
    print(e)

drop1 = "DROP TABLE IF EXISTS musicLibrary"
drop2 = "DROP TABLE IF EXISTS albumLibrary"
drop3 = "DROP TABLE IF EXISTS artistLibrary"

try: 
    session.execute(drop1)
    session.execute(drop2)
    session.execute(drop3)

except Exception as e:
    print(e)