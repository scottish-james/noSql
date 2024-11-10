Here’s your tidied code in PEP 8 style:

# Import the required modules from the Cassandra driver
from cassandra.cluster import Cluster  # Cluster class to connect to the Cassandra database

try:
    # Initialize a Cluster object with the specified IP address of the Cassandra node (localhost in this case)
    cluster = Cluster(['127.0.0.1'])
    
    # Establish a session with the Cassandra cluster
    session = cluster.connect()
    
    print('Session started successfully.')

except Exception as e:
    print(f"Session failed to start because of {e}")

# Create keyspace to work in
try:
    key_space_name = "james_keyspace"
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {key_space_name}
        WITH REPLICATION = 
        {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
    """)
    print(f"Keyspace '{key_space_name}' created successfully.")
    
    session.set_keyspace(key_space_name)

except Exception as e:
    print(f"Failed to create or set keyspace '{key_space_name}': {e}")

# Drop existing tables
drop_tables = ['musicLibrary', 'albumLibrary', 'artistLibrary']

for table in drop_tables:
    try:
        session.execute(f"DROP TABLE IF EXISTS {table}")
        print(f"Table '{table}' dropped successfully.")
    except Exception as e:
        print(f"Failed to drop table '{table}': {e}")


def create_table(session, query, table_name):
    """Creates a table in the Cassandra database."""
    try:
        session.execute(query)
        print(f"'{table_name}' table created successfully.")
    except Exception as e:
        print(f"Failed to create table '{table_name}': {e}")


# Table creation queries
table_queries = {
    "music_library": """
        CREATE TABLE IF NOT EXISTS music_library(
            year int,
            artist_name text, 
            album_name text, 
            PRIMARY KEY (year, artist_name)
        );
    """,
    "album_library": """
        CREATE TABLE IF NOT EXISTS album_library(
            album_name text,
            artist_name text,
            year int, 
            PRIMARY KEY (album_name, artist_name)
        );
    """,
    "artist_library": """
        CREATE TABLE IF NOT EXISTS artist_library(
            artist_name text,
            year int,
            album_name text, 
            PRIMARY KEY (artist_name, year)
        );
    """
}

# Loop through the table queries and create tables
for table_name, query in table_queries.items():
    create_table(session, query, table_name)