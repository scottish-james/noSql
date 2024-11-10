from cassandra.cluster import Cluster

def establish_connection(cluster_address='127.0.0.1'):
    """Establish a connection to the Cassandra cluster."""
    try:
        cluster = Cluster([cluster_address])
        session = cluster.connect()
        print('Session started successfully.')
        return session
    except Exception as e:
        raise RuntimeError(f"Session failed to start because of {e}")

def create_keyspace(session, keyspace_name):
    """Create a keyspace if it doesn't exist and set it."""
    try:
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
            WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
        """)
        print(f"Keyspace '{keyspace_name}' created successfully.")
        session.set_keyspace(keyspace_name)
    except Exception as e:
        raise RuntimeError(f"Failed to create or set keyspace '{keyspace_name}': {e}")

def drop_tables(session, tables):
    """Drop a list of tables if they exist."""
    for table in tables:
        try:
            session.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"Table '{table}' dropped successfully.")
        except Exception as e:
            print(f"Failed to drop table '{table}': {e}")

def create_tables(session, table_queries):
    """Create tables based on the provided queries."""
    for table_name, query in table_queries.items():
        try:
            session.execute(query)
            print(f"'{table_name}' table created successfully.")
        except Exception as e:
            print(f"Failed to create table '{table_name}': {e}")

def insert_all_data(session, data, queries):
    """Insert data into tables based on the provided queries."""
    for table_name, query in queries.items():
        for row in data:
            try:
                # Dynamically map fields based on table name
                session.execute(query, tuple(row[column] for column in queries_mapping[table_name]))
                print(f"Inserted {row} into {table_name}.")
            except Exception as e:
                print(f"Failed to insert {row} into {table_name}: {e}")

# Table queries
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

# Insert queries
insert_into_queries = {
    "music_library": """
        INSERT INTO music_library (year, artist_name, album_name)
        VALUES (%s, %s, %s)
    """,
    "album_library": """
        INSERT INTO album_library (album_name, artist_name, year)
        VALUES (%s, %s, %s)
    """,
    "artist_library": """
        INSERT INTO artist_library (artist_name, year, album_name)
        VALUES (%s, %s, %s)
    """
}

# Column mapping for data insertion
queries_mapping = {
    "music_library": ["year", "artist_name", "album_name"],
    "album_library": ["album_name", "artist_name", "year"],
    "artist_library": ["artist_name", "year", "album_name"]
}

# Sample data
data = [    
    {"year": 1970, "artist_name": "The Beatles", "album_name": "Let it Be"},
    {"year": 1965, "artist_name": "The Beatles", "album_name": "Rubber Soul"},
    {"year": 1965, "artist_name": "The Who", "album_name": "My Generation"},
    {"year": 1966, "artist_name": "The Monkees", "album_name": "The Monkees"},
    {"year": 1970, "artist_name": "The Carpenters", "album_name": "Close To You"}
]

# Main flow
if __name__ == "__main__":
    session = establish_connection()
    keyspace_name = "james_keyspace"
    create_keyspace(session, keyspace_name)

    drop_tables(session, ["music_library", "album_library", "artist_library"])
    create_tables(session, table_queries)
    insert_all_data(session, data, insert_into_queries)
    
    session.shutdown()
    cluster.shutdown()