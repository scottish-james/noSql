README
======

Overview
--------

This project demonstrates how to connect to a Cassandra database using Python, create a keyspace, and manage tables. The script includes operations for:

*   Establishing a connection to a Cassandra cluster
*   Creating a keyspace
*   Dropping and creating tables
*   Executing queries programmatically

Prerequisites
-------------

*   **Python 3.x**
*   **Cassandra** database running locally or remotely
*   **Cassandra Driver for Python** (`cassandra-driver`)

### Installing the Cassandra Driver

To install the driver, run:

```bash
pip install cassandra-driver
```

Usage
-----

### 1\. Establish Connection

The script initializes a connection to a Cassandra cluster:

```python
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
```

It prints a success message if the connection is established.

### 2\. Keyspace Creation

The keyspace `james_keyspace` is created with `SimpleStrategy` and a replication factor of 1:

```python
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS james_keyspace
    WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
""")
session.set_keyspace('james_keyspace')
```

### 3\. Dropping Existing Tables

The script drops any pre-existing tables:

```python
drop_tables = ['musicLibrary', 'albumLibrary', 'artistLibrary']

for table in drop_tables:
    session.execute(f"DROP TABLE IF EXISTS {table}")
```

### 4\. Table Creation

It defines and creates the following tables:

*   **music\_library**
*   **album\_library**
*   **artist\_library**

Each table is created based on specific queries:

```python
table_queries = {
    "music_library": """
        CREATE TABLE IF NOT EXISTS music_library(
            year int,
            artist_name text, 
            album_name text, 
            PRIMARY KEY (year, artist_name)
        );
    """,
    ...
}

for table_name, query in table_queries.items():
    create_table(session, query, table_name)
```

### 5\. `create_table` Function

This reusable function executes table creation queries:

```python
def create_table(session, query, table_name):
    try:
        session.execute(query)
        print(f"'{table_name}' table created successfully.")
    except Exception as e:
        print(f"Failed to create table '{table_name}': {e}")
```

Running the Script
------------------

1.  Ensure Cassandra is running on `127.0.0.1`.
2.  Execute the script:
    
    ```bash
    python script_name.py
    ```
    
3.  Verify the outputs in your Cassandra database.

License
-------

This project is licensed under the MIT License.