Cassandra Database Setup
========================

Overview
--------

This script connects to a Cassandra cluster, creates a keyspace, defines tables, and inserts sample data using Python and the Cassandra driver.

Requirements
------------

*   Python 3.x
*   Cassandra Cluster running locally or at the specified address (`127.0.0.1` by default)

Installation
------------

1.  Install Python dependencies:
    
    ```
    pip install cassandra-driver
    ```
    

Configuration
-------------

*   Ensure your Cassandra cluster is running and accessible at `127.0.0.1`, or modify `cluster_address` in the script accordingly.

Usage
-----

1.  Run the script:
    
    ```
    python cassandra_setup.py
    ```
    

Script Details
--------------

### `cassandra_setup.py`

#### Functions:

1.  **`establish_connection(cluster_address='127.0.0.1')`**:
    
    *   Establishes a connection to the Cassandra cluster.
2.  **`create_keyspace(session, keyspace_name)`**:
    
    *   Creates a keyspace if it doesn't exist and sets it.
3.  **`drop_tables(session, tables)`**:
    
    *   Drops specified tables if they exist.
4.  **`create_tables(session, table_queries)`**:
    
    *   Creates tables based on provided queries.
5.  **`insert_all_data(session, data, queries)`**:
    
    *   Inserts data into tables based on provided queries.

#### Table Queries:

*   Defines schema for `music_library`, `album_library`, and `artist_library` tables.

#### Insert Queries:

*   Provides queries for inserting sample data into respective tables.

#### Sample Data:

*   Includes sample data for demonstration purposes.
