import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, insert_table_queries


def drop_tables(cur, conn):
    """Drop all tables in the schema as imported with drop_table_queries

    Keyword arguments:
    cur -- cursor for executing a sql command in the given database session
    conn -- the database connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create all tables in the schema as imported with create_table_queries

    Keyword arguments:
    cur -- cursor for executing a sql command in the given database session
    conn -- the database connection
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Read the database configuration file
    Use the configuration to create a database connection and cursor for the database session
    Then ensure the schema is clean by dropping the tables and recreating them
    Close the cursor to clean up idle connections
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()