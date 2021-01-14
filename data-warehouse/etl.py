import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Excute the staging table load queries as imported with copy_table_queries
    These queries perform a direct copy from json files in S3 to staging tables within RedShift

    Keyword arguments:
    cur -- cursor for executing a sql command in the given database session
    conn -- the database connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Execute the star schema insert queries as imported from insert_table_queries
    These copy data from the staging tables, transform the data so it is suitable for fact and dimension
    tables and then insert it

    Keyword arguments:
    cur -- cursor for executing a sql command in the given database session
    conn -- the database connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Read the database configuration file
    Use the configuration to create a database connection and cursor for the database session
    Load the data into the dtaging tables, then insert the staging data once tranformed into the star schema
    Close the cursor to clean up idle connections
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()