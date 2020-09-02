import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function is used to copy log and song data 
    from S3 to staging tables.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function is used to insert data from staging tables 
    into analytical tables (both fact and dimension tables).
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function, used to read confidentials from 
    a separate file and connect to the Redshift cluster.
    This function also calls load_staging_tables 
    and insert_tables functions. 
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