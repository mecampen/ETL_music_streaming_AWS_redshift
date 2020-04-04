import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """executes all sql queries labled with 'drop table queries' this should drop all the 'old' tables if they exist
        cur: the cursor of psycopg2
        conn: the connection of psycopg2 to the server
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """executes all sql queries labled with 'create table queries' this should create all the wanted tables for the star
        schema and the staging tables
        cur: the cursor of psycopg2
        conn: the connection of psycopg2 to the server
    """
    i=0
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        i+=1
        print('created table: {}/7'.format(i))
    print('all tables created')

def main():
    """reads the config file and establishes a connetcion to the redshift cluster
       also it executes the functions drop_tables and create_tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('create_tables connected')
    drop_tables(cur, conn)
    print('old tables dropped')
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()