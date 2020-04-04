import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """executes all 'copy table queries', these are meant to obtain data from s3 buckets by copying it into the staging
       tables.
        cur: the cursor of psycopg2
        conn: the connection of psycopg2 to the server
    """
    i=0
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        i+=1
        print('loaded staging data from s3Bucket:{}'.format(i))
    print('all data loaded to staging tables')


def insert_tables(cur, conn):
    """executes all 'insert table queries', these are meant to take data from the staging tables and loads it into the
       star schema.
       cur: the cursor of psycopg2
       conn: the connection of psycopg2 to the server"""
    i=0
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        i+=1
        print('data inserted {}/5'.format(i))
    print('All data inserted in the data mart')


def main():
    """reads the config file and establishes a connection to the redshift cluster
       also it executes the functions load_staging_table and insert_table"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('etl connected')
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()