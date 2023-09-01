from configparser import ConfigParser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import 

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    dwh_cfg = ConfigParser()
    dwh_cfg.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        dwh_cfg['CLUSTER']['HOST'],
        dwh_cfg['CLUSTER']['DB_NAME'],
        dwh_cfg['CLUSTER']['DB_USER'],
        dwh_cfg['CLUSTER']['DB_PASSWORD'],
        dwh_cfg['CLUSTER']['PORT']
        )
    )
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    #insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()