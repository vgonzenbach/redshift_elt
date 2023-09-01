import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import logging
import os

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():

    # Read config file
    dwh_cfg = configparser.ConfigParser()
    dwh_cfg.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        dwh_cfg['CLUSTER']['HOST'],
        dwh_cfg['CLUSTER']['DB_NAME'],
        dwh_cfg['CLUSTER']['DB_USER'],
        dwh_cfg['CLUSTER']['DB_PASSWORD'],
        dwh_cfg['CLUSTER']['DB_PORT'])
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()