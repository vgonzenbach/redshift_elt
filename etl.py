from configparser import ConfigParser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from logger_cfg import setup_logger

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():

    # set up logger
    logger = setup_logger(__file__)

    # read config file
    dwh_cfg = ConfigParser()
    dwh_cfg.read('dwh.cfg')

    # connect to database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        dwh_cfg['CLUSTER']['HOST'],
        dwh_cfg['CLUSTER']['DB_NAME'],
        dwh_cfg['CLUSTER']['DB_USER'],
        dwh_cfg['CLUSTER']['DB_PASSWORD'],
        dwh_cfg['CLUSTER']['PORT']
        )
    )
    cur = conn.cursor()
    
    # Run queries
    logger.info('Loading data from S3 into staging tables...')
    load_staging_tables(cur, conn)
    logger.info('Loading data from S3 into staging tables - success!')
    
    logger.info('Transforming data into analytical form...')
    insert_tables(cur, conn)
    logger.info('Transforming data into analytical form - success!')

    conn.close()


if __name__ == "__main__":
    main()