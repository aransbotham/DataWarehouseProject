import configparser
import psycopg2
from redshift_utils import execute_sql_queries
from sql_queries import create_table_queries, drop_table_queries

def main():
    #Access Config File
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Establish Connection to Database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    
    # Create cursor object
    cur = conn.cursor()

    # Drop Tables
    print(f'Dropping Tables: { drop_table_queries }')
    execute_sql_queries(cur, conn, drop_table_queries)
    
    # Create Tables
    print(f'Creating Tables: { create_table_queries }')
    execute_sql_queries(cur, conn, create_table_queries)

    # Close Connection
    conn.close()

if __name__ == "__main__":
    main()