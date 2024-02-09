import configparser
import psycopg2
from redshift_utils import execute_sql_queries
from sql_queries import copy_table_queries, insert_table_queries

def main():
    # Read from Config
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Set Database Connection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Load data into Staging Tables
    print(f'Loading data into Staging Tables: { copy_table_queries }')
    execute_sql_queries(cur, conn, copy_table_queries)
    
    # Load Data into Fact and Dimension Tables 
    print(f'Loading data into Fact and Dim Tables: { insert_table_queries }')
    execute_sql_queries(cur, conn, insert_table_queries)

    # Close Database Connection
    conn.close()


if __name__ == "__main__":
    main()