import psycopg2
import os
from psycopg2 import Error


def create_connection(db_username,db_password,db_host,db_port,db_name):
    try:
        conn=psycopg2.connect(
            dbname=db_name,
            user=db_username,
            password=db_password,
            host=db_host,
            port=db_port
            )
        print('Connected To Database successfully')
        return conn
    except(Exception,Error) as e:
        print(f"There is an error {e}")
        return None



def close_connection(connection):
    if connection:
        connection.close()
        print("Connection is closed")
    



def execute_query(connection,query,table_name=None,database_name=None):
    try:
        cursor=connection.cursor()
        cursor.execute(query)
        if table_name:
            print(f'Query executed successfully for table: {table_name}')
        if database_name:
            print(f"Query executed for database: {database_name}")
        return True
    except (Exception,Error) as e:
        print('There is an error, ',e)
        return False
