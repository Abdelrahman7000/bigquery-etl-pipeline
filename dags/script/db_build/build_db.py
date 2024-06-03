import psycopg2
#import sqlalchemy
from db_conf import *
from psycopg2 import Error

create_tables_statements={
    'customer':
        """
        CREATE TABLE IF NOT EXISTS customer (
            customer_id BIGINT PRIMARY KEY,
            full_name VARCHAR(255),
            phone VARCHAR(30),
            email VARCHAR(255),
            gender VARCHAR(30),
            date_of_birth DATE,
            city VARCHAR(255),
            state VARCHAR(10),
            country VARCHAR(10),
            customer_segment VARCHAR(255),
            education VARCHAR(255),
            customer_source VARCHAR(255)
        )
        """
    
    ,
    'customer_app':
        """
        CREATE TABLE IF NOT EXISTS customer_app (
            customer_id BIGINT PRIMARY KEY,
            full_name VARCHAR(255),
            phone VARCHAR(30),
            email VARCHAR(255),
            gender VARCHAR(30),
            date_of_birth DATE,
            city VARCHAR(255),
            state VARCHAR(10),
            country VARCHAR(10),
            customer_segment VARCHAR(255),
            education VARCHAR(255),
            customer_source VARCHAR(255)
        )
        """
    ,
    'calendar_details':
        """
        CREATE TABLE IF NOT EXISTS calendar_details (
            calendar_id BIGINT PRIMARY KEY,
            date DATE,
            day_of_month BIGINT,
            week_of_month INTEGER,
            month INTEGER,
            month_name VARCHAR(30),
            quarter INTEGER,
            year INTEGER,
            weekday INTEGER,
            day VARCHAR(30),
            is_weekend VARCHAR(30)
        )
        """
    ,
    'product':
        """
        CREATE TABLE IF NOT EXISTS product (
            product_id BIGINT PRIMARY KEY,
            product_name VARCHAR(225),
            product_category VARCHAR(20),
            model_year INTEGER,
            product_availability VARCHAR(255)
        )
        """
    
    ,
    'staff':
        """
        CREATE TABLE IF NOT EXISTS staff (
            staff_id BIGINT PRIMARY KEY,
            full_name VARCHAR(225),
            phone VARCHAR(30),
            email VARCHAR(225),
            gender VARCHAR(20),
            city VARCHAR(255),
            state VARCHAR(10),
            country VARCHAR(10)
        )
        """
    
    ,
    'sales_fact':
        """
        CREATE TABLE IF NOT EXISTS sales_fact (
            order_id BIGINT PRIMARY KEY,
            calendar_id BIGINT,
            customer_id BIGINT,
            product_id BIGINT,
            staff_id BIGINT,
            quantity_sold NUMERIC,
            total_sales NUMERIC,
            order_date DATE,
            shipping_date DATE,
            list_price NUMERIC
        )
        """
        
}

drop_tables_statements={
    'customer':
        """
            DROP TABLE IF EXISTS customer
        """
    
    ,
    'customer_app':
        """
            DROP TABLE IF EXISTS customer_app
        """
    ,
    'calendar':
        """
            DROP TABLE IF EXISTS calendar
        """
    
    ,
    'product':
        """
            DROP TABLE IF EXISTS product
        """
    
    ,
    'staff':
        """
            DROP TABLE IF EXISTS staff
        """
    
    ,
    'sales':
        """
            DROP TABLE IF EXISTS sales
        """   
}


def drop_database(db_username,db_password,db_host,db_port,db_name):
    try:
        conn=create_connection(db_username,db_password,db_host,db_port,db_name)
        conn.autocommit=True
        
        # Kill the connection that is opened to the old database
        query1= """
            SELECT 
            pg_terminate_backend(pid) 
            FROM pg_stat_activity 
            WHERE 
                    pid <> pg_backend_pid()
                AND datname = 'commercedb';
        """
        # Drop database statement
        query2 = """
            DROP DATABASE IF EXISTS commercedb
        """

        if execute_query(connection=conn,query=query1):
            if execute_query(connection=conn,query=query2,database_name='commercedb'):
                print("Dropped commercedb database successfully")
        
        close_connection(conn)

    except (Exception,Error) as e:
        print("Faild to drop the database. There is an error: ",e)


def create_database(db_username,db_password,db_host,db_port,db_name):
    try:
        conn=create_connection(db_username,db_password,db_host,db_port,db_name)
        conn.autocommit=True
        query = """
            CREATE DATABASE commercedb
        """

        if execute_query(connection=conn,query=query,database_name='commercedb'):
            print("Successfully created commercedb database")
        
        close_connection(conn)

    except (Exception,Error) as e:
        print("There is an error: ",e)


def drop_tables(db_username,db_password,db_host,db_port,db_name):
    try:
        conn=create_connection(db_username,db_password,db_host,db_port,db_name)
        conn.autocommit=True
        
        for table,query in drop_tables_statements.items():
            if execute_query(connection=conn,query=query,table_name=table):
                print(f"{table} table dropped successfully")
            else:
                print(f'Failed to drop table {table}')
        close_connection(conn)
    except (Exception,Error) as e:
        print('There is an error, ',e)


def create_tables(db_username,db_password,db_host,db_port,db_name):
    try:
        conn=create_connection(db_username,db_password,db_host,db_port,db_name)
        conn.autocommit=True
        for table, query in create_tables_statements.items():
            if execute_query(conn,query,table):
                print(f"{table} table created successfully")
            else:
                print(f"Failed to create {table} table")
        close_connection(conn)
    except (Exception,Error) as e:
        print("There is an error, ",e)



if __name__== "__main__":
    try:
        create_database('postgres','password','192.168.1.6',5432,'postgres')
    except Exception as e:
        print(f'Got an error while creating the database, {e}')

    try:
        create_tables('postgres','password','192.168.1.6',5432,'commercedb')
    except Exception as e:
        print(f"Got an error while creating the tables, {e}")



        
    
