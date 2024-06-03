import psycopg2
from psycopg2 import sql
from db_build.db_conf import *

def staging_data_to_pg(table_name,db_username,db_password,db_host,db_port,db_name):
    print(f"Loading data into {table_name} table.....")
    try:
        conn=create_connection(db_username,db_password,db_host,db_port,db_name)
        conn.autocommit=True
        cursor=conn.cursor()
        #cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY")
        csv_files={
            'customer':r'G:\syllabus\projects\etl\data\customer.csv',
            'customer_app':r'G:\syllabus\projects\etl\data\customer_app.csv',
            'calendar_details':r'G:\syllabus\projects\etl\data\calendar_details.csv',
            'product':r'G:\syllabus\projects\etl\data\product.csv',
            'staff':r'G:\syllabus\projects\etl\data\staff.csv',
            'sales_fact':r'G:\syllabus\projects\etl\data\sales_fact.csv'
        }
        copy_sql=sql.SQL("""
                COPY {}         
                FROM stdin         
                WITH CSV HEADER         
                DELIMITER as ','
        """)
        copy_sql=copy_sql.format(sql.Identifier(table_name))

        # open the CSV file and execute the COPY command
        with open (csv_files[table_name],'r') as f:
            cursor.copy_expert(sql=copy_sql,file=f)
        cursor.close()
        close_connection(conn)
        print(f"Data Loaded successfully into the {table_name} table")
    except Exception as e:
        print(f'Got an error while loading the data into the table, {e}')


if __name__=="__main__":
    try:
        staging_data_to_pg('customer','postgres','password','192.168.1.6',5432,'commercedb')
        print("#"*50)
        staging_data_to_pg('customer_app','postgres','password','192.168.1.6',5432,'commercedb')
        print("#"*50)
        staging_data_to_pg('product','postgres','password','192.168.1.6',5432,'commercedb')
        print("#"*50)
        staging_data_to_pg('staff','postgres','password','192.168.1.6',5432,'commercedb')
        print("#"*50)
        staging_data_to_pg('calendar_details','postgres','password','192.168.1.6',5432,'commercedb')
        print("#"*50)
        staging_data_to_pg('sales_fact','postgres','password','192.168.1.6',5432,'commercedb')
    except Exception as e:
        print('Got an error,', e)
