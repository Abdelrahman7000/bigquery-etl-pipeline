from db_build.db_conf import *
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio

def process(db_username,db_password,db_host,db_port,db_name):
    try:
        conn=create_connection(db_username,db_password,db_host,db_port,db_name)
        cust_query1 = "select * from customer;"
        cust_query2 = "select * from customer_app;"
        
        sales_query="select * from sales_fact"

        df1 = sqlio.read_sql_query(cust_query1, conn)
        df2 = sqlio.read_sql_query(cust_query2, conn)
        
        sales_fact_df=sqlio.read_sql_query(sales_query,conn)
        
        close_connection(conn)
    except Exception as e:
        print('Got an error, ',e)

    dimcust = pd.concat([df1,df2], axis=0)
    dimcust_df = dimcust.drop(columns=['phone', 'email','country'])
    dimcust_df['date_of_birth']=pd.to_datetime(dimcust_df['date_of_birth'])

    sales_fact_df['price_per_unit'] = sales_fact_df['list_price'] / sales_fact_df['quantity_sold']
    sales_fact_df['order_date']=pd.to_datetime(sales_fact_df['order_date'])
    sales_fact_df['shipping_date']=pd.to_datetime(sales_fact_df['shipping_date'])

    return dimcust_df,sales_fact_df



def customer_dim(df):
    print("Start loading customer dimension into BigQery ...")
    #df=df.drop_duplicates('customer_id').set_index('customer_id')
    try:
        df.to_gbq(destination_table='retail_dataset.customer_dim',project_id='citric-expanse-423211-t4')
        print(f">>> customer dimension loaded successfully into BigQuery \n {'#'*60}")
    except Exception as e:
        print('Got an error while loading the customer dimension, ',e)



def product_dim(db_username,db_password,db_host,db_port,db_name):
    try:
        print("Start loading product dimension into BigQuery ...")
        conn=create_connection(db_username,db_password,db_host,db_port,db_name)
        query = "select * from product;"

        df = sqlio.read_sql_query(query, conn)
        close_connection(conn)
        df.to_gbq(destination_table='retail_dataset.product_dim',project_id='citric-expanse-423211-t4')
        print(f">>> product dimension loaded successfully into BigQuery \n {'#'*60}")
    except Exception as e:
        print('Got an error, ',e)

    


def calendar_details_dim(db_username,db_password,db_host,db_port,db_name):
    try:
        print("Start loading calendar details dimension into BigQuery ...")
        conn=create_connection(db_username,db_password,db_host,db_port,db_name)
        query = "select * from calendar_details;"

        df = sqlio.read_sql_query(query, conn)
        close_connection(conn)
        
        df['date']=pd.to_datetime(df['date'])

        df.to_gbq(destination_table='retail_dataset.calendar_details_dim',project_id='citric-expanse-423211-t4')
        print(f">>> calendar details dimension loaded successfully into BigQuery \n {'#'*60}")
    except Exception as e:
        print('Got an error, ',e)
    
    


def staff_dim(db_username,db_password,db_host,db_port,db_name):
    try:
        print("Start loading staff dimension into BigQuery ...")
        conn=create_connection(db_username,db_password,db_host,db_port,db_name)
        query = "select * from staff;"

        df = sqlio.read_sql_query(query, conn)
        close_connection(conn)
        df.to_gbq(destination_table='retail_dataset.staff_dim',project_id='citric-expanse-423211-t4')
        print(f">>> staff dimension loaded successfully into BigQuery \n {'#'*60}")
    except Exception as e:
        print('Got an error, ',e)

    




def sales_fact(df):
    try:
        print("Start loading sales fact table into BigQuery ...")
        df.to_gbq(destination_table='retail_dataset.sales_fact',project_id='citric-expanse-423211-t4')
        print(f">>> sales fact table loaded successfully into BigQuery \n {'#'*60}")
    except Exception as e:
        print("Got an error while loading sales fact table, ", e)



if __name__=="__main__":
    df1,df2=process('postgres','password','192.168.1.6',5432,'commercedb')
    customer_dim(df1)
    
    product_dim('postgres','password','192.168.1.6',5432,'commercedb')
    
    calendar_details_dim('postgres','password','192.168.1.6',5432,'commercedb')
    
    staff_dim('postgres','password','192.168.1.6',5432,'commercedb')

    sales_fact(df2)