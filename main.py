# %%
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd


def upload_dim_users():
     dc = DataCleaning()
     database = DatabaseConnector()
     table = DataExtractor()
     cred = database.read_db_creds("db_creds.yaml")
     engine = database.init_db_engine(cred)
     #print(type(engine))
     df = table.read_rds_table(engine, 'legacy_users')
     df = dc.clean_user_data(df)
     cred = database.read_db_creds("db_creds_local.yaml") 
     engine = database.init_db_engine(cred)
     engine.connect()
     database.upload_to_db(df,'dim_users',engine)
     return df


def upload_card_details():
     dc = DataCleaning()
     database = DatabaseConnector()
     table = DataExtractor()
     df = table.retrieve_pdf_data()
     df = dc.clean_card_data(df)
     cred = database.read_db_creds("db_creds_local.yaml")
     engine = database.init_db_engine(cred)
     engine.connect()
     database.upload_to_db(df, 'dim_card_details', engine)
     return df

#store info
def upload_store_info():                 
         de = DataExtractor()       
         dc = DataCleaning()  
         df = de.retrieve_stores_data()
         df = dc.clean_store_data(df)
         database = DatabaseConnector()         
         cred = database.read_db_creds("db_creds_local.yaml")
         engine = database.init_db_engine(cred)
         engine.connect()
         database.upload_to_db(df, 'dim_store_details', engine)
         return df

def upload_product_info():
      de = DataExtractor()
      dc = DataCleaning()
      database = DatabaseConnector()
      df = de.extract_from_s3()
      df = dc.clean_products_data(df)
      cred = database.read_db_creds("db_creds_local.yaml")
      engine = database.init_db_engine(cred)
      engine.connect()
      database.upload_to_db(df, 'dim_products', engine)
      return df

def order_data():
     dc = DataCleaning()
     database = DatabaseConnector()
     de = DataExtractor()
     cred = database.read_db_creds("db_creds.yaml")
     engine = database.init_db_engine(cred)
     df = de.read_rds_table(engine, 'orders_table')
     df = dc.clean_orders_data(df)
     cred = database.read_db_creds("db_creds_local.yaml")
     engine = database.init_db_engine(cred)
     engine.connect()
     database.upload_to_db(df, 'orders_table', engine)
     return df

def sale_dates():
     dc = DataCleaning()
     database = DatabaseConnector()
     de = DataExtractor()
     df = de.retrieve_json_products()
     df = dc.clean_sales_data(df)
     cred = database.read_db_creds("db_creds_local.yaml")
     engine = database.init_db_engine(cred)
     engine.connect()
     database.upload_to_db(df, 'dim_date_times', engine)
     return df

      

if __name__ == '__main__':
     user_df = upload_dim_users()
     card_df = upload_card_details()
     stores_df = upload_store_info()
     s3 = upload_product_info()
     order_df = order_data()
     sales_df = sale_dates()