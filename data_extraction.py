# %%
from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
import boto3
import io


class DataExtractor:
    def __init__(self) -> None:
        pass

    def read_rds_table(self, engine, table_name: str) -> pd.DataFrame:
        '''
        Arguments:
        engine: SQLAlchemy engine object that comes from DatabaseConnector class
        table_name: Which table one is looking to extract from the db
        ------
        Return: DataFrame object of table_name
        '''
        while True:
            if table_name not in DatabaseConnector.list_db_tables(self, engine):
                print(f"Table name not in database. Please select from following options: \n")
                print(engine.list_db_tables(engine))
                break
            else:
                with engine.begin() as conn:
                    return pd.read_sql_table(table_name, con = conn, index_col='index')



    def retrieve_pdf_data(self) -> pd.DataFrame:
        df_list = tabula.read_pdf("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf", pages='all')
        df = pd.concat(df_list, ignore_index=True)
        return df
    

    def API_key(self):
        return {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        

    def list_number_stores(self):
        url_base = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        response = requests.get(url_base, headers=self.API_key())
        return response.json()['number_stores']
    
    
    def retrieve_stores_data(self):
        df_list = []
        num_stores = self.list_number_stores()
        for _ in range(num_stores):
            url_base = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{_}'
            response = requests.get(url_base, headers=self.API_key())

            df_list.append(pd.json_normalize(response.json()))
       
        return pd.concat(df_list)    
  
    
    def extract_from_s3(self):
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket='data-handling-public', Key='products.csv')
        df = df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
        return df
    
    
    def retrieve_json_products(self):
        url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        df = pd.read_json(url)
        return df


# if __name__ == '__main__':
#     de = DataExtractor()
#     sales = de.retrieve_json_products()
# %%
