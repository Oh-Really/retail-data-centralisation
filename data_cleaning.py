# %%
import data_extraction
import database_utils
import pandas as pd
import re

class DataCleaning:
    def __init__(self) -> None:
        pass

    def clean_user_data(self, df) -> pd.DataFrame:
            # These are not the same as NaN values, but the string 'NULL'. Lets get rid of these rows
            # Returns a Series object where True is in place for the string NULL    
            null_rows = df['date_of_birth'] == 'NULL'
            df = df[~null_rows]

            df.loc[:,'date_of_birth'] = df['date_of_birth'].apply(self.convert_to_datetime)
            # Instance where rows in 'date_of_birth' is a string were found to be invalid, so we drop those
            dob_strings = df['date_of_birth'].apply(lambda x: isinstance(x, str))
            df = df[~dob_strings]
            df.loc[:,'date_of_birth'] = pd.to_datetime(df['date_of_birth'])

            #Do same datetime cleaning for 'join_date' column
            df.loc[:, 'join_date'] = df['join_date'].apply(self.convert_to_datetime)
            df.loc[:, 'join_date'] = pd.to_datetime(df['join_date'])

            #Convert relevant columns into categorical types
            df.loc[:, 'country'] = df['country'].astype('category')
            df.loc[:, 'country_code'] = df['country_code'].astype('category')

            # Removing unwanted characters from address column
            df.loc[:, 'address'] = df['address'].str.replace('\n', ' ')
            df.loc[:, 'address'] = df['address'].str.replace('/', '')
            df.loc[:, 'country_code'] = df['country_code'].str.replace('GBB', 'GB')


            return df

    # This function goes through all the formats of times in the df, converting them to Timestamp prior to the column then 
    # undergoing .to_datetime()
    def convert_to_datetime(self, date_str):
        try:
            return pd.to_datetime(date_str, format='%Y-%m-%d')
        except ValueError:
            try:
                return pd.to_datetime(date_str, format='%Y %B %d')
            except ValueError:
                try:
                    return pd.to_datetime(date_str, format='%Y/%m/%d')
                except ValueError:
                    try:
                        return pd.to_datetime(date_str, format='%B %Y %d')
                    except ValueError:
                        return date_str
    

    def clean_card_data(self, card_df) -> pd.DataFrame:
        null_rows = card_df['date_payment_confirmed'] == 'NULL'
        card_df = card_df[~null_rows]
        card_df.loc[:, 'date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], errors='coerce')
        card_df = card_df[~card_df['date_payment_confirmed'].isna()]
        card_df.loc[:,'card_number'] = card_df['card_number'].apply(self.card_number_cleaning)
        
        return card_df
    
    def clean_store_data(self, df) -> pd.DataFrame:
        df = df.drop('index', axis=1)
        df = df.reset_index(drop=True)
        df = self.clean_address(df, 'address')
        df = df.drop('lat', axis=1)
        df.loc[:,'opening_date'] = df['opening_date'].apply(self.convert_to_datetime)
        df.loc[:,'opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')
        df.dropna(subset = 'opening_date', how='any', inplace=True)
        df.loc[:, 'continent'] = df['continent'].str.replace('ee', '')
        df.loc[:, 'staff_numbers'] = df['staff_numbers'].apply(lambda x: re.sub('[a-zA-Z]', '', x))
        return df
    
    def clean_products_data(self, df) -> pd.DataFrame:
        df = df.drop('Unnamed: 0', axis=1)
        df.loc[:,'weight'] = df['weight'].apply(self.convert_product_weights)
        df.dropna(inplace=True)
        return df
    
    def clean_orders_data(self, df) -> pd.DataFrame:
        df.drop(['1', 'first_name', 'last_name', 'level_0'], axis=1, inplace=True)
        return df
    
    def clean_sales_data(self, df) -> pd.DataFrame:
        df['timestamp'] = pd.to_datetime(df['timestamp'], format="%H:%M:%S", errors='coerce')
        df['hour'] = df['timestamp'].dt.hour
        df['minute'] = df['timestamp'].dt.minute
        df['second'] = df['timestamp'].dt.second
        df['sale_date'] = pd.to_datetime(df[['year', 'month', 'day', 'hour', 'minute', 'second']], errors='coerce')
        df = df[~df['sale_date'].isna()]
        return df

    def convert_product_weights(self, weight):
        # if last two chars are 'kg' then skip
        weight = str(weight)
        if weight.endswith('kg'):
            weight = weight.replace('kg', '')
            weight = self.weight_calculation(weight)
            return float(weight)
        elif weight.endswith('g'):
            weight = weight.replace('g', '')
            weight = self.weight_calculation(weight)
            return float(float(weight)/1000)
        elif weight.endswith('ml'):
            weight = weight.replace('ml', '')
            weight = self.weight_calculation(weight)
            return float(float(weight)/1000)
        elif weight.endswith('oz'):
            weight = weight.replace('oz', '')
            weight = self.weight_calculation(weight)
            return float(float(weight)/35.274)


    def weight_calculation(self, value):
        if 'x' in value:
            value.replace(' ','')
            components = value.split('x')
            return str(float(components[0])*float(components[1]))
        return value



    def card_number_cleaning(self, card_num):
        if isinstance(card_num, int):
            pass
        elif isinstance(card_num, str):
            card_num = card_num.replace('?', '')
            if card_num.isdigit():
                return int(card_num)
        return card_num
    
    def clean_address(self, df, column_name) -> pd.DataFrame:
        df.loc[:, column_name] = df[column_name].str.replace('\n', ' ')
        df.loc[:, column_name] = df[column_name].str.replace('/', '')
        return df


if __name__ == '__main__':
     dc = DataCleaning()
     database = database_utils.DatabaseConnector()
     de = data_extraction.DataExtractor()
     #df = de.retrieve_json_products()
     #df = dc.clean_sales_data(df)
    #  df = dc.clean_sales_data(df)
    #  cred = database.read_db_creds("db_creds_local.yaml")
    #  engine = database.init_db_engine(cred)
    #  engine.connect()
    #  database.upload_to_db(df, 'dim_date_times', engine)
  
# %%
