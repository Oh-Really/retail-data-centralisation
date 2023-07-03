# import data_extraction
# import database_utils
import pandas as pd

class DataCleaning:


    def clean_user_data(self, df):
            # database = database_utils.DatabaseConnector('db_creds.yaml')
            # table = data_extraction.DataExtractor()
            # df = table.read_rds_table(database, 'legacy_users')
            # print(df.head())
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
    

    def clean_card_data(self, card_df):
        null_rows = card_df['date_payment_confirmed'] == 'NULL'
        card_df = card_df[~null_rows]
        card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], errors='coerce')
        card_df = card_df[~card_df['date_payment_confirmed'].isna()]
        card_df.loc[:,'card_number'] = card_df['card_number'].apply(self.card_number_cleaning)
        #card_df.loc[:, 'expiry_date'] = pd.to_datetime(card_df['expiry_date'], format='%m/%d')
        
        return card_df
    
    def clean_store_date(self, df):
        df = df.drop('index', axis=1)
        df = df.reset_index(drop=True)
        df = self.clean_address(df, 'address')
        df = df.drop('lat', axis=1)
        df.loc[:,'opening_date'] = df['opening_date'].apply(self.convert_to_datetime)
        df.loc[:,'opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')
        df.dropna(subset = 'opening_date', how='any', inplace=True)
        df.loc[:, 'continent'] = df['continent'].str.replace('ee', '')
        return df


    def card_number_cleaning(self, card_num):
        if isinstance(card_num, int):
            pass
        elif isinstance(card_num, str):
            card_num = card_num.replace('?', '')
            if card_num.isdigit():
                return int(card_num)
        return card_num
    
    def clean_address(self, df, column_name):
        df.loc[:, column_name] = df[column_name].str.replace('\n', ' ')
        df.loc[:, column_name] = df[column_name].str.replace('/', '')
        return df

    def standardise_phone_number(phone_number):
        ## if the first character is a "+", remove it.
        if phone_number[0] == '+':
            phone_number = phone_number.replace('+', '')        
        ## remove all whitespace from the phone number
        phone_number = phone_number.strip()        
        ## remove hyphens from the phone number
        phone_number = phone_number.replace('-', '')        
        ## if the number doesn't start with 00, prepend 00 to it beginning of the number
        if phone_number[:3] != '00':
            phone_number = '00' + phone_number        
        ## return the phone number
        return phone_number