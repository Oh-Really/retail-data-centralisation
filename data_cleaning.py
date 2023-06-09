import data_extraction
import database_utils
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

    # This function goes through all the formats of times in the df, conversting them to Timestamp prior to the column then 
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




if __name__ == '__main__':
     dc = DataCleaning()
     database = database_utils.DatabaseConnector('db_creds.yaml')
     table = data_extraction.DataExtractor()
     df = table.read_rds_table(database, 'legacy_users')
     dc.clean_user_data(df)
     print(df.info())
