import data_extraction
import database_utils
import pandas as pd

class DataCleaning:


    def clean_user_data():
            database = database_utils.DatabaseConnector('db_creds.yaml')
            table = data_extraction.DataExtractor()
            df = table.read_rds_table(database, 'legacy_users')
            print(df.head())
