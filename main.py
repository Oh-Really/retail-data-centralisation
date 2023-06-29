# %%
import data_extraction
import database_utils
import data_cleaning
import pandas as pd

if __name__ == '__main__':
     dc = data_cleaning.DataCleaning()
     database = database_utils.DatabaseConnector('db_creds.yaml')
     table = data_extraction.DataExtractor()
     df = table.read_rds_table(database, 'legacy_users')
     df = dc.clean_user_data(df)
     card_df = table.retrieve_pdf_data()
     card_df = dc.clean_card_data(card_df)
# %%
#duplicates = df.duplicated(['first_name', 'last_name'], False)
# %%
