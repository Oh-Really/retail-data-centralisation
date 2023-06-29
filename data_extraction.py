# %%
import database_utils
import pandas as pd
from datetime import datetime
import tabula


class DataExtractor:
    def __init__(self) -> None:
        pass

    def read_rds_table(self, DatabaseConnector: type[database_utils.DatabaseConnector], 
                       table_name: str) -> pd.DataFrame:
        '''
        Arguments:
        DatabaseConenctor: instance of the DatabaseConenctor class
        table_name: Which table one is looking to extract from the db
        ------
        Return: DataFrame object of table_name
        '''
        while True:
            if table_name not in DatabaseConnector.list_db_tables():
                print(f"Table name not in database. Please select from following options: \n")
                print(DatabaseConnector.list_db_tables())
                break
            else:
                user_data = pd.read_sql_table(table_name, DatabaseConnector.init_db_engine(), index_col='index')
            return user_data



    def retrieve_pdf_data(self) -> pd.DataFrame:
        df_list = tabula.read_pdf("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf", pages='all')
        df = pd.concat(df_list, ignore_index=True)
        return df
# %%
